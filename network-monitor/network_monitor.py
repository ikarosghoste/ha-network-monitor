#!/usr/bin/env python3

import concurrent.futures
import json
import logging
import platform
import signal
import subprocess
import sys
import time

from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt


# ============================================================
# Configuration
# ============================================================

OPTIONS_FILE = Path("/data/options.json")


def load_config():
    if not OPTIONS_FILE.is_file():
        raise FileNotFoundError(
            f"Home Assistant options file not found: {OPTIONS_FILE}"
        )

    with OPTIONS_FILE.open("r", encoding="utf-8") as file:
        config = json.load(file)

    required = [
        "monitor_name",
        "mqtt_host",
        "mqtt_port",
        "mqtt_base_topic",
        "discord_alert_topic",
        "hosts",
        "poll_interval",
        "ping_timeout",
        "heartbeat_interval",
        "failure_threshold",
        "recovery_threshold",
        "max_workers",
    ]

    missing = [key for key in required if key not in config]
    if missing:
        raise ValueError(
            f"Missing configuration options in {OPTIONS_FILE}: "
            f"{', '.join(missing)}"
        )

    return config


CONFIG = load_config()

MONITOR_NAME = str(CONFIG["monitor_name"]).strip()
MQTT_HOST = str(CONFIG["mqtt_host"]).strip()
MQTT_PORT = int(CONFIG["mqtt_port"])
MQTT_USERNAME = CONFIG.get("mqtt_username") or None
MQTT_PASSWORD = CONFIG.get("mqtt_password") or None
MQTT_BASE_TOPIC = str(CONFIG["mqtt_base_topic"]).strip("/")
DISCORD_ALERT_TOPIC = str(CONFIG["discord_alert_topic"]).strip("/")

POLL_INTERVAL = int(CONFIG["poll_interval"])
PING_TIMEOUT = int(CONFIG["ping_timeout"])
HEARTBEAT_INTERVAL = int(CONFIG["heartbeat_interval"])
FAILURE_THRESHOLD = int(CONFIG["failure_threshold"])
RECOVERY_THRESHOLD = int(CONFIG["recovery_threshold"])
MAX_WORKERS = int(CONFIG["max_workers"])

LOG_LEVEL = str(CONFIG.get("log_level", "info")).upper()
LOG_HOST_STATUS = bool(CONFIG.get("log_host_status", True))


# ============================================================
# Host Configuration
# ============================================================

def load_hosts():
    hosts = {}

    for entry in str(CONFIG["hosts"]).split(","):
        entry = entry.strip()
        if not entry:
            continue

        if "=" not in entry:
            raise ValueError(f"Invalid host entry {entry!r}; expected name=address")

        hostname, address = entry.split("=", 1)
        hostname = hostname.strip()
        address = address.strip()

        if not hostname or not address:
            raise ValueError(f"Invalid host entry {entry!r}; name and address are required")
        if hostname in hosts:
            raise ValueError(f"Duplicate hostname in hosts configuration: {hostname}")

        hosts[hostname] = address

    if not hosts:
        raise ValueError("No monitoring hosts configured in the app Configuration tab")

    return hosts


HOSTS = load_hosts()


# ============================================================
# Validation and Logging
# ============================================================

if not MONITOR_NAME:
    raise ValueError("monitor_name cannot be empty")
if not MQTT_HOST:
    raise ValueError("mqtt_host cannot be empty")
if not MQTT_BASE_TOPIC:
    raise ValueError("mqtt_base_topic cannot be empty")
if not DISCORD_ALERT_TOPIC:
    raise ValueError("discord_alert_topic cannot be empty")
if not 1 <= MQTT_PORT <= 65535:
    raise ValueError("mqtt_port must be between 1 and 65535")
if POLL_INTERVAL < 1:
    raise ValueError("poll_interval must be >= 1")
if PING_TIMEOUT < 1:
    raise ValueError("ping_timeout must be >= 1")
if HEARTBEAT_INTERVAL < 5:
    raise ValueError("heartbeat_interval must be >= 5")
if FAILURE_THRESHOLD < 1 or RECOVERY_THRESHOLD < 1:
    raise ValueError("Failure and recovery thresholds must be >= 1")
if MAX_WORKERS < 1:
    raise ValueError("max_workers must be >= 1")
if LOG_LEVEL not in {"DEBUG", "INFO", "WARNING", "ERROR"}:
    raise ValueError(f"Unsupported log_level: {LOG_LEVEL}")

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    stream=sys.stdout,
    force=True,
)

log = logging.getLogger(MONITOR_NAME)


# ============================================================
# Runtime State
# ============================================================

running = True
latest_status = {}
last_published = {}
failure_counts = {}
recovery_counts = {}
last_heartbeat = 0.0


# ============================================================
# Shutdown
# ============================================================

def shutdown(signum, frame):
    global running
    log.info("Shutdown requested (signal %s)", signum)
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ============================================================
# ICMP Monitoring
# ============================================================

def ping(host: str) -> bool:
    if platform.system().lower() == "windows":
        command = ["ping", "-n", "1", "-w", str(PING_TIMEOUT * 1000), host]
    else:
        command = ["ping", "-c", "1", "-W", str(PING_TIMEOUT), host]

    try:
        result = subprocess.run(
            command,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=PING_TIMEOUT + 2,
            check=False,
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, OSError) as exc:
        log.debug("Ping execution failed for %s: %s", host, exc)
        return False


# ============================================================
# MQTT
# ============================================================

def topic(name: str) -> str:
    return f"{MQTT_BASE_TOPIC}/{name}"


def publish(destination: str, payload, retain=True) -> bool:
    if not client.is_connected():
        log.warning("Cannot publish to %s: MQTT disconnected", destination)
        return False

    try:
        result = client.publish(
            destination,
            payload,
            qos=1,
            retain=retain,
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            log.error(
                "MQTT publish failed for %s (code %s)",
                destination,
                result.rc,
            )
            return False
        return True
    except Exception:
        log.exception("MQTT publish exception for %s", destination)
        return False


def publish_alert(hostname: str, online: bool) -> bool:
    if online:
        title = f"PING RECOVERED | {hostname.upper()}"
        message = f"{hostname} is responding to ICMP ping again."
        severity = "info"
    else:
        title = f"PING ALERT | {hostname.upper()}"
        message = f"{hostname} is not responding to ICMP ping."
        severity = "critical"

    payload = {
        "title": title,
        "message": message,
        "severity": severity,
        "source": "Home Assistant",
    }

    if publish(DISCORD_ALERT_TOPIC, json.dumps(payload), retain=False):
        log.info("Discord alert published: %s", title)
        return True

    log.error("Failed to publish Discord alert: %s", title)
    return False


def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        log.error("MQTT connection failed: %s", reason_code)
        return

    log.info("Connected to MQTT broker at %s:%s", MQTT_HOST, MQTT_PORT)
    publish(topic("availability"), "online")

    for hostname, status in latest_status.items():
        publish(
            topic(f"{hostname}/status"),
            "online" if status else "offline",
        )


def on_disconnect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        log.warning("Unexpected MQTT disconnection: %s", reason_code)
    else:
        log.info("Disconnected from MQTT broker")


client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=MONITOR_NAME,
)

if MQTT_USERNAME:
    client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)

client.will_set(
    topic("availability"),
    payload="offline",
    qos=1,
    retain=True,
)

client.on_connect = on_connect
client.on_disconnect = on_disconnect
client.reconnect_delay_set(min_delay=1, max_delay=60)


# ============================================================
# Host Monitoring
# ============================================================

def monitor_host(item):
    hostname, address = item
    return hostname, ping(address)


def monitoring_cycle(executor):
    results = executor.map(monitor_host, HOSTS.items())

    for hostname, reachable in results:
        failure_counts.setdefault(hostname, 0)
        recovery_counts.setdefault(hostname, 0)
        previous = latest_status.get(hostname)

        if reachable:
            recovery_counts[hostname] += 1
            failure_counts[hostname] = 0
        else:
            failure_counts[hostname] += 1
            recovery_counts[hostname] = 0

        # Establish initial state without generating startup alerts.
        if previous is None:
            if reachable:
                latest_status[hostname] = True
            elif failure_counts[hostname] >= FAILURE_THRESHOLD:
                latest_status[hostname] = False
        elif previous and failure_counts[hostname] >= FAILURE_THRESHOLD:
            latest_status[hostname] = False
        elif not previous and recovery_counts[hostname] >= RECOVERY_THRESHOLD:
            latest_status[hostname] = True

        current = latest_status.get(hostname)
        if current is None:
            log.warning(
                "%s: awaiting initial confirmation (%d/%d failures)",
                hostname,
                failure_counts[hostname],
                FAILURE_THRESHOLD,
            )
            continue

        if previous is not None and previous != current:
            status_label = "ONLINE" if current else "OFFLINE"
            log.warning("%s (%s): %s", hostname, HOSTS[hostname], status_label)
            publish_alert(hostname, current)

        if last_published.get(hostname) != current:
            status_value = "online" if current else "offline"
            if publish(topic(f"{hostname}/status"), status_value):
                last_published[hostname] = current

        # This option is intended to be useful at the default INFO level.
        if LOG_HOST_STATUS:
            log.info(
                "%s (%s): %s",
                hostname,
                HOSTS[hostname],
                "ONLINE" if current else "OFFLINE",
            )
        else:
            log.debug(
                "%s (%s): %s | failures=%d | recoveries=%d",
                hostname,
                HOSTS[hostname],
                "ONLINE" if current else "OFFLINE",
                failure_counts[hostname],
                recovery_counts[hostname],
            )


# ============================================================
# Heartbeat
# ============================================================

def send_heartbeat():
    global last_heartbeat

    now = time.monotonic()
    if now - last_heartbeat < HEARTBEAT_INTERVAL:
        return

    online_count = sum(status is True for status in latest_status.values())
    offline_count = sum(status is False for status in latest_status.values())
    unknown_count = len(HOSTS) - online_count - offline_count

    payload = {
        "monitor": MONITOR_NAME,
        "status": "online",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hosts_monitored": len(HOSTS),
        "hosts_online": online_count,
        "hosts_offline": offline_count,
    }

    if publish(topic("heartbeat"), json.dumps(payload)):
        last_heartbeat = now
        log.info(
            "Heartbeat published | %d/%d online | %d offline | "
            "%d awaiting confirmation | MQTT connected",
            online_count,
            len(HOSTS),
            offline_count,
            unknown_count,
        )
    else:
        log.warning("Heartbeat not published; MQTT unavailable")


# ============================================================
# Main
# ============================================================

def main():
    log.info("=" * 55)
    log.info("Starting Network Monitor: %s", MONITOR_NAME)
    log.info("Monitoring %d hosts", len(HOSTS))

    for hostname, address in HOSTS.items():
        log.info("Registered host: %s (%s)", hostname, address)

    log.info("MQTT broker: %s:%s", MQTT_HOST, MQTT_PORT)
    log.info("MQTT base topic: %s", MQTT_BASE_TOPIC)
    log.info("Discord alert topic: %s", DISCORD_ALERT_TOPIC)
    log.info("Polling interval: %ss", POLL_INTERVAL)
    log.info("Ping timeout: %ss", PING_TIMEOUT)
    log.info("Heartbeat interval: %ss", HEARTBEAT_INTERVAL)
    log.info("Failure threshold: %s", FAILURE_THRESHOLD)
    log.info("Recovery threshold: %s", RECOVERY_THRESHOLD)
    log.info("Concurrent workers: %s", MAX_WORKERS)
    log.info("Log level: %s", LOG_LEVEL)
    log.info("Per-host INFO logging: %s", LOG_HOST_STATUS)
    log.info("Configuration source: %s", OPTIONS_FILE)
    log.info("=" * 55)
    log.info("Connecting to MQTT broker...")

    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    try:
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=MAX_WORKERS
        ) as executor:
            while running:
                cycle_start = time.monotonic()
                monitoring_cycle(executor)
                send_heartbeat()

                elapsed = time.monotonic() - cycle_start
                sleep_time = max(0, POLL_INTERVAL - elapsed)
                if running:
                    time.sleep(sleep_time)
    finally:
        log.info("Stopping Network Monitor")
        if client.is_connected():
            publish(topic("availability"), "offline")
            client.disconnect()
        client.loop_stop()
        log.info("Network Monitor stopped")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        log.exception("Fatal Network Monitor error")
        raise
