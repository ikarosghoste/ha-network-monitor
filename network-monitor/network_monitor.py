
#!/usr/bin/env python3

import concurrent.futures
import json
import logging
import os
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
    ]

    missing = [key for key in required if key not in config]
    if missing:
        raise ValueError(f"Missing configuration options: {', '.join(missing)}")

    return config


CONFIG = load_config()

MONITOR_NAME = CONFIG["monitor_name"]
MQTT_HOST = CONFIG["mqtt_host"]
MQTT_PORT = int(CONFIG["mqtt_port"])
MQTT_USERNAME = CONFIG.get("mqtt_username") or None
MQTT_PASSWORD = CONFIG.get("mqtt_password") or None

MQTT_BASE_TOPIC = CONFIG["mqtt_base_topic"].strip("/")
DISCORD_ALERT_TOPIC = CONFIG["discord_alert_topic"]

POLL_INTERVAL = int(CONFIG["poll_interval"])
PING_TIMEOUT = int(CONFIG["ping_timeout"])
HEARTBEAT_INTERVAL = int(CONFIG["heartbeat_interval"])
FAILURE_THRESHOLD = int(CONFIG["failure_threshold"])
RECOVERY_THRESHOLD = int(CONFIG["recovery_threshold"])
MAX_WORKERS = int(CONFIG["max_workers"])

LOG_LEVEL = CONFIG.get("log_level", "INFO").upper()
LOG_HOST_STATUS = bool(CONFIG.get("log_host_status", True))


def load_hosts():
    hosts = {}

    for entry in CONFIG["hosts"].split(","):
        entry = entry.strip()
        if not entry:
            continue

        if "=" not in entry:
            raise ValueError(f"Invalid host entry: {entry}")

        hostname, address = entry.split("=", 1)
        hostname = hostname.strip()
        address = address.strip()

        if not hostname or not address:
            raise ValueError(f"Invalid host entry: {entry}")

        if hostname in hosts:
            raise ValueError(f"Duplicate hostname: {hostname}")

        hosts[hostname] = address

    if not hosts:
        raise ValueError("No monitoring hosts configured")

    return hosts


HOSTS = load_hosts()


# ============================================================
# Validation
# ============================================================

if POLL_INTERVAL < 1:
    raise ValueError("poll_interval must be >= 1")

if PING_TIMEOUT < 1:
    raise ValueError("ping_timeout must be >= 1")

if FAILURE_THRESHOLD < 1 or RECOVERY_THRESHOLD < 1:
    raise ValueError("Failure and recovery thresholds must be >= 1")


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
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

last_heartbeat = 0


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
        command = [
            "ping", "-n", "1",
            "-w", str(PING_TIMEOUT * 1000),
            host,
        ]
    else:
        command = [
            "ping", "-c", "1",
            "-W", str(PING_TIMEOUT),
            host,
        ]

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

def topic(name):
    return f"{MQTT_BASE_TOPIC}/{name}"


def publish(destination, payload, retain=True):
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


def publish_alert(hostname, online):
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

    if publish(
        DISCORD_ALERT_TOPIC,
        json.dumps(payload),
        retain=False,
    ):
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
    global latest_status

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

        # Initial discovery: do not alert on startup.
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
                "%s: awaiting initial confirmation (%s/%s failures)",
                hostname,
                failure_counts[hostname],
                FAILURE_THRESHOLD,
            )
            continue

        # State transition
        if previous is not None and previous != current:
            status = "ONLINE" if current else "OFFLINE"

            log.warning(
                "%s (%s): %s",
                hostname,
                HOSTS[hostname],
                status,
            )

            publish_alert(hostname, current)

        # Publish only when state changes or MQTT needs resynchronizing.
        if last_published.get(hostname) != current:
            status = "online" if current else "offline"

            if publish(topic(f"{hostname}/status"), status):
                last_published[hostname] = current

        if LOG_HOST_STATUS:
            log.debug(
                "%s (%s): %s",
                hostname,
                HOSTS[hostname],
                "ONLINE" if current else "OFFLINE",
            )


# ============================================================
# Heartbeat
# ============================================================

def send_heartbeat():
    global last_heartbeat

    now = time.monotonic()

    if now - last_heartbeat < HEARTBEAT_INTERVAL:
        return

    online_count = sum(
        1 for status in latest_status.values() if status is True
    )

    offline_count = sum(
        1 for status in latest_status.values() if status is False
    )

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
            "Heartbeat published | %d/%d online | %d offline | %d awaiting confirmation | MQTT connected",
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
    log.info("=" * 55)

    log.info("Connecting to MQTT broker...")

    client.connect_async(
        MQTT_HOST,
        MQTT_PORT,
        keepalive=60,
    )
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
