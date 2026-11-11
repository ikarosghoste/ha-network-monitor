
#!/usr/bin/env python3

import concurrent.futures
import json
import logging
import os
import platform
import signal
import subprocess
import time

from datetime import datetime, timezone
from pathlib import Path

import paho.mqtt.client as mqtt
from dotenv import load_dotenv


# ============================================================
# Configuration
# ============================================================

# Always load .env from the script directory
load_dotenv("/config/.env", override=True)

MONITOR_NAME = os.getenv(
    "MONITOR_NAME",
    "raspberrypi-monitor"
)

# MQTT
MQTT_HOST = os.getenv("MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME") or None
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD") or None

MQTT_BASE_TOPIC = os.getenv(
    "MQTT_BASE_TOPIC",
    "monitor"
)

DISCORD_ALERT_TOPIC = os.getenv(
    "DISCORD_ALERT_TOPIC",
    "discord-ha/alerts"
)

# Monitoring
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))
PING_TIMEOUT = int(os.getenv("PING_TIMEOUT", "2"))
HEARTBEAT_INTERVAL = int(
    os.getenv("HEARTBEAT_INTERVAL", "60")
)

# Consecutive checks required to change state
FAILURE_THRESHOLD = int(
    os.getenv("FAILURE_THRESHOLD", "3")
)

RECOVERY_THRESHOLD = int(
    os.getenv("RECOVERY_THRESHOLD", "2")
)

MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))


# ============================================================
# Host Configuration
# ============================================================

def load_hosts():
    hosts = {}

    raw_hosts = os.getenv("HOSTS", "")

    for entry in raw_hosts.split(","):
        entry = entry.strip()

        if not entry:
            continue

        try:
            hostname, address = entry.split("=", 1)

            hostname = hostname.strip()
            address = address.strip()

            if not hostname or not address:
                raise ValueError

            hosts[hostname] = address

        except ValueError:
            raise ValueError(
                f"Invalid HOSTS entry: {entry}"
            )

    if not hosts:
        raise ValueError(
            "No monitoring hosts configured in .env"
        )

    return hosts


HOSTS = load_hosts()


# ============================================================
# Configuration Validation
# ============================================================

if POLL_INTERVAL <= 0:
    raise ValueError("POLL_INTERVAL must be greater than zero")

if PING_TIMEOUT <= 0:
    raise ValueError("PING_TIMEOUT must be greater than zero")

if FAILURE_THRESHOLD < 1 or RECOVERY_THRESHOLD < 1:
    raise ValueError("Failure and recovery thresholds must be >= 1")

if MAX_WORKERS < 1:
    raise ValueError("MAX_WORKERS must be >= 1")


# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

log = logging.getLogger(MONITOR_NAME)


# ============================================================
# Runtime State
# ============================================================

running = True

# Confirmed host states
latest_status = {}

# Last successfully queued MQTT states
last_published = {}

# Consecutive check counters
failure_counts = {}
recovery_counts = {}

last_heartbeat = 0


# ============================================================
# Shutdown Handling
# ============================================================

def shutdown(signum, frame):
    global running

    log.info("Shutdown requested")
    running = False


signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)


# ============================================================
# ICMP Monitoring
# ============================================================

def ping(host: str) -> bool:

    system = platform.system().lower()

    if system == "windows":
        cmd = [
            "ping",
            "-n", "1",
            "-w", str(PING_TIMEOUT * 1000),
            host
        ]

    else:
        cmd = [
            "ping",
            "-c", "1",
            "-W", str(PING_TIMEOUT),
            host
        ]

    try:
        result = subprocess.run(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=PING_TIMEOUT + 2
        )

        return result.returncode == 0

    except (subprocess.TimeoutExpired, OSError):
        return False


# ============================================================
# MQTT Publishing
# ============================================================

def topic(name: str) -> str:
    return f"{MQTT_BASE_TOPIC}/{name}"


def publish(name: str, payload, retain=True) -> bool:

    if not client.is_connected():
        return False

    try:
        result = client.publish(
            topic(name),
            payload,
            qos=1,
            retain=retain
        )

        return result.rc == mqtt.MQTT_ERR_SUCCESS

    except Exception as exc:
        log.error("MQTT publish failed: %s", exc)
        return False


# ============================================================
# Discord Alerts
# ============================================================

def publish_alert(hostname: str, online: bool) -> bool:

    if online:
        title = f"PING RECOVERED | {hostname.upper()}"
        message = (
            f"{hostname} is responding to ICMP ping again."
        )
        severity = "info"

    else:
        title = f"PING ALERT | {hostname.upper()}"
        message = (
            f"{hostname} is not responding to ICMP ping."
        )
        severity = "critical"

    payload = {
        "title": title,
        "message": message,
        "severity": severity,
        "source": "Home Assistant"
    }

    try:
        result = client.publish(
            DISCORD_ALERT_TOPIC,
            json.dumps(payload),
            qos=1,
            retain=False
        )

        if result.rc == mqtt.MQTT_ERR_SUCCESS:
            log.info("Discord alert queued: %s", title)
            return True

        log.error(
            "Discord alert publish failed: %s",
            result.rc
        )

        return False

    except Exception as exc:
        log.error("Discord alert error: %s", exc)
        return False


# ============================================================
# MQTT Callbacks
# ============================================================

def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties
):

    if reason_code.is_failure:
        log.error(
            "MQTT connection failed: %s",
            reason_code
        )
        return

    log.info("Connected to MQTT broker")

    # Announce monitor availability
    publish("availability", "online")

    # Republish known states after reconnecting
    for hostname, status in latest_status.items():

        publish(
            f"{hostname}/status",
            "online" if status else "offline"
        )


def on_disconnect(
    client,
    userdata,
    flags,
    reason_code,
    properties
):

    if reason_code.is_failure:
        log.warning(
            "Unexpected MQTT disconnection: %s",
            reason_code
        )


# ============================================================
# MQTT Client
# ============================================================

client = mqtt.Client(
    callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
    client_id=MONITOR_NAME
)

if MQTT_USERNAME:
    client.username_pw_set(
        MQTT_USERNAME,
        MQTT_PASSWORD
    )

# Automatically mark monitor offline if connection drops
client.will_set(
    topic("availability"),
    payload="offline",
    qos=1,
    retain=True
)

client.on_connect = on_connect
client.on_disconnect = on_disconnect

client.reconnect_delay_set(
    min_delay=1,
    max_delay=60
)


# ============================================================
# Host Monitoring
# ============================================================

def monitor_host(item):

    hostname, address = item

    return hostname, ping(address)


def monitoring_cycle(executor):

    global latest_status

    results = executor.map(
        monitor_host,
        HOSTS.items()
    )

    for hostname, reachable in results:

        # Initialize counters
        failure_counts.setdefault(hostname, 0)
        recovery_counts.setdefault(hostname, 0)

        previous = latest_status.get(hostname)

        # ----------------------------------------------------
        # Track consecutive failures and successes
        # ----------------------------------------------------

        if reachable:

            recovery_counts[hostname] += 1
            failure_counts[hostname] = 0

        else:

            failure_counts[hostname] += 1
            recovery_counts[hostname] = 0

        # ----------------------------------------------------
        # Determine confirmed state
        # ----------------------------------------------------

        if previous is None:

            # Initial discovery
            # Avoid generating alerts on startup

            if reachable:
                latest_status[hostname] = True

            elif failure_counts[hostname] >= FAILURE_THRESHOLD:
                latest_status[hostname] = False

        elif previous and failure_counts[hostname] >= FAILURE_THRESHOLD:

            latest_status[hostname] = False

        elif not previous and recovery_counts[hostname] >= RECOVERY_THRESHOLD:

            latest_status[hostname] = True

        current = latest_status.get(hostname)

        # Await initial confirmation
        if current is None:

            log.warning(
                "%s: awaiting initial confirmation (%d/%d failures)",
                hostname,
                failure_counts[hostname],
                FAILURE_THRESHOLD
            )

            continue

        # ----------------------------------------------------
        # Detect state transitions
        # ----------------------------------------------------

        if previous is not None and previous != current:

            status = "ONLINE" if current else "OFFLINE"

            log.warning(
                "%s (%s): %s",
                hostname,
                HOSTS[hostname],
                status
            )

            publish_alert(hostname, current)

        # ----------------------------------------------------
        # Publish MQTT status
        # ----------------------------------------------------

        if last_published.get(hostname) != current:

            status = "online" if current else "offline"

            if publish(
                f"{hostname}/status",
                status
            ):

                last_published[hostname] = current

        # ----------------------------------------------------
        # Diagnostic logging
        # ----------------------------------------------------

        log.debug(
            "%s: %s | failures=%d | recoveries=%d",
            hostname,
            "ONLINE" if current else "OFFLINE",
            failure_counts[hostname],
            recovery_counts[hostname]
        )


# ============================================================
# Monitor Heartbeat
# ============================================================

def send_heartbeat():

    global last_heartbeat

    now = time.time()

    if now - last_heartbeat < HEARTBEAT_INTERVAL:
        return

    online_count = sum(
        1 for status in latest_status.values() if status
    )

    offline_count = sum(
        1 for status in latest_status.values() if not status
    )

    payload = {
        "monitor": MONITOR_NAME,
        "status": "online",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "hosts_monitored": len(HOSTS),
        "hosts_online": online_count,
        "hosts_offline": offline_count
    }

    if publish(
        "heartbeat",
        json.dumps(payload)
    ):

        last_heartbeat = now


# ============================================================
# Main
# ============================================================

def main():

    log.info("Starting %s", MONITOR_NAME)
    log.info("Monitoring %d hosts", len(HOSTS))
    log.info("MQTT broker: %s:%d", MQTT_HOST, MQTT_PORT)
    log.info("Polling interval: %d seconds", POLL_INTERVAL)
    log.info("Ping timeout: %d seconds", PING_TIMEOUT)
    log.info("Failure threshold: %d", FAILURE_THRESHOLD)
    log.info("Recovery threshold: %d", RECOVERY_THRESHOLD)
    log.info("Concurrent workers: %d", MAX_WORKERS)

    for hostname, address in HOSTS.items():
        log.info("Registered host: %s (%s)", hostname, address)

    client.connect_async(
        MQTT_HOST,
        MQTT_PORT,
        keepalive=60
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

                sleep_time = max(
                    0,
                    POLL_INTERVAL - elapsed
                )

                if running:
                    time.sleep(sleep_time)

    finally:

        log.info("Stopping monitor")

        if client.is_connected():

            publish("availability", "offline")

            # Allow queued messages to finish
            client.disconnect()

        client.loop_stop()

        log.info("Monitor stopped")


# ============================================================
# Entry Point
# ============================================================

if __name__ == "__main__":

    try:
        main()

    except Exception:
        log.exception("Fatal monitor error")
        raise
