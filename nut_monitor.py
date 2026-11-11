#!/usr/bin/env python3
"""Home Assistant NUT-to-MQTT monitor.

All configuration is supplied by the Home Assistant add-on UI and read from
/data/options.json. This service only reads NUT variables; it never issues
UPS control commands.
"""
from __future__ import annotations

import json
import logging
import re
import signal
import socket
import sys
import time
from typing import Any

import paho.mqtt.client as mqtt

OPTIONS_FILE = "/data/options.json"
LOG = logging.getLogger("nut-mqtt-monitor")
RUNNING = True


def stop_handler(_signum: int, _frame: Any) -> None:
    global RUNNING
    RUNNING = False


signal.signal(signal.SIGTERM, stop_handler)
signal.signal(signal.SIGINT, stop_handler)


def load_options() -> dict[str, Any]:
    try:
        with open(OPTIONS_FILE, "r", encoding="utf-8") as handle:
            options = json.load(handle)
    except FileNotFoundError:
        raise RuntimeError(f"Home Assistant options file not found: {OPTIONS_FILE}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON in {OPTIONS_FILE}: {exc}") from exc

    if not isinstance(options, dict):
        raise RuntimeError("Add-on options must be a JSON object.")

    required = ("nut_host", "nut_ups_name", "mqtt_host", "mqtt_base_topic")
    missing = [key for key in required if not str(options.get(key, "")).strip()]
    if missing:
        raise RuntimeError("Missing required add-on options: " + ", ".join(missing))

    options.setdefault("nut_port", 3493)
    options.setdefault("nut_timeout", 5)
    options.setdefault("mqtt_port", 1883)
    options.setdefault("mqtt_client_id", "nut-mqtt-monitor")
    options.setdefault("mqtt_alert_topic", "discord-ha/alerts")
    options.setdefault("mqtt_qos", 1)
    options.setdefault("poll_interval", 15)
    options.setdefault("heartbeat_interval", 60)
    return options


def nut_quote(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')


def read_nut_variables(options: dict[str, Any]) -> dict[str, str]:
    """Read all variables for the configured UPS using the NUT text protocol."""
    host = str(options["nut_host"])
    port = int(options["nut_port"])
    ups = str(options["nut_ups_name"])
    timeout = int(options["nut_timeout"])

    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.settimeout(timeout)
        stream = sock.makefile("rwb", buffering=0)

        username = str(options.get("nut_username", "") or "")
        password = str(options.get("nut_password", "") or "")
        if username or password:
            if not username or not password:
                raise RuntimeError("Both NUT username and password are required when authentication is configured.")
            stream.write(f'USERNAME "{nut_quote(username)}"\n'.encode())
            response = stream.readline().decode("utf-8", "replace").strip()
            if response != "OK":
                raise RuntimeError(f"NUT username rejected: {response}")
            stream.write(f'PASSWORD "{nut_quote(password)}"\n'.encode())
            response = stream.readline().decode("utf-8", "replace").strip()
            if response != "OK":
                raise RuntimeError(f"NUT password rejected: {response}")

        stream.write(f"LIST VAR {ups}\n".encode())
        first = stream.readline().decode("utf-8", "replace").strip()
        expected = f"BEGIN LIST VAR {ups}"
        if first != expected:
            raise RuntimeError(f"NUT LIST VAR failed: {first or 'empty response'}")

        variables: dict[str, str] = {}
        line_pattern = re.compile(r'^VAR\s+\S+\s+([^\s]+)\s+"(.*)"$')
        while True:
            raw = stream.readline()
            if not raw:
                raise RuntimeError("NUT server closed connection before completing LIST VAR.")
            line = raw.decode("utf-8", "replace").strip()
            if line == f"END LIST VAR {ups}":
                break
            match = line_pattern.match(line)
            if match:
                key, value = match.groups()
                value = value.replace(r'\"', '"').replace(r"\\", "\\")
                variables[key] = value

        if not variables:
            raise RuntimeError("NUT server returned no UPS variables.")
        return variables


def as_float(values: dict[str, str], key: str) -> float | None:
    try:
        return float(values[key])
    except (KeyError, TypeError, ValueError):
        return None


def make_telemetry(values: dict[str, str], ups_name: str) -> dict[str, Any]:
    status_tokens = (values.get("ups.status") or "UNKNOWN").upper().split()
    battery_status = (values.get("battery.status") or "unknown").lower()

    def number(key: str) -> float | None:
        return as_float(values, key)

    charge = number("battery.charge")
    runtime = number("battery.runtime")
    voltage = number("input.voltage")
    output_voltage = number("output.voltage")
    load = number("ups.load")

    return {
        "ups_name": ups_name,
        "ups_model": values.get("device.model") or values.get("ups.model"),
        "manufacturer": values.get("device.mfr") or values.get("ups.mfr"),
        "status": " ".join(status_tokens),
        "on_battery": "OB" in status_tokens,
        "online": "OL" in status_tokens,
        "low_battery": "LB" in status_tokens,
        "replace_battery": "RB" in status_tokens or "replace" in battery_status,
        "battery_status": battery_status,
        "battery_charge": charge,
        "battery_runtime": int(runtime) if runtime is not None else None,
        "input_voltage": voltage,
        "output_voltage": output_voltage,
        "ups_load": load,
        "last_updated": int(time.time()),
    }


class NutMqttMonitor:
    def __init__(self, options: dict[str, Any]) -> None:
        self.o = options
        self.base = str(options["mqtt_base_topic"]).strip("/")
        self.alert_topic = str(options.get("mqtt_alert_topic", "discord-ha/alerts")).strip("/")
        self.qos = int(options.get("mqtt_qos", 1))
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=str(options.get("mqtt_client_id", "nut-mqtt-monitor")),
            protocol=mqtt.MQTTv311,
        )
        username = str(options.get("mqtt_username", "") or "")
        password = str(options.get("mqtt_password", "") or "")
        if username:
            self.client.username_pw_set(username, password or None)

        self.client.will_set(
            f"{self.base}/availability", "offline", qos=self.qos, retain=True
        )
        self.client.on_connect = self.on_connect
        self.client.on_disconnect = self.on_disconnect
        self.connected = False
        self.last_heartbeat = 0.0
        self.last_telemetry: dict[str, Any] | None = None
        self.last_nut_ok = False
        self.alert_states: dict[str, bool] = {}

    def on_connect(self, client: mqtt.Client, _userdata: Any, _flags: Any,
                   reason_code: Any, _properties: Any) -> None:
        self.connected = reason_code == 0
        if self.connected:
            LOG.info("Connected to MQTT broker.")
            self.publish("availability", "online", retain=True)
        else:
            LOG.error("MQTT connection rejected: %s", reason_code)

    def on_disconnect(self, _client: mqtt.Client, _userdata: Any,
                      _disconnect_flags: Any, reason_code: Any,
                      _properties: Any) -> None:
        self.connected = False
        LOG.warning("Disconnected from MQTT broker: %s", reason_code)

    def publish(self, suffix: str, payload: Any, retain: bool = True) -> None:
        if not self.connected:
            return
        topic = f"{self.base}/{suffix}"
        if not isinstance(payload, str):
            payload = json.dumps(payload, separators=(",", ":"), allow_nan=False)
        result = self.client.publish(topic, payload, qos=self.qos, retain=retain)
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOG.warning("MQTT publish failed for %s: rc=%s", topic, result.rc)

    def alert(self, key: str, active: bool, title: str, message: str,
              severity: str = "warning", enabled: bool = True,
              recovery_enabled: bool = True, recovery_message: str | None = None) -> None:
        previous = self.alert_states.get(key, False)
        self.alert_states[key] = active
        if active == previous or not self.connected or not self.alert_topic:
            return
        if active and not enabled:
            return
        if not active and not recovery_enabled:
            return
        if active:
            payload = {
                "title": title,
                "message": message,
                "severity": severity,
                "source": "Home Assistant",
            }
        else:
            payload = {
                "title": title.replace("ALERT", "RECOVERED"),
                "message": recovery_message or f"Condition cleared: {message}",
                "severity": "info",
                "source": "Home Assistant",
            }
        result = self.client.publish(
            self.alert_topic,
            json.dumps(payload, separators=(",", ":")),
            qos=self.qos,
            retain=False,
        )
        if result.rc != mqtt.MQTT_ERR_SUCCESS:
            LOG.warning("Alert publish failed: rc=%s", result.rc)

    def evaluate(self, telemetry: dict[str, Any]) -> None:
        o = self.o
        on_battery = bool(telemetry["on_battery"])
        charge = telemetry["battery_charge"]
        runtime = telemetry["battery_runtime"]
        low_battery = bool(telemetry["low_battery"])
        replace_battery = bool(telemetry["replace_battery"])

        self.alert(
            "power_loss", on_battery, "UPS POWER ALERT",
            "Utility power lost. UPS operating on battery.",
            enabled=bool(o.get("alert_on_power_loss", True)),
            recovery_enabled=bool(o.get("alert_on_power_restore", True)),
            recovery_message="Utility power restored. UPS is back on mains power.",
        )

        charge_critical = low_battery or (
            charge is not None and charge <= int(o.get("battery_critical_threshold", 15))
        )
        charge_warning = (
            charge is not None
            and charge <= int(o.get("battery_warning_threshold", 30))
            and not charge_critical
        )
        self.alert(
            "battery_warning", charge_warning, "UPS BATTERY ALERT",
            f"UPS battery charge is {charge:g}% (warning threshold).",
            severity="warning",
            enabled=bool(o.get("alert_on_low_battery", True)),
        )
        self.alert(
            "battery_critical", charge_critical, "UPS BATTERY ALERT",
            f"UPS battery charge is {charge:g}% (critical threshold)." if charge is not None else "UPS reports a low battery condition.",
            severity="critical",
            enabled=bool(o.get("alert_on_low_battery", True)),
        )

        runtime_critical = runtime is not None and runtime <= int(o.get("runtime_critical_seconds", 300))
        runtime_warning = (
            runtime is not None
            and runtime <= int(o.get("runtime_warning_seconds", 600))
            and not runtime_critical
        )
        self.alert(
            "runtime_warning", runtime_warning, "UPS RUNTIME ALERT",
            f"Estimated UPS runtime is {int(runtime)} seconds (warning threshold).",
            severity="warning",
            enabled=bool(o.get("alert_on_low_runtime", True)),
        )
        self.alert(
            "runtime_critical", runtime_critical, "UPS RUNTIME ALERT",
            f"Estimated UPS runtime is {int(runtime)} seconds (critical threshold).",
            severity="critical",
            enabled=bool(o.get("alert_on_low_runtime", True)),
        )

        self.alert(
            "replace_battery", replace_battery, "UPS BATTERY ALERT",
            "UPS reports that its battery needs replacement.",
            enabled=bool(o.get("alert_on_replace_battery", True)),
        )

    def publish_telemetry(self, telemetry: dict[str, Any]) -> None:
        self.publish("status", telemetry["status"])
        self.publish("battery", telemetry["battery_charge"])
        self.publish("runtime", telemetry["battery_runtime"])
        self.publish("voltage", telemetry["input_voltage"])
        self.publish("telemetry", telemetry)
        self.publish("heartbeat", {
            "status": "online",
            "timestamp": int(time.time()),
            "nut_connected": True,
        })

    def run(self) -> None:
        host = str(self.o["mqtt_host"])
        port = int(self.o.get("mqtt_port", 1883))
        LOG.info("Starting NUT monitor for %s@%s:%s",
                 self.o["nut_ups_name"], self.o["nut_host"], self.o["nut_port"])
        self.client.connect_async(host, port, keepalive=60)
        self.client.loop_start()

        poll_interval = int(self.o.get("poll_interval", 15))
        heartbeat_interval = int(self.o.get("heartbeat_interval", 60))

        try:
            while RUNNING:
                started = time.monotonic()
                try:
                    values = read_nut_variables(self.o)
                    telemetry = make_telemetry(values, str(self.o["nut_ups_name"]))
                    if not self.last_nut_ok:
                        LOG.info("NUT connection established.")
                        self.alert(
                            "nut_disconnect", False, "UPS COMMUNICATION RECOVERED",
                            "Communication with the UPS has been restored.",
                            enabled=bool(self.o.get("alert_on_nut_reconnect", True)),
                        )
                    self.last_nut_ok = True
                    self.publish_telemetry(telemetry)
                    self.evaluate(telemetry)
                    self.last_telemetry = telemetry
                    LOG.debug("UPS status=%s battery=%s runtime=%s",
                              telemetry["status"], telemetry["battery_charge"],
                              telemetry["battery_runtime"])
                except Exception as exc:
                    LOG.warning("Unable to read NUT server: %s", exc)
                    if self.last_nut_ok or "nut_disconnect" not in self.alert_states:
                        self.alert(
                            "nut_disconnect", True, "UPS COMMUNICATION ALERT",
                            f"Unable to communicate with NUT server at {self.o['nut_host']}:{self.o['nut_port']}.",
                            severity="critical",
                            enabled=bool(self.o.get("alert_on_nut_disconnect", True)),
                        )
                    self.last_nut_ok = False
                    self.publish("nut_connection", "offline")
                    self.publish("heartbeat", {
                        "status": "degraded",
                        "timestamp": int(time.time()),
                        "nut_connected": False,
                    })

                now = time.monotonic()
                if now - self.last_heartbeat >= heartbeat_interval:
                    self.publish("heartbeat", {
                        "status": "online" if self.last_nut_ok else "degraded",
                        "timestamp": int(time.time()),
                        "nut_connected": self.last_nut_ok,
                    })
                    self.last_heartbeat = now

                elapsed = time.monotonic() - started
                sleep_for = max(1, poll_interval - elapsed)
                for _ in range(int(sleep_for)):
                    if not RUNNING:
                        break
                    time.sleep(1)
        finally:
            try:
                if self.connected:
                    self.publish("availability", "offline", retain=True)
                self.client.loop_stop()
                self.client.disconnect()
            except Exception:
                pass


def main() -> int:
    try:
        options = load_options()
        log_level = str(options.get("log_level", "INFO")).upper()
        logging.basicConfig(
            level=getattr(logging, log_level, logging.INFO),
            format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        )
        monitor = NutMqttMonitor(options)
        monitor.run()
        return 0
    except Exception as exc:
        LOG.exception("Fatal startup error: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
