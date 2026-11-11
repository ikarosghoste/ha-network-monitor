# Changelog

## 1.1.2

- Publish a critical Discord alert when a host is first confirmed offline after startup; previously initial offline discovery was silently suppressed.
- Log the configured alert topic and distinguish an alert queued for MQTT delivery from a confirmed broker acknowledgement.


## 1.1.1

- Log per-host status at INFO when `log_host_status` is enabled; previously the setting only emitted DEBUG messages and appeared inactive at the default INFO level.
- Document custom-repository installation and Home Assistant's repository-prefixed app identifiers.

## 1.1.0

- Move app configuration to Home Assistant's `/data/options.json`.
- Expose MQTT, host, polling, threshold, and logging settings in the app UI.
- Add startup and heartbeat logging.
