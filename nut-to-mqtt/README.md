# NUT to MQTT Monitor — Home Assistant add-on

This add-on polls the QNAP NUT server and publishes UPS telemetry and transition-based alerts to MQTT.

## Configuration

All settings are configured in **Settings → Add-ons → NUT to MQTT Monitor → Configuration**.
There is no `.env` file and no environment-variable configuration. Every application setting is exposed in the add-on Configuration UI; the application reads Home Assistant's `/data/options.json`.

The default NUT host is `192.168.86.252`, port `3493`, UPS name `ups`.
The MQTT default is `127.0.0.1:1883`, assuming the broker is reachable from the HA host network.

## MQTT topics

Under the configured base topic (default `ups-monitor`):

- `availability` — retained `online` / `offline` (Last Will)
- `heartbeat` — retained JSON monitor health
- `status` — retained NUT `ups.status` string
- `battery` — retained battery percentage
- `runtime` — retained remaining runtime in seconds
- `voltage` — retained input voltage
- `telemetry` — retained JSON telemetry
- `nut_connection` — retained NUT connection state when a poll fails

Alerts are published non-retained to the configured alert topic (default `discord-ha/alerts`) using:
`title`, `message`, `severity`, and `source`.

## QNAP setup

Keep the QNAP USB UPS service and its own shutdown protection enabled.
Enable QNAP Network UPS Master and allow the Home Assistant host IP as a client.
This add-on only reads NUT variables; it does not send UPS shutdown or control commands.

## Install

1. Copy the `nut-mqtt-monitor` folder into your add-on repository.
2. Add the repository in Home Assistant if it is not already configured.
3. Install **NUT to MQTT Monitor** from the local add-on store.
4. Configure all settings in the add-on UI and start it.

The `repository.yaml` file at the repository root is a template; replace the GitHub URL and maintainer before publishing.
