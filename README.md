# Network Monitor

Home Assistant app for ICMP host monitoring with MQTT status/heartbeat publishing and Discord alerts over MQTT.

## Install from this repository

1. In Home Assistant, open **Settings → Apps → App Store**.
2. Open the store menu and choose **Repositories**.
3. Add `https://github.com/ikarosghoste/ha-network-monitor`.
4. Refresh the store and locate **Network Monitor** under this repository.
5. Install that repository's app and configure it in the app's **Configuration** tab.

## Important: local and GitHub installations are different apps

Home Assistant prefixes each app's `slug` with its repository ID. The built-in local repository ID is `local`, so an installed ID of `local_network_monitor` is the local-folder copy, not the copy from this GitHub repository.

For this exact GitHub URL, Home Assistant's repository ID is `205fb5e6`; the full app ID is therefore `205fb5e6_network_monitor`. A GitHub repository update will not update `local_network_monitor`, because those are different app identities and sources.

To use the GitHub-managed copy, add the repository as above and install `205fb5e6_network_monitor` from the store. Keep the local copy until the GitHub-managed copy is installed, configured, and verified. Copy the existing app's options into the new app's Configuration tab before starting it. Do not uninstall the working local app until the replacement is confirmed.

## Configuration

All settings are exposed in the app UI and stored by Home Assistant in `/data/options.json`. There is no `.env` file and no `python-dotenv` dependency.

The `hosts` setting is a comma-separated list of `name=address` pairs, for example:

`thoth=192.168.86.252,omoikane=192.168.86.253`

## MQTT topics

With the default base topic `monitor`, the app publishes:

- `monitor/availability` — retained `online`/`offline` availability.
- `monitor/heartbeat` — retained monitor heartbeat JSON.
- `monitor/<hostname>/status` — retained host state.
- The configured Discord alert topic — non-retained JSON on host state transitions.

## Logging

Startup configuration and MQTT connection status are logged at INFO. A heartbeat summary is logged at the configured heartbeat interval. Set `log_host_status` to true to log each host's current status at INFO on each polling cycle. Host state transitions are logged at WARNING.
