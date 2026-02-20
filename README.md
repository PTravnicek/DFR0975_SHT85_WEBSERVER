# FireBeetle 2 (ESP32-S3) SHT85 Web Logger (DFR0975)

Standalone temperature + humidity data logger with an onboard web UI.

- **Board:** DFRobot FireBeetle 2 ESP32-S3 (16 MB flash / PSRAM)
- **Sensor:** SHT85 (I2C, default address `0x44`)
- **UI:** Retro DOS-style dashboard (ApexCharts)
- **Storage:** LittleFS (primary, fixed-size ring-buffer log files) + optional SD (append-only CSV)

![Web Interface Screenshot](screenshot.png)

*Web UI: per-sensor live status, temperature/humidity charts (50 points), and chunked CSV download.*

## Features

- **Access Point mode** (default): device creates its own WiFi network
- **Web UI**: live status + interactive charts + CSV download
- **LittleFS logging**: fixed-size ring-buffer per month (prevents full-disk issues)
- **SD logging (optional)**: monthly CSV files (human-readable)
- **RTC support (optional)**: PCF8523
- **Storage status**: shows free space + estimated retention
- **Pruning**: delete samples older than N days (safe for ring-buffer logs)
- **Sensor heater control**: configurable periodic heater cycles

## Wiring (FireBeetle 2 ESP32-S3 defaults used by this firmware)

- **LED:** GPIO 21
- **I2C:** SDA=GPIO 1, SCL=GPIO 2
- **SD CS:** GPIO 9

> If your wiring differs, update `LED_PIN`, `I2C_SDA`, `I2C_SCL`, `SD_CS` in `src/main.cpp`.

## Build / Flash (PlatformIO)

### Build firmware

```bash
~/.platformio/penv/bin/pio run
```

### Build LittleFS image

```bash
~/.platformio/penv/bin/pio run -t buildfs
```

### Upload (when hardware is connected)

```bash
~/.platformio/penv/bin/pio run -t upload
~/.platformio/penv/bin/pio run -t uploadfs
```

## Web UI

- Connect to the device AP (printed on serial on boot)
- Open: `http://192.168.4.1/`

### Time setup

If the device time is not set (no RTC + never set by browser), the UI will prompt you to set it.

## API

- `GET /api/status` – device status + (optional) live readings
- `GET /api/config` – current configuration
- `POST /api/config` – update configuration (JSON body)
- `GET /api/time` – time status
- `POST /api/time` – set device time (JSON: `{ "epoch": <seconds> }`)
- `GET /api/storage` – storage status + retention estimate
- `GET /api/prune?days=N` – delete samples older than N days
- `GET /api/data?from=<ISO8601Z>&to=<ISO8601Z>` – JSON points for charts
- `GET /api/download?from=<ISO8601Z>&to=<ISO8601Z>` – CSV download
- `GET /api/files` – list log files
- `GET /api/test-sd` – SD read/write test

### Timestamp format

All timestamps are **UTC** ISO8601 with `Z`, e.g.:

`2026-02-05T12:00:00Z`

## Data format

CSV rows (SD export and API download) are:

```csv
timestamp,device_id,temperature_c,humidity_rh
2026-02-05T12:00:00Z,LOGGER_00112233aabbccdd,22.50,45.30
```

## Partitions / OTA

This project uses a custom partition table: `huge_app.csv`.

- Adds **dual OTA slots** (`ota_0` + `ota_1`) so OTA can be supported in the future.
- Provides a large filesystem partition for LittleFS.

If you change partitions, you must do a **full flash** (and usually re-upload FS).

## Notes

- LittleFS monthly log files use a custom **RB1 ring-buffer** layout. The prune endpoint preserves this format.
- Password hashing uses SHA-256 (legacy weak hashes are upgraded on successful login).

## License

MIT (see `LICENSE`).
