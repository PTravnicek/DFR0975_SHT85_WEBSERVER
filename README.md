# ESP8266 Temperature & Humidity Logger

A standalone ESP8266-based data logger for temperature and humidity monitoring with a web-based interface. Built for the Adafruit HUZZAH ESP8266 board with SI7021 sensor, featuring SD card storage, authentication, and real-time data visualization.

![Web Interface Screenshot](screenshot.png)

## Features

- **Temperature & Humidity Monitoring**: Continuous logging using Adafruit SI7021 sensor
- **Web Interface**: Modern retro DOS-style web UI with real-time charts
- **SD Card Storage**: Persistent data logging to SD card with monthly file rotation
- **LittleFS Storage**: On-board filesystem for configuration and web assets
- **Authentication**: Secure password-protected access to the web interface
- **Real-time Charts**: Interactive ApexCharts visualization with synchronized zoom/pan
- **Time Management**: RTC support (PCF8523) with timezone configuration
- **Data Export**: CSV download functionality for data analysis
- **Auto-refresh**: Configurable automatic data refresh intervals
- **Storage Management**: Built-in storage monitoring and data pruning tools
- **Access Point Mode**: Creates its own WiFi network for easy setup

## Hardware Requirements

- **Adafruit HUZZAH ESP8266** (or compatible ESP8266 board)
- **Adafruit SI7021** Temperature & Humidity Sensor
- **SD Card Module** (SPI interface)
- **PCF8523 RTC Module** (optional, for accurate timekeeping)
- **MicroSD Card** (formatted as FAT32)
- **Breadboard and jumper wires** for connections

### Wiring

#### SI7021 Sensor
- **VCC** → 3.3V
- **GND** → GND
- **SCL** → GPIO 5 (D1)
- **SDA** → GPIO 4 (D2)

#### SD Card Module
- **VCC** → 3.3V
- **GND** → GND
- **MISO** → GPIO 12 (D6)
- **MOSI** → GPIO 13 (D7)
- **SCK** → GPIO 14 (D5)
- **CS** → GPIO 15 (D8)

#### PCF8523 RTC (Optional)
- **VCC** → 3.3V
- **GND** → GND
- **SDA** → GPIO 4 (D2) - shared with SI7021
- **SCL** → GPIO 5 (D1) - shared with SI7021

## Software Requirements

- **PlatformIO** (IDE or CLI)
- **Arduino Framework** (configured via PlatformIO)

## Installation

### 1. Clone the Repository

```bash
git clone https://github.com/yourusername/esp8266-temp-humidity-logger.git
cd esp8266-temp-humidity-logger
```

### 2. Install PlatformIO

If you haven't already, install PlatformIO:

- **VS Code Extension**: Install "PlatformIO IDE" from the VS Code marketplace
- **CLI**: Follow instructions at [platformio.org](https://platformio.org/install/cli)

### 3. Install Dependencies

PlatformIO will automatically install the required libraries specified in `platformio.ini`:

- Adafruit RTClib (v2.1.1+)
- ArduinoJson (v6.21.3+)
- Adafruit SI7021 Library (v1.5.3+)

### 4. Upload Filesystem

Before uploading the main code, you need to upload the web interface files to LittleFS:

```bash
pio run --target uploadfs
```

This uploads the files from the `data/` directory to the ESP8266's LittleFS filesystem.

### 5. Build and Upload

```bash
pio run --target upload
```

### 6. Monitor Serial Output

```bash
pio device monitor
```

The default baud rate is 115200. You should see:
- WiFi AP credentials
- Device ID
- Sample interval
- Web server status

## Configuration

### Initial Setup

1. **Connect to Access Point**: The device creates a WiFi access point named `LOGGER_<chipid>` with password `logger123`
2. **Access Web Interface**: Open a browser and navigate to `http://192.168.4.1`
3. **Login**: Default credentials are:
   - Username: (not required)
   - Password: `admin`
4. **Set Device Time**: The interface will prompt you to set the device time on first access
5. **Configure Settings**: Access the Settings menu to configure:
   - Sampling interval (seconds, minutes, hours, or days)
   - Timezone offset
   - File rotation settings

### Configuration File

The device stores configuration in `/config.json` on LittleFS. You can modify settings via the web interface or by editing the config file directly (requires filesystem access).

## Usage

### Web Interface

The web interface provides:

- **Live Status**: Real-time temperature and humidity readings
- **Charts**: Interactive temperature and humidity graphs with:
  - Time range selection (24h, 7d, 30d, 6mo, 1y)
  - Custom date range picker
  - Synchronized zoom and pan
  - Auto-refresh capability
- **Data Management**: 
  - View data point count
  - Download CSV files
  - Monitor storage usage
  - Prune old data
- **Settings**: Configure sampling interval and timezone

### API Endpoints

The device exposes a REST API for programmatic access:

- `GET /api/status` - Device status and sensor readings
- `GET /api/config` - Current configuration
- `POST /api/config` - Update configuration
- `GET /api/data?from=<ISO8601>&to=<ISO8601>` - Retrieve data points
- `GET /api/storage` - Storage information
- `POST /api/prune` - Delete old data
- `GET /api/download?from=<ISO8601>&to=<ISO8601>` - Download CSV
- `POST /api/time` - Set device time
- `GET /api/files` - List log files
- `POST /api/test-sd` - Test SD card

### Data Format

Data is stored in CSV format on the SD card:

```csv
timestamp,temperature,humidity
2026-01-20T12:00:00Z,22.5,45.3
2026-01-20T13:00:00Z,22.7,45.1
```

Files are organized by month: `/logs/YYYY-MM.csv`

## Project Structure

```
esp8266-temp-humidity-logger/
├── data/                    # Web interface files (uploaded to LittleFS)
│   ├── index.html          # Main web interface
│   ├── styles.css          # Retro DOS-style CSS
│   ├── app.js              # Frontend JavaScript
│   ├── apexcharts.min.js   # Charting library
│   ├── apexcharts-sync-plugin.js  # Chart synchronization
│   └── ModernDOS8x16.ttf   # Retro font
├── src/
│   └── main.cpp            # Main firmware code
├── platformio.ini          # PlatformIO configuration
├── .gitignore              # Git ignore rules
├── LICENSE                 # MIT License
├── README.md               # This file
└── screenshot.png          # Web interface screenshot
```

## Development

### Building

```bash
pio run
```

### Uploading

```bash
pio run --target upload
```

### Uploading Filesystem

```bash
pio run --target uploadfs
```

### Serial Monitor

```bash
pio device monitor
```

### Clean Build

```bash
pio run --target clean
```

## Troubleshooting

### SD Card Not Detected

- Ensure the SD card is formatted as FAT32
- Check wiring connections
- Verify CS pin configuration
- Try a different SD card

### Sensor Not Reading

- Verify I2C connections (SDA/SCL)
- Check sensor power (3.3V)
- Ensure sensor is properly initialized in code

### Web Interface Not Loading

- Verify filesystem was uploaded (`pio run --target uploadfs`)
- Check serial output for filesystem mount errors
- Clear browser cache

### Time Not Set

- Set time via web interface
- Ensure RTC is properly connected (if using)
- Check timezone offset configuration

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## Acknowledgments

- Adafruit for hardware and libraries
- PlatformIO for the development platform
- ApexCharts for data visualization
