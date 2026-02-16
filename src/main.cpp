#include <Arduino.h>
#include <WiFi.h>
#include <WebServer.h>
#include <LittleFS.h>
#include <SD.h>
#include <SPI.h>
#include <Wire.h>
#include <ArduinoJson.h>
#include <SHT85.h>
#include <RTClib.h>
#include <time.h>
#include <mbedtls/sha256.h>
#include <WebSocketsServer.h>

// Hardware objects (SHT85 I2C default address 0x44)
// ESP32-S3 has only 2 I2C peripherals: Wire and Wire1. Sensors 2 and 3 use software I2C
// (bit-bang) on their own SDA/SCL pairs so all 4 buses work with your existing wiring.
SHT85 sht(0x44, &Wire);
SHT85 sht1(0x44, &Wire1);

// ----- Software I2C for SHT85 on buses 3 and 4 (no hardware I2C on ESP32-S3 for bus 2/3) -----
static uint8_t sht85SoftCrc8(const uint8_t* data, uint8_t len) {
  const uint8_t POLY = 0x31;
  uint8_t crc = 0xFF;
  for (; len; --len) {
    crc ^= *data++;
    for (uint8_t i = 8; i; --i)
      crc = (crc & 0x80) ? (uint8_t)((crc << 1) ^ POLY) : (uint8_t)(crc << 1);
  }
  return crc;
}

static void softI2CDelay() {
  delayMicroseconds(3);
}

static void softI2CSetSda(int pin, int level) {
  if (level) {
    pinMode(pin, INPUT);
  } else {
    pinMode(pin, OUTPUT);
    digitalWrite(pin, LOW);
  }
}

static void softI2CSetScl(int pin, int level) {
  if (level) {
    pinMode(pin, INPUT);
  } else {
    pinMode(pin, OUTPUT);
    digitalWrite(pin, LOW);
  }
}

static int softI2CReadSda(int pin) {
  pinMode(pin, INPUT);
  return digitalRead(pin);
}

static void softI2CStart(int sda, int scl) {
  softI2CSetSda(sda, 1);
  softI2CSetScl(scl, 1);
  softI2CDelay();
  softI2CSetSda(sda, 0);
  softI2CDelay();
  softI2CSetScl(scl, 0);
}

static void softI2CStop(int sda, int scl) {
  softI2CSetSda(sda, 0);
  softI2CDelay();
  softI2CSetScl(scl, 1);
  softI2CDelay();
  softI2CSetSda(sda, 1);
  softI2CDelay();
}

static bool softI2CWriteByte(int sda, int scl, uint8_t byte) {
  for (int i = 7; i >= 0; i--) {
    softI2CSetSda(sda, (byte >> i) & 1);
    softI2CDelay();
    softI2CSetScl(scl, 1);
    softI2CDelay();
    softI2CSetScl(scl, 0);
  }
  softI2CSetSda(sda, 1);
  softI2CDelay();
  softI2CSetScl(scl, 1);
  softI2CDelay();
  int ack = !softI2CReadSda(sda);
  softI2CSetScl(scl, 0);
  return (bool)ack;
}

static uint8_t softI2CReadByte(int sda, int scl, bool ack) {
  uint8_t byte = 0;
  softI2CSetSda(sda, 1);
  for (int i = 7; i >= 0; i--) {
    softI2CSetScl(scl, 1);
    softI2CDelay();
    byte = (byte << 1) | (softI2CReadSda(sda) ? 1 : 0);
    softI2CSetScl(scl, 0);
  }
  softI2CSetSda(sda, ack ? 0 : 1);
  softI2CDelay();
  softI2CSetScl(scl, 1);
  softI2CDelay();
  softI2CSetScl(scl, 0);
  softI2CSetSda(sda, 1);
  return byte;
}

// SHT85 at 0x44 on bit-banged bus (sda, scl). Returns true if read OK; fills temp and hum.
static bool softSht85WriteCmd(int sda, int scl, uint16_t cmd) {
  const uint8_t addr = 0x44;
  pinMode(sda, INPUT);
  pinMode(scl, INPUT);
  softI2CStart(sda, scl);
  if (!softI2CWriteByte(sda, scl, (addr << 1) | 0)) {
    softI2CStop(sda, scl);
    return false;
  }
  if (!softI2CWriteByte(sda, scl, (uint8_t)(cmd >> 8)) || !softI2CWriteByte(sda, scl, (uint8_t)(cmd & 0xFF))) {
    softI2CStop(sda, scl);
    return false;
  }
  softI2CStop(sda, scl);
  return true;
}

static bool softSht85HeaterOn(int sda, int scl) {
  // SHT3x/SHT85 heater enable command
  return softSht85WriteCmd(sda, scl, 0x306D);
}

static bool softSht85HeaterOff(int sda, int scl) {
  // SHT3x/SHT85 heater disable command
  return softSht85WriteCmd(sda, scl, 0x3066);
}

static bool readSHT85Soft(int sda, int scl, float* temp, float* hum) {
  const uint8_t addr = 0x44;
  const uint16_t cmd = 0x2416;  // SHT_MEASUREMENT_FAST
  pinMode(sda, INPUT);
  pinMode(scl, INPUT);
  softI2CStart(sda, scl);
  if (!softI2CWriteByte(sda, scl, (addr << 1) | 0)) {
    softI2CStop(sda, scl);
    return false;
  }
  if (!softI2CWriteByte(sda, scl, cmd >> 8) || !softI2CWriteByte(sda, scl, cmd & 0xFF)) {
    softI2CStop(sda, scl);
    return false;
  }
  softI2CStop(sda, scl);
  delay(5);
  softI2CStart(sda, scl);
  if (!softI2CWriteByte(sda, scl, (addr << 1) | 1)) {
    softI2CStop(sda, scl);
    return false;
  }
  uint8_t buf[6];
  for (int i = 0; i < 6; i++)
    buf[i] = softI2CReadByte(sda, scl, (i < 5));
  softI2CStop(sda, scl);
  if (buf[2] != sht85SoftCrc8(buf, 2) || buf[5] != sht85SoftCrc8(buf + 3, 2)) {
    return false;
  }
  uint16_t rawT = (uint16_t)buf[0] << 8 | buf[1];
  uint16_t rawH = (uint16_t)buf[3] << 8 | buf[4];
  *temp = rawT * (175.0f / 65535.0f) - 45.0f;
  *hum = rawH * (100.0f / 65535.0f);
  return true;
}
static const int NUM_SENSORS = 4;
RTC_PCF8523 rtc;
bool rtcPresent = false;
bool sensorPresent[4] = { false, false, false, false };
#define sensorPresentAny() (sensorPresent[0] || sensorPresent[1] || sensorPresent[2] || sensorPresent[3])

// FireBeetle 2 ESP32-S3: onboard LED = GPIO 21 (D13), I2C default SDA=1 SCL=2, SD_CS=9
#define LED_PIN 21
#define SD_CS 9
#define I2C_SDA 1
#define I2C_SCL 2
#define I2C1_SDA 4
#define I2C1_SCL 5
#define I2C2_SDA 6
#define I2C2_SCL 8
#define I2C3_SDA 10
#define I2C3_SCL 11
unsigned long lastHeartbeat = 0;
const unsigned long HEARTBEAT_INTERVAL = 3000; // 3 seconds

// Network objects
WebServer server(80);
// WebSocket server (separate port so we can keep WebServer unchanged)
WebSocketsServer ws(81);
IPAddress apIP(192, 168, 4, 1);

// Config structure
struct Config {
  int config_version = 1;
  String device_id;
  String ap_ssid;
  String ap_password = "logger123";
  String auth_hash;
  String auth_salt;
  uint32_t sample_period_s = 3600;        // legacy; use sample_period_sensor[0] when per-sensor
  uint32_t sample_period_sensor[4] = { 3600, 3600, 3600, 3600 };
  int32_t log_timezone_offset_min = 0;
  String file_rotation = "monthly";
  bool time_set = false;
  String heating_mode = "off";            // legacy; use heating_mode_sensor[0] for S0 heater
  String heating_mode_sensor[4] = { "off", "off", "off", "off" };
  uint32_t heating_duration_sensor[4] = { 0, 0, 0, 0 };   // seconds heater on (0 = off)
  uint32_t heating_interval_sensor[4] = { 0, 0, 0, 0 };   // seconds between cycles (0 = off)
} config;

// Session management
struct AuthSession {
  String token;
  unsigned long expires;
};
AuthSession authSessions[10];
int sessionCount = 0;
const unsigned long SESSION_TIMEOUT = 3600000; // 1 hour

// Sampling (per-sensor intervals)
unsigned long lastSampleTime = 0;
unsigned long lastReadTime[4] = { 0, 0, 0, 0 };
unsigned long nextReadAtMs[4] = { 0, 0, 0, 0 }; // per-sensor scheduler
float lastT[4] = { NAN, NAN, NAN, NAN };
float lastH[4] = { NAN, NAN, NAN, NAN };
bool sdPresent = false;
uint32_t writeErrors = 0;

// SHT85 heater state machine (non-blocking) — per-sensor
// Heater duty cycle runs independently of sampling.
struct HeaterState {
  bool active = false;                 // true while heater is on
  unsigned long startMs = 0;           // when current heating cycle started
  unsigned long lastRetriggerMs = 0;   // when heatOn() was last called (for re-trigger for library timeout)
  unsigned long lastCycleEndMs = 0;    // when last heating cycle ended
};
HeaterState heater[NUM_SENSORS];

// LittleFS ring-buffer settings (match larger partition; was 256 KB)
#ifndef LFS_RING_MAX_BYTES_DEFAULT
#define LFS_RING_MAX_BYTES_DEFAULT (4 * 1024 * 1024)
#endif
#ifndef LFS_RING_RECORD_LEN_DEFAULT
#define LFS_RING_RECORD_LEN_DEFAULT 96
#endif
#define LFS_RING_RECORD_LEN_EXT 128  // 4-sensor format (timestamp,device_id,t0,h0,t1,h1,t2,h2,t3,h3)
#define LFS_RING_RECORD_LEN_SPARSE 48  // RB2: one record per sensor read (timestamp,sensor_id,t,h)
const size_t LFS_RING_MAX_BYTES = LFS_RING_MAX_BYTES_DEFAULT;
const size_t LFS_RING_RECORD_LEN = LFS_RING_RECORD_LEN_DEFAULT;

// Helper functions
String getChipId() {
  uint64_t mac = ESP.getEfuseMac();
  char buf[17];
  snprintf(buf, sizeof(buf), "%04x%08x", (uint32_t)(mac >> 32), (uint32_t)mac);
  return String(buf);
}

time_t parseISO8601(const String& iso);

String generateSessionToken() {
  // 128-bit token encoded as 32 hex chars
  uint32_t a = esp_random();
  uint32_t b = esp_random();
  uint32_t c = esp_random();
  uint32_t d = esp_random();
  char buf[33];
  snprintf(buf, sizeof(buf), "%08x%08x%08x%08x", a, b, c, d);
  return String(buf);
}

String bytesToHex(const uint8_t* bytes, size_t len) {
  const char* hex = "0123456789abcdef";
  String out;
  out.reserve(len * 2);
  for (size_t i = 0; i < len; i++) {
    out += hex[(bytes[i] >> 4) & 0x0F];
    out += hex[bytes[i] & 0x0F];
  }
  return out;
}

String hashPasswordLegacy(const String& password, const String& salt) {
  // Legacy (weak) 32-bit hash kept for backward-compat with existing config.json
  String combined = password + salt;
  uint32_t hash = 0;
  for (size_t i = 0; i < combined.length(); i++) {
    hash = ((hash << 5) + hash) + combined.charAt(i);
  }
  return String(hash, HEX);
}

String hashPasswordSHA256(const String& password, const String& salt) {
  String combined = password + salt;
  uint8_t out[32];

  mbedtls_sha256_context ctx;
  mbedtls_sha256_init(&ctx);
  mbedtls_sha256_starts_ret(&ctx, 0 /* is224 */);
  mbedtls_sha256_update_ret(&ctx, (const unsigned char*)combined.c_str(), combined.length());
  mbedtls_sha256_finish_ret(&ctx, out);
  mbedtls_sha256_free(&ctx);

  return bytesToHex(out, sizeof(out));
}

String hashPassword(const String& password, const String& salt) {
  // Default to strong hashing for new configs
  return hashPasswordSHA256(password, salt);
}

String generateSalt() {
  // 64-bit salt encoded as 16 hex chars
  uint32_t a = esp_random();
  uint32_t b = esp_random();
  char buf[17];
  snprintf(buf, sizeof(buf), "%08x%08x", a, b);
  return String(buf);
}

bool verifyPassword(const String& password, const String& hash, const String& salt) {
  if (hash.length() >= 64) {
    String computed = hashPasswordSHA256(password, salt);
    String h = hash;
    computed.toLowerCase();
    h.toLowerCase();
    return computed == h;
  }
  String computed = hashPasswordLegacy(password, salt);
  return computed == hash;
}

String getSessionToken() {
  if (!server.hasHeader("Cookie")) {
    return "";
  }
  String cookie = server.header("Cookie");
  int start = cookie.indexOf("SESSION=");
  if (start == -1) return "";
  start += 8;
  int end = cookie.indexOf(";", start);
  if (end == -1) end = cookie.length();
  return cookie.substring(start, end);
}

bool isAuthenticated() {
  String token = getSessionToken();
  if (token.length() == 0) return false;
  
  unsigned long now = millis();
  for (int i = 0; i < sessionCount; i++) {
    if (authSessions[i].token == token && authSessions[i].expires > now) {
      return true;
    }
  }
  return false;
}

void addSession(const String& token) {
  if (sessionCount >= 10) {
    // Remove oldest session
    for (int i = 0; i < 9; i++) {
      authSessions[i] = authSessions[i + 1];
    }
    sessionCount = 9;
  }
  authSessions[sessionCount].token = token;
  authSessions[sessionCount].expires = millis() + SESSION_TIMEOUT;
  sessionCount++;
}

void removeSession(const String& token) {
  for (int i = 0; i < sessionCount; i++) {
    if (authSessions[i].token == token) {
      for (int j = i; j < sessionCount - 1; j++) {
        authSessions[j] = authSessions[j + 1];
      }
      sessionCount--;
      break;
    }
  }
}

bool requireAuth() {
  if (!isAuthenticated()) {
    server.sendHeader("Location", "/login");
    server.send(302, "text/plain", "");
    return false;
  }
  return true;
}

bool requireApiAuth() {
  if (!isAuthenticated()) {
    server.send(401, "application/json", "{\"error\":\"Unauthorized\"}");
    return false;
  }
  return true;
}

// Config management
bool loadConfig() {
  if (!LittleFS.exists("/config.json")) {
    return false;
  }
  
  File file = LittleFS.open("/config.json", "r");
  if (!file) {
    return false;
  }
  
  String content = file.readString();
  file.close();
  
  DynamicJsonDocument doc(1024);
  DeserializationError error = deserializeJson(doc, content);
  if (error) {
    return false;
  }
  
  config.config_version = doc["config_version"] | 1;
  {
    const char* p;
    p = doc["device_id"].as<const char*>();
    config.device_id = (p && p[0]) ? String(p) : ("LOGGER_" + getChipId());
    p = doc["ap_ssid"].as<const char*>();
    config.ap_ssid = (p && p[0]) ? String(p) : ("LOGGER_" + getChipId());
    p = doc["ap_password"].as<const char*>();
    config.ap_password = (p && p[0]) ? String(p) : "logger123";
    if (config.ap_password.length() < 8 || config.ap_password.length() > 63) {
      config.ap_password = "logger123";
    }
    p = doc["auth_hash"].as<const char*>();
    config.auth_hash = p ? String(p) : "";
    p = doc["auth_salt"].as<const char*>();
    config.auth_salt = p ? String(p) : "";
    p = doc["file_rotation"].as<const char*>();
    config.file_rotation = (p && p[0]) ? String(p) : "monthly";
    p = doc["heating_mode"].as<const char*>();
    config.heating_mode = (p && p[0]) ? String(p) : "off";
  }
  config.sample_period_s = doc["sample_period_s"] | 3600;
  if (doc["sample_period_sensor"].is<JsonArray>() && doc["sample_period_sensor"].size() >= 4) {
    for (size_t i = 0; i < 4; i++) {
      uint32_t v = doc["sample_period_sensor"][i] | 3600;
      if (v < 10) v = 10;
      if (v > 604800) v = 604800;
      config.sample_period_sensor[i] = v;
    }
    config.sample_period_s = config.sample_period_sensor[0];
  } else {
    uint32_t single = config.sample_period_s;
    if (single < 10) single = 10;
    if (single > 604800) single = 604800;
    for (size_t i = 0; i < 4; i++) config.sample_period_sensor[i] = single;
  }
  // Heating: new duration/interval or legacy mode_sensor
  if (doc["heating_duration_sensor"].is<JsonArray>() && doc["heating_interval_sensor"].is<JsonArray>() &&
      doc["heating_duration_sensor"].size() >= 4 && doc["heating_interval_sensor"].size() >= 4) {
    for (size_t i = 0; i < 4; i++) {
      uint32_t interval = doc["heating_interval_sensor"][i] | 0;
      uint32_t duration = doc["heating_duration_sensor"][i] | 0;
      if (interval == 0) duration = 0;
      else if (duration > interval) duration = interval;
      config.heating_interval_sensor[i] = interval;
      config.heating_duration_sensor[i] = duration;
    }
    // Derive legacy mode for compatibility
    for (size_t i = 0; i < 4; i++) {
      uint32_t d = config.heating_duration_sensor[i];
      uint32_t inv = config.heating_interval_sensor[i];
      if (inv == 0) config.heating_mode_sensor[i] = "off";
      else if (d == 10 && inv == 300) config.heating_mode_sensor[i] = "10s_5min";
      else if (d == 60 && inv == 3600) config.heating_mode_sensor[i] = "1min_1hr";
      else if (d == 60 && inv == 86400) config.heating_mode_sensor[i] = "1min_1day";
      else config.heating_mode_sensor[i] = "off";
    }
    config.heating_mode = config.heating_mode_sensor[0];
  } else if (doc["heating_mode_sensor"].is<JsonArray>() && doc["heating_mode_sensor"].size() >= 4) {
    for (size_t i = 0; i < 4; i++) {
      const char* m = doc["heating_mode_sensor"][i].as<const char*>();
      if (m && (String(m) == "off" || String(m) == "10s_5min" || String(m) == "1min_1hr" || String(m) == "1min_1day")) {
        config.heating_mode_sensor[i] = String(m);
      } else {
        config.heating_mode_sensor[i] = "off";
      }
      // Derive duration/interval from mode
      if (config.heating_mode_sensor[i] == "10s_5min") {
        config.heating_duration_sensor[i] = 10;
        config.heating_interval_sensor[i] = 300;
      } else if (config.heating_mode_sensor[i] == "1min_1hr") {
        config.heating_duration_sensor[i] = 60;
        config.heating_interval_sensor[i] = 3600;
      } else if (config.heating_mode_sensor[i] == "1min_1day") {
        config.heating_duration_sensor[i] = 60;
        config.heating_interval_sensor[i] = 86400;
      } else {
        config.heating_duration_sensor[i] = 0;
        config.heating_interval_sensor[i] = 0;
      }
    }
    config.heating_mode = config.heating_mode_sensor[0];
  } else {
    config.heating_mode_sensor[0] = config.heating_mode;
    for (size_t i = 1; i < 4; i++) config.heating_mode_sensor[i] = "off";
    for (size_t i = 0; i < 4; i++) {
      if (config.heating_mode_sensor[i] == "10s_5min") {
        config.heating_duration_sensor[i] = 10;
        config.heating_interval_sensor[i] = 300;
      } else if (config.heating_mode_sensor[i] == "1min_1hr") {
        config.heating_duration_sensor[i] = 60;
        config.heating_interval_sensor[i] = 3600;
      } else if (config.heating_mode_sensor[i] == "1min_1day") {
        config.heating_duration_sensor[i] = 60;
        config.heating_interval_sensor[i] = 86400;
      } else {
        config.heating_duration_sensor[i] = 0;
        config.heating_interval_sensor[i] = 0;
      }
    }
  }
  config.log_timezone_offset_min = doc["log_timezone_offset_min"] | 0;
  config.time_set = doc["time_set"] | false;
  
  return true;
}

bool saveConfig() {
  DynamicJsonDocument doc(1024);
  doc["config_version"] = config.config_version;
  doc["device_id"] = config.device_id;
  doc["ap_ssid"] = config.ap_ssid;
  doc["ap_password"] = config.ap_password;
  doc["auth_hash"] = config.auth_hash;
  doc["auth_salt"] = config.auth_salt;
  doc["sample_period_s"] = config.sample_period_sensor[0];
  JsonArray sp = doc.createNestedArray("sample_period_sensor");
  for (size_t i = 0; i < 4; i++) sp.add(config.sample_period_sensor[i]);
  doc["log_timezone_offset_min"] = config.log_timezone_offset_min;
  doc["file_rotation"] = config.file_rotation;
  doc["time_set"] = config.time_set;
  // Derive legacy mode from duration/interval for compatibility
  for (size_t i = 0; i < 4; i++) {
    uint32_t d = config.heating_duration_sensor[i];
    uint32_t inv = config.heating_interval_sensor[i];
    if (inv == 0) config.heating_mode_sensor[i] = "off";
    else if (d == 10 && inv == 300) config.heating_mode_sensor[i] = "10s_5min";
    else if (d == 60 && inv == 3600) config.heating_mode_sensor[i] = "1min_1hr";
    else if (d == 60 && inv == 86400) config.heating_mode_sensor[i] = "1min_1day";
    else config.heating_mode_sensor[i] = "off";
  }
  config.heating_mode = config.heating_mode_sensor[0];
  doc["heating_mode"] = config.heating_mode_sensor[0];
  JsonArray hm = doc.createNestedArray("heating_mode_sensor");
  for (size_t i = 0; i < 4; i++) hm.add(config.heating_mode_sensor[i]);
  JsonArray hd = doc.createNestedArray("heating_duration_sensor");
  JsonArray hi = doc.createNestedArray("heating_interval_sensor");
  for (size_t i = 0; i < 4; i++) {
    hd.add(config.heating_duration_sensor[i]);
    hi.add(config.heating_interval_sensor[i]);
  }
  
  File file = LittleFS.open("/config.json", "w");
  if (!file) {
    return false;
  }
  
  serializeJson(doc, file);
  file.close();
  return true;
}

void getDefaultConfig() {
  String chipId = getChipId();
  config.device_id = "LOGGER_" + chipId;
  config.ap_ssid = "LOGGER_" + chipId;
  config.ap_password = "logger123";
  
  // Set default password hash (password: "admin")
  String defaultPassword = "admin";
  config.auth_salt = generateSalt();
  config.auth_hash = hashPassword(defaultPassword, config.auth_salt);
  
  config.sample_period_s = 3600;
  for (size_t i = 0; i < 4; i++) config.sample_period_sensor[i] = 3600;
  config.log_timezone_offset_min = 0;
  config.file_rotation = "monthly";
  config.time_set = false;
  config.heating_mode = "off";
  for (size_t i = 0; i < 4; i++) {
    config.heating_mode_sensor[i] = "off";
    config.heating_duration_sensor[i] = 0;
    config.heating_interval_sensor[i] = 0;
  }
}

// Time functions
String getISOTimestamp() {
  time_t now = time(nullptr);
  if (now < 946684800) { // Before 2000-01-01, time not set
    return "1970-01-01T00:00:00Z";
  }
  
  struct tm* timeinfo = gmtime(&now);
  int year = timeinfo->tm_year + 1900;
  int month = timeinfo->tm_mon + 1;
  int day = timeinfo->tm_mday;
  int hour = timeinfo->tm_hour;
  int minute = timeinfo->tm_min;
  int second = timeinfo->tm_sec;
  String ts = String(year);
  ts += "-";
  if (month < 10) ts += "0";
  ts += String(month);
  ts += "-";
  if (day < 10) ts += "0";
  ts += String(day);
  ts += "T";
  if (hour < 10) ts += "0";
  ts += String(hour);
  ts += ":";
  if (minute < 10) ts += "0";
  ts += String(minute);
  ts += ":";
  if (second < 10) ts += "0";
  ts += String(second);
  ts += "Z";
  return ts;
}

String getLogFilename() {
  time_t now = time(nullptr);
  struct tm* timeinfo = gmtime(&now);
  char buffer[40];
  snprintf(buffer, sizeof(buffer), "/logs/%04d-%02d.csv",
           timeinfo->tm_year + 1900, timeinfo->tm_mon + 1);
  return String(buffer);
}

String getLogFilenameForMonth(int year, int month) {
  char buffer[40];
  snprintf(buffer, sizeof(buffer), "/logs/%04d-%02d.csv", year, month);
  return String(buffer);
}

// Forward declaration (used by getSparseLogPath before the implementation below)
void lfsRingWriteHeaderSparse(File& file, unsigned long offset, unsigned long count);

// Path for sparse (RB2) writes.
// NOTE: On ESP32 VFS, checking existence/opening missing files can spam logs.
// We therefore cache the chosen path and only probe/create once per month.
String getSparseLogPath() {
  static int cachedYear = -1;
  static int cachedMonth = -1;
  static String cachedPath = "";

  time_t now = time(nullptr);
  struct tm* t = gmtime(&now);
  int year = t ? (t->tm_year + 1900) : 1970;
  int month = t ? (t->tm_mon + 1) : 1;

  if (cachedYear == year && cachedMonth == month && cachedPath.length() > 0) {
    return cachedPath;
  }

  String path = getLogFilename();

  // Prefer writing RB2 into the main monthly file if it already exists and is RB2.
  // If the file exists but is RB1/legacy, write to a separate "_s.csv" file.
  File f = LittleFS.open(path, "r");
  if (f) {
    String magic = f.readStringUntil('\n');
    f.close();
    if (magic == "RB2") {
      cachedPath = path;
    } else {
      int dot = path.lastIndexOf('.');
      cachedPath = (dot > 0) ? (path.substring(0, dot) + "_s.csv") : (path + "_s");
    }
  } else {
    // File missing: we'll create an RB2 file at the main path.
    cachedPath = path;
  }

  // Ensure the chosen sparse file exists (create header once). Use "r" probe only once/month.
  File r = LittleFS.open(cachedPath, "r");
  if (r) {
    r.close();
  } else {
    File w = LittleFS.open(cachedPath, "w");
    if (w) {
      lfsRingWriteHeaderSparse(w, 0, 0);
      w.close();
    }
  }

  cachedYear = year;
  cachedMonth = month;
  return cachedPath;
}

void normalizeMonthStart(time_t epoch, int& year, int& month) {
  struct tm* t = gmtime(&epoch);
  year = t->tm_year + 1900;
  month = t->tm_mon + 1;
}

static const char LFS_RING_HEADER_1SENSOR[] = "H:timestamp,device_id,temperature_c,humidity_rh\n";
static const char LFS_RING_HEADER_4SENSOR[] = "H:timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh\n";
static const char LFS_RING_HEADER_SPARSE[] = "H:timestamp,sensor_id,t_c,h_rh\n";

size_t lfsRingHeaderSize() {
  String header = "RB1\n";
  header += "W:00000000\n";
  header += "C:00000000\n";
  header += LFS_RING_HEADER_1SENSOR;
  return header.length();
}

size_t lfsRingHeaderSizeExt() {
  String header = "RB1\n";
  header += "W:00000000\n";
  header += "C:00000000\n";
  header += LFS_RING_HEADER_4SENSOR;
  return header.length();
}

size_t lfsRingHeaderSizeSparse() {
  String header = "RB2\n";
  header += "W:00000000\n";
  header += "C:00000000\n";
  header += LFS_RING_HEADER_SPARSE;
  return header.length();
}

void lfsRingWriteHeaderSparse(File& file, unsigned long offset, unsigned long count) {
  file.seek(0);
  file.print("RB2\n");
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "W:%08lu\n", offset);
  file.print(buffer);
  snprintf(buffer, sizeof(buffer), "C:%08lu\n", count);
  file.print(buffer);
  file.print(LFS_RING_HEADER_SPARSE);
}

size_t lfsRingSlotCount(size_t dataStart, size_t recordLen) {
  if (LFS_RING_MAX_BYTES <= dataStart + recordLen) {
    return 0;
  }
  return (LFS_RING_MAX_BYTES - dataStart) / recordLen;
}

void lfsRingWriteHeader(File& file, unsigned long offset, unsigned long count) {
  file.seek(0);
  file.print("RB1\n");
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "W:%08lu\n", offset);
  file.print(buffer);
  snprintf(buffer, sizeof(buffer), "C:%08lu\n", count);
  file.print(buffer);
  file.print(LFS_RING_HEADER_1SENSOR);
}

void lfsRingWriteHeaderExt(File& file, unsigned long offset, unsigned long count) {
  file.seek(0);
  file.print("RB1\n");
  char buffer[32];
  snprintf(buffer, sizeof(buffer), "W:%08lu\n", offset);
  file.print(buffer);
  snprintf(buffer, sizeof(buffer), "C:%08lu\n", count);
  file.print(buffer);
  file.print(LFS_RING_HEADER_4SENSOR);
}

bool lfsRingReadHeader(File& file, size_t& dataStart, unsigned long& offset, unsigned long& count, size_t& slots, size_t& recordLen) {
  file.seek(0);
  String magic = file.readStringUntil('\n');
  if (magic != "RB1") {
    return false;
  }

  String offsetLine = file.readStringUntil('\n');
  String countLine = file.readStringUntil('\n');
  String headerLine = file.readStringUntil('\n');

  if (!offsetLine.startsWith("W:") || !countLine.startsWith("C:") || !headerLine.startsWith("H:")) {
    return false;
  }

  offset = offsetLine.substring(2).toInt();
  count = countLine.substring(2).toInt();
  dataStart = magic.length() + 1 + offsetLine.length() + 1 + countLine.length() + 1 + headerLine.length() + 1;
  recordLen = headerLine.indexOf("t1_c") >= 0 ? LFS_RING_RECORD_LEN_EXT : LFS_RING_RECORD_LEN;
  slots = lfsRingSlotCount(dataStart, recordLen);
  if (slots == 0 || offset >= slots || count > slots) {
    return false;
  }

  return true;
}

bool lfsRingReadHeaderSparse(File& file, size_t& dataStart, unsigned long& offset, unsigned long& count, size_t& slots, size_t& recordLen) {
  file.seek(0);
  String magic = file.readStringUntil('\n');
  if (magic != "RB2") {
    return false;
  }
  String offsetLine = file.readStringUntil('\n');
  String countLine = file.readStringUntil('\n');
  String headerLine = file.readStringUntil('\n');
  if (!offsetLine.startsWith("W:") || !countLine.startsWith("C:") || !headerLine.startsWith("H:")) {
    return false;
  }
  offset = offsetLine.substring(2).toInt();
  count = countLine.substring(2).toInt();
  dataStart = magic.length() + 1 + offsetLine.length() + 1 + countLine.length() + 1 + headerLine.length() + 1;
  recordLen = LFS_RING_RECORD_LEN_SPARSE;
  slots = lfsRingSlotCount(dataStart, recordLen);
  if (slots == 0 || offset >= slots || count > slots) {
    return false;
  }
  return true;
}

String lfsRingTrimRecord(const char* buf, size_t len) {
  if (len == 0) return "";
  int end = (int)len;
  while (end > 0 && (buf[end - 1] == ' ' || buf[end - 1] == '\n' || buf[end - 1] == '\r' || buf[end - 1] == '\0')) {
    end--;
  }
  if (end <= 0) return "";
  String out = "";
  for (int i = 0; i < end; i++) {
    if (buf[i] == '\0') break;
    out += buf[i];
  }
  return out;
}

// Create empty ring-buffer file with extended header so LittleFS allows creation
// (open "w+" on missing file can fail with "no permits for creation" on ESP32 VFS)
static bool lfsEnsureLogFileExists(const String& filename) {
  File r = LittleFS.open(filename, "r");
  if (r) {
    r.close();
    return true;
  }
  File w = LittleFS.open(filename, "w");
  if (!w) {
    return false;
  }
  lfsRingWriteHeaderExt(w, 0, 0);
  w.close();
  return true;
}

static bool lfsEnsureSparseLogFileExists(const String& path) {
  // Keep this very lightweight: getSparseLogPath() already creates the file once/month.
  File r = LittleFS.open(path, "r");
  if (r) {
    r.close();
    return true;
  }
  File w = LittleFS.open(path, "w");
  if (!w) {
    return false;
  }
  lfsRingWriteHeaderSparse(w, 0, 0);
  w.close();
  return true;
}

// Write one sparse record (RB2): timestamp,sensor_id,t,h. Returns true on success.
static bool writeLfsRingRecordSparse(const String& path, const String& timestamp, int sensorId, float t, float h) {
  if (!lfsEnsureSparseLogFileExists(path)) {
    return false;
  }
  File file = LittleFS.open(path, "r+");
  if (!file) {
    return false;
  }
  size_t dataStart = 0;
  unsigned long offset = 0;
  unsigned long count = 0;
  size_t slots = 0;
  size_t recordLen = LFS_RING_RECORD_LEN_SPARSE;
  if (!lfsRingReadHeaderSparse(file, dataStart, offset, count, slots, recordLen)) {
    file.close();
    return false;
  }
  char buf[LFS_RING_RECORD_LEN_SPARSE + 1];
  int n = snprintf(buf, sizeof(buf), "%-24s,%d,%6.2f,%6.2f\n",
                   timestamp.c_str(), sensorId, (double)t, (double)h);
  if (n < 0 || (size_t)n >= recordLen) {
    file.close();
    return false;
  }
  while ((size_t)n < recordLen - 1) {
    buf[n++] = ' ';
  }
  buf[recordLen - 1] = '\n';
  buf[recordLen] = '\0';

  size_t pos = dataStart + (offset * recordLen);
  file.seek(pos);
  size_t written = file.print(buf);
  file.close();
  if (written != recordLen) {
    return false;
  }
  offset = (offset + 1) % slots;
  if (count < slots) {
    count++;
  }
  file = LittleFS.open(path, "r+");
  if (!file) {
    return true;  // wrote data, only header update failed
  }
  lfsRingWriteHeaderSparse(file, offset, count);
  file.flush();
  file.close();
  return true;
}

bool writeLfsRingRecord(const String& filename, const String& lineShort, const String& lineLong) {
  if (!lfsEnsureLogFileExists(filename)) {
    Serial.println("LittleFS could not create log file: " + filename);
    return false;
  }
  File file = LittleFS.open(filename, "r+");
  if (!file) {
    Serial.println("LittleFS file open failed: " + filename);
    return false;
  }

  size_t dataStart = 0;
  unsigned long offset = 0;
  unsigned long count = 0;
  size_t slots = 0;
  size_t recordLen = LFS_RING_RECORD_LEN;

  if (!lfsRingReadHeader(file, dataStart, offset, count, slots, recordLen)) {
    file.close();
    file = LittleFS.open(filename, "w");
    if (!file) {
      Serial.println("LittleFS file reinit failed: " + filename);
      return false;
    }
    lfsRingWriteHeaderExt(file, 0, 0);
    file.close();
    file = LittleFS.open(filename, "r+");
    if (!file) {
      Serial.println("LittleFS file reopen failed: " + filename);
      return false;
    }
    dataStart = lfsRingHeaderSizeExt();
    recordLen = LFS_RING_RECORD_LEN_EXT;
    slots = lfsRingSlotCount(dataStart, recordLen);
    if (slots == 0) {
      file.close();
      Serial.println("LittleFS ring buffer size too small");
      return false;
    }
    offset = 0;
    count = 0;
  }

  String line = (recordLen >= LFS_RING_RECORD_LEN_EXT) ? lineLong : lineShort;
  String record = line;
  if (record.endsWith("\n")) {
    record.remove(record.length() - 1);
  }
  size_t maxRec = recordLen - 1;
  if (record.length() > maxRec) {
    record = record.substring(0, maxRec);
  }
  while (record.length() < maxRec) {
    record += " ";
  }
  record += "\n";

  size_t pos = dataStart + (offset * recordLen);
  file.seek(pos);
  size_t written = file.print(record);

  if (written != recordLen) {
    file.close();
    Serial.println("LittleFS ring write failed");
    return false;
  }

  offset = (offset + 1) % slots;
  if (count < slots) {
    count++;
  }

  if (recordLen >= LFS_RING_RECORD_LEN_EXT) {
    lfsRingWriteHeaderExt(file, offset, count);
  } else {
    lfsRingWriteHeader(file, offset, count);
  }
  file.flush();
  file.close();
  return true;
}

// Helper: format one value for CSV (NaN if not finite)
static String fmtVal(float v) {
  if (!isfinite(v)) return "NaN";
  return String(v, 2);
}

// Write one full row to SD (optional): timestamp,device_id,t0,h0,t1,h1,t2,h2,t3,h3. Returns true if written or SD not present.
static bool writeSDDataPoint(const String& timestamp, float t0, float h0, float t1, float h1, float t2, float h2, float t3, float h3) {
  if (!sdPresent) return true;
  String filename = getLogFilename();
  if (!SD.exists("/logs")) {
    SD.mkdir("/logs");
  }
  bool sdExists = SD.exists(filename);
  File sdFile = SD.open(filename, FILE_WRITE);
  if (!sdFile) return false;
  sdFile.seek(sdFile.size());
  if (!sdExists) {
    sdFile.print("timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh\n");
  }
  String line = timestamp + "," + config.device_id + "," +
      fmtVal(t0) + "," + fmtVal(h0) + "," + fmtVal(t1) + "," + fmtVal(h1) + "," +
      fmtVal(t2) + "," + fmtVal(h2) + "," + fmtVal(t3) + "," + fmtVal(h3) + "\n";
  sdFile.print(line);
  sdFile.flush();
  sdFile.close();
  return true;
}

size_t estimateSampleBytes() {
  return LFS_RING_RECORD_LEN_SPARSE;  // sparse: one record per sensor read
}

// API Handlers
void handleRoot() {
  // Serve index.html without auth - login modal handles authentication
  File file = LittleFS.open("/index.html", "r");
  if (!file) {
    server.send(404, "text/plain", "File not found");
    return;
  }
  server.streamFile(file, "text/html");
  file.close();
}

void handleLogin() {
  if (server.method() == HTTP_GET) {
    // Serve index.html (login modal will show)
    File file = LittleFS.open("/index.html", "r");
    if (!file) {
      server.send(404, "text/plain", "File not found");
      return;
    }
    server.streamFile(file, "text/html");
    file.close();
  } else if (server.method() == HTTP_POST) {
    if (!server.hasArg("password")) {
      server.send(400, "application/json", "{\"error\":\"Missing password\"}");
      return;
    }
    
    String password = server.arg("password");
    if (verifyPassword(password, config.auth_hash, config.auth_salt)) {
      // If the stored hash is legacy (32-bit), transparently upgrade it on successful login.
      if (config.auth_hash.length() < 64) {
        config.auth_salt = generateSalt();
        config.auth_hash = hashPasswordSHA256(password, config.auth_salt);
        saveConfig();
      }

      String token = generateSessionToken();
      addSession(token);
      // Cookie hardening: HttpOnly + SameSite. (No Secure flag because this is usually served over plain HTTP on a local AP.)
      server.sendHeader("Set-Cookie", "SESSION=" + token + "; Path=/; Max-Age=3600; HttpOnly; SameSite=Strict");
      server.send(200, "application/json", "{\"success\":true}");
    } else {
      server.send(401, "application/json", "{\"error\":\"Invalid password\"}");
    }
  }
}

void handleLogout() {
  String token = getSessionToken();
  if (token.length() > 0) {
    removeSession(token);
  }
  server.sendHeader("Set-Cookie", "SESSION=; Path=/; Max-Age=0; HttpOnly; SameSite=Strict");
  server.sendHeader("Location", "/login");
  server.send(302, "text/plain", "");
}

void handleApiConfig() {
  // No auth required - WiFi password is sufficient
  
  if (server.method() == HTTP_GET) {
    DynamicJsonDocument doc(768);
    doc["device_id"] = config.device_id;
    doc["sample_period_s"] = config.sample_period_sensor[0];
    JsonArray sp = doc.createNestedArray("sample_period_sensor");
    for (size_t i = 0; i < 4; i++) sp.add(config.sample_period_sensor[i]);
    doc["log_timezone_offset_min"] = config.log_timezone_offset_min;
    doc["file_rotation"] = config.file_rotation;
    doc["time_set"] = config.time_set;
    doc["heating_mode"] = config.heating_mode_sensor[0];
    JsonArray hm = doc.createNestedArray("heating_mode_sensor");
    for (size_t i = 0; i < 4; i++) hm.add(config.heating_mode_sensor[i]);
    JsonArray hd = doc.createNestedArray("heating_duration_sensor");
    JsonArray hi = doc.createNestedArray("heating_interval_sensor");
    for (size_t i = 0; i < 4; i++) {
      hd.add(config.heating_duration_sensor[i]);
      hi.add(config.heating_interval_sensor[i]);
    }
    
    String response;
    serializeJson(doc, response);
    server.send(200, "application/json", response);
  } else if (server.method() == HTTP_POST) {
    if (!server.hasArg("plain")) {
      server.send(400, "application/json", "{\"error\":\"Invalid request\"}");
      return;
    }
    
    DynamicJsonDocument doc(512);
    DeserializationError error = deserializeJson(doc, server.arg("plain"));
    if (error) {
      server.send(400, "application/json", "{\"error\":\"Invalid JSON\"}");
      return;
    }
    
    if (doc.containsKey("sample_period_sensor") && doc["sample_period_sensor"].is<JsonArray>() && doc["sample_period_sensor"].size() >= 4) {
      for (size_t i = 0; i < 4; i++) {
        uint32_t period = doc["sample_period_sensor"][i] | 3600;
        if (period < 10) period = 10;
        if (period > 604800) period = 604800;
        config.sample_period_sensor[i] = period;
      }
      config.sample_period_s = config.sample_period_sensor[0];
    } else if (doc.containsKey("sample_period_s")) {
      uint32_t period = doc["sample_period_s"];
      if (period >= 10 && period <= 604800) {
        for (size_t i = 0; i < 4; i++) config.sample_period_sensor[i] = period;
        config.sample_period_s = period;
      } else {
        server.send(400, "application/json", "{\"error\":\"Invalid sample period\"}");
        return;
      }
    }
    
    if (doc.containsKey("log_timezone_offset_min")) {
      config.log_timezone_offset_min = doc["log_timezone_offset_min"];
    }
    
    if (doc.containsKey("heating_duration_sensor") && doc["heating_duration_sensor"].is<JsonArray>() && doc["heating_duration_sensor"].size() >= 4 &&
        doc.containsKey("heating_interval_sensor") && doc["heating_interval_sensor"].is<JsonArray>() && doc["heating_interval_sensor"].size() >= 4) {
      for (size_t i = 0; i < 4; i++) {
        uint32_t interval = doc["heating_interval_sensor"][i] | 0;
        uint32_t duration = doc["heating_duration_sensor"][i] | 0;
        if (interval == 0) duration = 0;
        else if (duration > interval) duration = interval;
        config.heating_interval_sensor[i] = interval;
        config.heating_duration_sensor[i] = duration;
      }
      for (size_t i = 0; i < 4; i++) {
        uint32_t d = config.heating_duration_sensor[i];
        uint32_t inv = config.heating_interval_sensor[i];
        if (inv == 0) config.heating_mode_sensor[i] = "off";
        else if (d == 10 && inv == 300) config.heating_mode_sensor[i] = "10s_5min";
        else if (d == 60 && inv == 3600) config.heating_mode_sensor[i] = "1min_1hr";
        else if (d == 60 && inv == 86400) config.heating_mode_sensor[i] = "1min_1day";
        else config.heating_mode_sensor[i] = "off";
      }
      config.heating_mode = config.heating_mode_sensor[0];
    } else if (doc.containsKey("heating_mode_sensor") && doc["heating_mode_sensor"].is<JsonArray>() && doc["heating_mode_sensor"].size() >= 4) {
      for (size_t i = 0; i < 4; i++) {
        const char* mode = doc["heating_mode_sensor"][i].as<const char*>();
        if (mode) {
          String m = String(mode);
          if (m == "off" || m == "10s_5min" || m == "1min_1hr" || m == "1min_1day") {
            config.heating_mode_sensor[i] = m;
          }
        }
        if (config.heating_mode_sensor[i] == "10s_5min") {
          config.heating_duration_sensor[i] = 10;
          config.heating_interval_sensor[i] = 300;
        } else if (config.heating_mode_sensor[i] == "1min_1hr") {
          config.heating_duration_sensor[i] = 60;
          config.heating_interval_sensor[i] = 3600;
        } else if (config.heating_mode_sensor[i] == "1min_1day") {
          config.heating_duration_sensor[i] = 60;
          config.heating_interval_sensor[i] = 86400;
        } else {
          config.heating_duration_sensor[i] = 0;
          config.heating_interval_sensor[i] = 0;
        }
      }
      config.heating_mode = config.heating_mode_sensor[0];
    } else if (doc.containsKey("heating_mode")) {
      const char* mode = doc["heating_mode"].as<const char*>();
      if (mode) {
        String m = String(mode);
        if (m == "off" || m == "10s_5min" || m == "1min_1hr" || m == "1min_1day") {
          config.heating_mode_sensor[0] = m;
          config.heating_mode = m;
        }
      }
      if (config.heating_mode_sensor[0] == "10s_5min") {
        config.heating_duration_sensor[0] = 10;
        config.heating_interval_sensor[0] = 300;
      } else if (config.heating_mode_sensor[0] == "1min_1hr") {
        config.heating_duration_sensor[0] = 60;
        config.heating_interval_sensor[0] = 3600;
      } else if (config.heating_mode_sensor[0] == "1min_1day") {
        config.heating_duration_sensor[0] = 60;
        config.heating_interval_sensor[0] = 86400;
      } else {
        config.heating_duration_sensor[0] = 0;
        config.heating_interval_sensor[0] = 0;
      }
    }
    
    if (saveConfig()) {
      // Reset per-sensor scheduler so new sampling periods take effect immediately
      for (int i = 0; i < 4; i++) nextReadAtMs[i] = 0;
      Serial.printf("[HTTP] Config saved. Sampling: S0=%lus S1=%lus S2=%lus S3=%lus\n",
                    (unsigned long)config.sample_period_sensor[0], (unsigned long)config.sample_period_sensor[1],
                    (unsigned long)config.sample_period_sensor[2], (unsigned long)config.sample_period_sensor[3]);
      Serial.printf("[HTTP] Heating (duration/interval): S0=%lu/%lu S1=%lu/%lu S2=%lu/%lu S3=%lu/%lu\n",
                    (unsigned long)config.heating_duration_sensor[0], (unsigned long)config.heating_interval_sensor[0],
                    (unsigned long)config.heating_duration_sensor[1], (unsigned long)config.heating_interval_sensor[1],
                    (unsigned long)config.heating_duration_sensor[2], (unsigned long)config.heating_interval_sensor[2],
                    (unsigned long)config.heating_duration_sensor[3], (unsigned long)config.heating_interval_sensor[3]);
      server.send(200, "application/json", "{\"success\":true}");
    } else {
      server.send(500, "application/json", "{\"error\":\"Failed to save config\"}");
    }
  }
}

void handleApiTime() {
  // No auth required - WiFi password is sufficient
  
  if (server.method() == HTTP_GET) {
    time_t now = time(nullptr);
    DynamicJsonDocument doc(256);
    doc["epoch"] = now;
    doc["time_set"] = config.time_set && (now >= 946684800);
    doc["iso"] = getISOTimestamp();
    
    String response;
    serializeJson(doc, response);
    server.send(200, "application/json", response);
  } else if (server.method() == HTTP_POST) {
    if (!server.hasArg("plain")) {
      server.send(400, "application/json", "{\"error\":\"Invalid request\"}");
      return;
    }
    
    DynamicJsonDocument doc(256);
    DeserializationError error = deserializeJson(doc, server.arg("plain"));
    if (error) {
      server.send(400, "application/json", "{\"error\":\"Invalid JSON\"}");
      return;
    }
    
    if (doc.containsKey("epoch")) {
      time_t epoch = doc["epoch"];
      timeval tv = {epoch, 0};
      settimeofday(&tv, nullptr);
      config.time_set = true;
      saveConfig();
      
      // Also set RTC if present
      if (rtcPresent) {
        struct tm* timeinfo = localtime(&epoch);
        rtc.adjust(DateTime(timeinfo->tm_year + 1900, timeinfo->tm_mon + 1, 
                           timeinfo->tm_mday, timeinfo->tm_hour, 
                           timeinfo->tm_min, timeinfo->tm_sec));
      }
      
      server.send(200, "application/json", "{\"success\":true}");
    } else {
      server.send(400, "application/json", "{\"error\":\"Missing epoch\"}");
    }
  }
}

void handleApiStorage() {
  // No auth required - WiFi password is sufficient
  
  size_t lfsTotal = LittleFS.totalBytes();
  size_t lfsUsed = LittleFS.usedBytes();
  size_t bytesPerSample = estimateSampleBytes();
  size_t freeBytes = lfsTotal - lfsUsed;
  size_t capacityBytes = LFS_RING_MAX_BYTES;
  if (capacityBytes > lfsTotal) {
    capacityBytes = lfsTotal;
  }
  if (capacityBytes > freeBytes) {
    capacityBytes = freeBytes;
  }
  unsigned long samplePeriod = config.sample_period_s;
  unsigned long estSamples = 0;
  unsigned long estDuration = 0;
  if (bytesPerSample > 0 && samplePeriod > 0) {
    estSamples = capacityBytes / bytesPerSample;
    estDuration = estSamples * samplePeriod;
  }
  
  DynamicJsonDocument doc(768);
  doc["lfs"]["total"] = lfsTotal;
  doc["lfs"]["used"] = lfsUsed;
  doc["lfs"]["free"] = freeBytes;
  doc["sd"]["present"] = sdPresent;
  doc["write_errors"] = writeErrors;
  doc["sample_period_s"] = samplePeriod;
  doc["retention"]["bytes_per_sample"] = bytesPerSample;
  doc["retention"]["est_samples"] = estSamples;
  doc["retention"]["est_duration_s"] = estDuration;
  
  if (sdPresent) {
    // SD card size info not always available
    doc["sd"]["total"] = 0;
    doc["sd"]["used"] = 0;
    doc["sd"]["free"] = 0;
  } else {
    doc["sd"]["total"] = 0;
    doc["sd"]["used"] = 0;
    doc["sd"]["free"] = 0;
  }
  
  String response;
  serializeJson(doc, response);
  server.send(200, "application/json", response);
}

void handleApiPrune() {
  // No auth required - WiFi password is sufficient
  if (!server.hasArg("days")) {
    server.send(400, "application/json", "{\"error\":\"Missing days parameter\"}");
    return;
  }

  int days = server.arg("days").toInt();
  if (days <= 0) {
    server.send(400, "application/json", "{\"error\":\"Invalid days value\"}");
    return;
  }

  time_t now = time(nullptr);
  if (now < 946684800) { // Before 2000-01-01, time not set
    server.send(400, "application/json", "{\"error\":\"Time not set\"}");
    return;
  }

  time_t cutoff = now - (time_t)days * 86400;

  int deletedSamples = 0;
  int keptSamples = 0;
  int deletedFiles = 0;
  size_t freedBytes = 0;

  auto sendEmpty = [&]() {
    DynamicJsonDocument docEmpty(256);
    docEmpty["deleted_samples"] = 0;
    docEmpty["kept_samples"] = 0;
    docEmpty["deleted_files"] = 0;
    docEmpty["freed_bytes"] = 0;
    String resp;
    serializeJson(docEmpty, resp);
    server.send(200, "application/json", resp);
  };

  File root = LittleFS.open("/logs");
  if (!root || !root.isDirectory()) {
    root.close();
    sendEmpty();
    return;
  }

  File file = root.openNextFile();
  while (file) {
    String fileName = file.name();
    if (!fileName.endsWith(".csv")) {
      file = root.openNextFile();
      continue;
    }
    if (!fileName.startsWith("/")) {
      fileName = "/logs/" + fileName;
    }

    size_t originalSize = file.size();

    file.seek(0);
    String magic = file.readStringUntil('\n');
    file.seek(0);
    size_t dataStart = 0;
    unsigned long offset = 0;
    unsigned long count = 0;
    size_t slots = 0;
    size_t recordLen = LFS_RING_RECORD_LEN;
    bool isSparse = (magic == "RB2");
    bool isRing = isSparse ? lfsRingReadHeaderSparse(file, dataStart, offset, count, slots, recordLen)
                           : lfsRingReadHeader(file, dataStart, offset, count, slots, recordLen);

    String tmpName = fileName + ".tmp";

    if (isRing) {
      File tmp = LittleFS.open(tmpName, "w+");
      if (!tmp) {
        file.close();
        file = root.openNextFile();
        continue;
      }

      if (isSparse) {
        lfsRingWriteHeaderSparse(tmp, 0, 0);
      } else if (recordLen >= LFS_RING_RECORD_LEN_EXT) {
        lfsRingWriteHeaderExt(tmp, 0, 0);
      } else {
        lfsRingWriteHeader(tmp, 0, 0);
      }
      size_t tmpDataStart = isSparse ? lfsRingHeaderSizeSparse() : ((recordLen >= LFS_RING_RECORD_LEN_EXT) ? lfsRingHeaderSizeExt() : lfsRingHeaderSize());
      size_t tmpSlots = lfsRingSlotCount(tmpDataStart, recordLen);
      if (tmpSlots == 0) {
        tmp.close();
        file.close();
        LittleFS.remove(tmpName);
        file = root.openNextFile();
        continue;
      }

      unsigned long keptInFile = 0;
      unsigned long startIndex = (offset + slots - count) % slots;
      for (unsigned long i = 0; i < count; i++) {
        unsigned long idx = (startIndex + i) % slots;
        size_t pos = dataStart + (idx * recordLen);
        file.seek(pos);

        char buf[LFS_RING_RECORD_LEN_EXT + 1];
        size_t nread = file.readBytes(buf, recordLen);
        buf[nread] = '\0';

        String line = lfsRingTrimRecord(buf, nread);
        if (line.length() == 0) {
          continue;
        }

        int comma1 = line.indexOf(',');
        if (comma1 <= 0) {
          deletedSamples++;
          continue;
        }

        String tsStr = line.substring(0, comma1);
        time_t tsTime = parseISO8601(tsStr);
        if (tsTime != 0 && tsTime >= cutoff) {
          String record = line;
          if (record.endsWith("\n")) {
            record.remove(record.length() - 1);
          }
          size_t maxRec = recordLen - 1;
          if (record.length() > maxRec) {
            record = record.substring(0, maxRec);
          }
          while (record.length() < maxRec) {
            record += " ";
          }
          record += "\n";

          size_t wpos = tmpDataStart + (keptInFile * recordLen);
          tmp.seek(wpos);
          size_t written = tmp.print(record);
          if (written != recordLen) {
            break;
          }

          keptSamples++;
          keptInFile++;
        } else {
          deletedSamples++;
        }

        yield();
      }

      unsigned long newCount = keptInFile;
      unsigned long newOffset = (tmpSlots > 0) ? (newCount % tmpSlots) : 0;
      if (isSparse) {
        lfsRingWriteHeaderSparse(tmp, newOffset, newCount);
      } else if (recordLen >= LFS_RING_RECORD_LEN_EXT) {
        lfsRingWriteHeaderExt(tmp, newOffset, newCount);
      } else {
        lfsRingWriteHeader(tmp, newOffset, newCount);
      }
      tmp.flush();
      tmp.close();
      file.close();

      if (newCount == 0) {
        LittleFS.remove(fileName);
        LittleFS.remove(tmpName);
        deletedFiles++;
        freedBytes += originalSize;
      } else {
        size_t newSize = 0;
        File tmpRead = LittleFS.open(tmpName, "r");
        if (tmpRead) {
          newSize = tmpRead.size();
          tmpRead.close();
        }
        LittleFS.remove(fileName);
        LittleFS.rename(tmpName, fileName);
        if (originalSize > newSize) {
          freedBytes += (originalSize - newSize);
        }
      }

      yield();
      file = root.openNextFile();
      continue;
    }

    // Plain CSV pruning (legacy files)
    File tmp = LittleFS.open(tmpName, "w");
    if (!tmp) {
      file.close();
      file = root.openNextFile();
      continue;
    }

    String line = file.readStringUntil('\n');
    if (line.startsWith("timestamp")) {
      tmp.print(line + "\n");
    } else {
      file.seek(0);
    }

    int fileKept = 0;
    while (file.available()) {
      line = file.readStringUntil('\n');
      if (line.length() == 0) break;

      int comma1 = line.indexOf(',');
      if (comma1 <= 0) {
        deletedSamples++;
        continue;
      }

      String tsStr = line.substring(0, comma1);
      time_t tsTime = parseISO8601(tsStr);
      if (tsTime != 0 && tsTime >= cutoff) {
        tmp.print(line + "\n");
        keptSamples++;
        fileKept++;
      } else {
        deletedSamples++;
      }

      yield();
    }

    file.close();
    tmp.flush();
    tmp.close();

    if (fileKept == 0) {
      LittleFS.remove(fileName);
      LittleFS.remove(tmpName);
      deletedFiles++;
      freedBytes += originalSize;
    } else {
      size_t newSize = 0;
      File tmpRead = LittleFS.open(tmpName, "r");
      if (tmpRead) {
        newSize = tmpRead.size();
        tmpRead.close();
      }
      LittleFS.remove(fileName);
      LittleFS.rename(tmpName, fileName);
      if (originalSize > newSize) {
        freedBytes += (originalSize - newSize);
      }
    }

    yield();
    file = root.openNextFile();
  }
  root.close();

  DynamicJsonDocument doc(512);
  doc["deleted_samples"] = deletedSamples;
  doc["kept_samples"] = keptSamples;
  doc["deleted_files"] = deletedFiles;
  doc["freed_bytes"] = freedBytes;

  String response;
  serializeJson(doc, response);
  server.send(200, "application/json", response);
}


static bool isLeapYear(int year) {
  return ((year % 4) == 0 && (year % 100) != 0) || ((year % 400) == 0);
}

static int daysInMonth(int year, int month) {
  static const int mdays[] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  if (month < 1 || month > 12) return 0;
  if (month == 2) return mdays[1] + (isLeapYear(year) ? 1 : 0);
  return mdays[month - 1];
}

static time_t timegmCompat(const struct tm& t) {
  // Convert a UTC tm into Unix epoch seconds (no timezone/DST effects).
  int year = t.tm_year + 1900;
  int month = t.tm_mon + 1;
  int day = t.tm_mday;

  if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31) return 0;

  int64_t days = 0;
  for (int y = 1970; y < year; y++) {
    days += isLeapYear(y) ? 366 : 365;
  }
  for (int m = 1; m < month; m++) {
    days += daysInMonth(year, m);
  }
  days += (day - 1);

  int64_t seconds = days * 86400LL + (int64_t)t.tm_hour * 3600LL + (int64_t)t.tm_min * 60LL + (int64_t)t.tm_sec;
  if (seconds < 0) return 0;
  return (time_t)seconds;
}

// Parse ISO8601 timestamp to epoch (UTC). Accepts: YYYY-MM-DDTHH:MM:SSZ
time_t parseISO8601(const String& iso) {
  if (iso.length() < 19) return 0;
  if (iso.charAt(4) != '-' || iso.charAt(7) != '-' || iso.charAt(10) != 'T' || iso.charAt(13) != ':' || iso.charAt(16) != ':') {
    return 0;
  }

  int year = iso.substring(0, 4).toInt();
  int month = iso.substring(5, 7).toInt();
  int day = iso.substring(8, 10).toInt();
  int hour = iso.substring(11, 13).toInt();
  int minute = iso.substring(14, 16).toInt();
  int second = iso.substring(17, 19).toInt();

  if (month < 1 || month > 12) return 0;
  if (day < 1 || day > daysInMonth(year, month)) return 0;
  if (hour < 0 || hour > 23) return 0;
  if (minute < 0 || minute > 59) return 0;
  if (second < 0 || second > 59) return 0;

  struct tm timeinfo = {0};
  timeinfo.tm_year = year - 1900;
  timeinfo.tm_mon = month - 1;
  timeinfo.tm_mday = day;
  timeinfo.tm_hour = hour;
  timeinfo.tm_min = minute;
  timeinfo.tm_sec = second;

  return timegmCompat(timeinfo);
}

void handleApiData() {
  // No auth required - WiFi password is sufficient
  
  if (!server.hasArg("from") || !server.hasArg("to")) {
    server.send(400, "application/json", "{\"error\":\"Missing from/to parameters\"}");
    return;
  }
  
  String fromStr = server.arg("from");
  String toStr = server.arg("to");
  
  // Parse ISO8601 timestamps
  time_t fromTime = parseISO8601(fromStr);
  time_t toTime = parseISO8601(toStr);
  
  if (fromTime == 0 || toTime == 0) {
    server.send(400, "application/json", "{\"error\":\"Invalid timestamp format\"}");
    return;
  }
  
  int pointCount = 0;
  const int MAX_POINTS = 20000;

  // Stream JSON to avoid large heap usage
  server.setContentLength(CONTENT_LENGTH_UNKNOWN);
  server.send(200, "application/json", "");
  server.sendContent("{\"points\":[");

  bool firstPoint = true;
  
  // Iterate through log files (ESP32: File openNextFile)
  File dataRoot = LittleFS.open("/logs");
  if (!dataRoot || !dataRoot.isDirectory()) {
    dataRoot.close();
    server.sendContent("]");
    server.sendContent(",\"count\":0}");
    server.sendContent("");
    server.client().stop();
    return;
  }
  File file = dataRoot.openNextFile();
  while (file && pointCount < MAX_POINTS) {
    if (String(file.name()).endsWith(".csv")) {
        file.seek(0);
        String magic = file.readStringUntil('\n');
        file.seek(0);
        size_t dataStart = 0;
        unsigned long offset = 0;
        unsigned long count = 0;
        size_t slots = 0;
        size_t recordLen = LFS_RING_RECORD_LEN;

        if (magic == "RB2" && lfsRingReadHeaderSparse(file, dataStart, offset, count, slots, recordLen)) {
          unsigned long startIndex = (offset + slots - count) % slots;
          for (unsigned long i = 0; i < count && pointCount < MAX_POINTS; i++) {
            unsigned long idx = (startIndex + i) % slots;
            size_t pos = dataStart + (idx * recordLen);
            file.seek(pos);
            char buf[LFS_RING_RECORD_LEN_SPARSE + 1];
            size_t nread = file.readBytes(buf, recordLen);
            buf[nread] = '\0';
            String line = lfsRingTrimRecord(buf, nread);
            if (line.length() < 28) continue;
            int c1 = line.indexOf(',');
            if (c1 <= 0) continue;
            int c2 = line.indexOf(',', c1 + 1);
            if (c2 <= 0) continue;
            int c3 = line.indexOf(',', c2 + 1);
            if (c3 <= 0) continue;
            String tsStr = line.substring(0, c1);
            tsStr.trim();
            time_t tsTime = parseISO8601(tsStr);
            if (tsTime < fromTime || tsTime > toTime) { yield(); continue; }
            int sensorId = line.substring(c1 + 1, c2).toInt();
            if (sensorId < 0 || sensorId > 3) continue;
            String tStr = line.substring(c2 + 1, c3);
            String hStr = line.substring(c3 + 1);
            String payload = "[\"" + tsStr + "\"";
            for (int s = 0; s < 4; s++) {
              if (s == sensorId) {
                payload += "," + tStr + "," + hStr;
              } else {
                payload += ",null,null";
              }
            }
            payload += "]";
            if (!firstPoint) server.sendContent(",");
            firstPoint = false;
            server.sendContent(payload);
            pointCount++;
            yield();
          }
        } else if (magic == "RB1" && lfsRingReadHeader(file, dataStart, offset, count, slots, recordLen)) {
          unsigned long startIndex = (offset + slots - count) % slots;
          for (unsigned long i = 0; i < count && pointCount < MAX_POINTS; i++) {
            unsigned long idx = (startIndex + i) % slots;
            size_t pos = dataStart + (idx * recordLen);
            file.seek(pos);
            char buf[LFS_RING_RECORD_LEN_EXT + 1];
            size_t nread = file.readBytes(buf, recordLen);
            buf[nread] = '\0';
            String line = lfsRingTrimRecord(buf, nread);
            if (line.length() == 0) continue;
            int comma1 = line.indexOf(',');
            if (comma1 <= 0) continue;
            int comma2 = line.indexOf(',', comma1 + 1);
            if (comma2 <= 0) continue;
            String tsStr = line.substring(0, comma1);
            time_t tsTime = parseISO8601(tsStr);
            if (tsTime < fromTime || tsTime > toTime) { yield(); continue; }
            String rest = line.substring(comma2 + 1);
            int nCommas = 0;
            for (size_t c = 0; c < rest.length(); c++) {
              if (rest.charAt(c) == ',') nCommas++;
            }
            String payload;
            if (nCommas >= 7) {
              payload = "[\"" + tsStr + "\"," + rest + "]";
            } else {
              int comma3 = line.indexOf(',', comma2 + 1);
              if (comma3 <= 0) { yield(); continue; }
              String t0 = line.substring(comma2 + 1, comma3);
              String h0 = line.substring(comma3 + 1);
              payload = "[\"" + tsStr + "\"," + t0 + "," + h0 + ",null,null,null,null,null,null]";
            }
            if (!firstPoint) server.sendContent(",");
            firstPoint = false;
            server.sendContent(payload);
            pointCount++;
            yield();
          }
        } else {
          file.seek(0);
          String line = file.readStringUntil('\n');
          if (!line.startsWith("timestamp")) file.seek(0);
          while (file.available() && pointCount < MAX_POINTS) {
            line = file.readStringUntil('\n');
            if (line.length() == 0) break;
            int comma1 = line.indexOf(',');
            if (comma1 <= 0) continue;
            int comma2 = line.indexOf(',', comma1 + 1);
            int comma3 = line.indexOf(',', comma2 + 1);
            if (comma1 > 0 && comma2 > 0 && comma3 > 0) {
              String tsStr = line.substring(0, comma1);
              time_t tsTime = parseISO8601(tsStr);
              if (tsTime >= fromTime && tsTime <= toTime) {
                if (!firstPoint) server.sendContent(",");
                firstPoint = false;
                String tempStr = line.substring(comma2 + 1, comma3);
                String humStr = line.substring(comma3 + 1);
                server.sendContent("[\"" + tsStr + "\"," + tempStr + "," + humStr + "]");
                pointCount++;
              }
            }
            yield();
          }
        }
    }
    yield();
    file = dataRoot.openNextFile();
  }
  dataRoot.close();
  
  server.sendContent("]");
  server.sendContent(",\"count\":" + String(pointCount));
  if (pointCount >= MAX_POINTS) {
    server.sendContent(",\"warning\":\"Result truncated to 20000 points\"");
  }
  server.sendContent("}");
  server.sendContent("");
  server.client().stop();
}

void handleApiDownload() {
  // No auth required - WiFi password is sufficient
  
  if (!server.hasArg("from") || !server.hasArg("to")) {
    server.send(400, "text/plain", "Missing from/to parameters");
    return;
  }
  
  // Generate filename
  String filename = config.device_id + "_" + server.arg("from").substring(0, 10) + 
                    "_" + server.arg("to").substring(0, 10) + ".csv";
  
  // Parse time range
  String fromStr = server.arg("from");
  String toStr = server.arg("to");
  time_t fromTime = parseISO8601(fromStr);
  time_t toTime = parseISO8601(toStr);
  
  if (fromTime == 0 || toTime == 0) {
    server.send(400, "text/plain", "Invalid timestamp format");
    return;
  }
  
  server.sendHeader("Content-Type", "text/csv");
  server.sendHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
  server.setContentLength(CONTENT_LENGTH_UNKNOWN);
  server.send(200, "text/csv", "");
  unsigned long sentLines = 0;

  bool useSd = false;
  useSd = sdPresent;
  if (useSd) {
    server.sendContent("timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh\n");
  }

  if (useSd) {
    int startYear = 0, startMonth = 0;
    int endYear = 0, endMonth = 0;
    normalizeMonthStart(fromTime, startYear, startMonth);
    normalizeMonthStart(toTime, endYear, endMonth);

    int year = startYear;
    int month = startMonth;
    while (year < endYear || (year == endYear && month <= endMonth)) {
      String fileName = getLogFilenameForMonth(year, month);
      File file = SD.open(fileName, FILE_READ);
      if (file) {
        String header = file.readStringUntil('\n');
        if (!header.startsWith("timestamp")) {
          file.seek(0);
        }

        while (file.available()) {
          String line = file.readStringUntil('\n');
          if (line.length() == 0) break;

          int comma1 = line.indexOf(',');
          if (comma1 > 0) {
            String tsStr = line.substring(0, comma1);
            time_t tsTime = parseISO8601(tsStr);
            if (tsTime >= fromTime && tsTime <= toTime) {
              server.sendContent(line + "\n");
              sentLines++;
            }
          }

          yield();
        }
        file.close();
      }

      month++;
      if (month > 12) {
        month = 1;
        year++;
      }
      yield();
    }
  } else {
    bool headerSent = false;
    File dlRoot = LittleFS.open("/logs");
    if (dlRoot && dlRoot.isDirectory()) {
      File file = dlRoot.openNextFile();
      while (file) {
        if (String(file.name()).endsWith(".csv")) {
          file.seek(0);
          String magic = file.readStringUntil('\n');
          file.seek(0);
          size_t dataStart = 0;
          unsigned long offset = 0;
          unsigned long count = 0;
          size_t slots = 0;
          size_t recordLen = LFS_RING_RECORD_LEN;

          if (magic == "RB2" && lfsRingReadHeaderSparse(file, dataStart, offset, count, slots, recordLen)) {
            if (!headerSent) {
              server.sendContent("timestamp,sensor_id,t_c,h_rh\n");
              headerSent = true;
            }
            unsigned long startIndex = (offset + slots - count) % slots;
            for (unsigned long i = 0; i < count; i++) {
              unsigned long idx = (startIndex + i) % slots;
              size_t pos = dataStart + (idx * recordLen);
              file.seek(pos);
              char buf[LFS_RING_RECORD_LEN_SPARSE + 1];
              size_t nread = file.readBytes(buf, recordLen);
              buf[nread] = '\0';
              String line = lfsRingTrimRecord(buf, nread);
              if (line.length() < 28) continue;
              int c1 = line.indexOf(',');
              if (c1 <= 0) continue;
              String tsStr = line.substring(0, c1);
              time_t tsTime = parseISO8601(tsStr);
              if (tsTime >= fromTime && tsTime <= toTime) {
                server.sendContent(line + "\n");
                sentLines++;
              }
              yield();
            }
          } else if (magic == "RB1" && lfsRingReadHeader(file, dataStart, offset, count, slots, recordLen)) {
            if (!headerSent) {
              server.sendContent("timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh\n");
              headerSent = true;
            }
            unsigned long startIndex = (offset + slots - count) % slots;
            for (unsigned long i = 0; i < count; i++) {
              unsigned long idx = (startIndex + i) % slots;
              size_t pos = dataStart + (idx * recordLen);
              file.seek(pos);
              char buf[LFS_RING_RECORD_LEN_EXT + 1];
              size_t nread = file.readBytes(buf, recordLen);
              buf[nread] = '\0';
              String line = lfsRingTrimRecord(buf, nread);
              if (line.length() == 0) continue;
              int comma1 = line.indexOf(',');
              if (comma1 > 0) {
                String tsStr = line.substring(0, comma1);
                time_t tsTime = parseISO8601(tsStr);
                if (tsTime >= fromTime && tsTime <= toTime) {
                  server.sendContent(line + "\n");
                  sentLines++;
                }
              }
              yield();
            }
          } else {
            if (!headerSent) {
              server.sendContent("timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh\n");
              headerSent = true;
            }
            String hdr = file.readStringUntil('\n');
            if (!hdr.startsWith("timestamp")) file.seek(0);
            while (file.available()) {
              String line = file.readStringUntil('\n');
              if (line.length() == 0) break;
              int comma1 = line.indexOf(',');
              if (comma1 > 0) {
                String tsStr = line.substring(0, comma1);
                time_t tsTime = parseISO8601(tsStr);
                if (tsTime >= fromTime && tsTime <= toTime) {
                  server.sendContent(line + "\n");
                  sentLines++;
                }
              }
              yield();
            }
          }
        }
        yield();
        file = dlRoot.openNextFile();
      }
      dlRoot.close();
    }
    if (!headerSent) {
      server.sendContent("timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh\n");
    }
  }
  server.sendContent("");
  server.client().stop();
}

void handleApiFiles() {
  // No auth required - WiFi password is sufficient
  
  DynamicJsonDocument doc(2048);
  JsonArray files = doc.createNestedArray("files");
  
  File root = LittleFS.open("/logs");
  if (root && root.isDirectory()) {
    File file = root.openNextFile();
    while (file) {
      if (String(file.name()).endsWith(".csv")) {
        JsonObject fileObj = files.createNestedObject();
        fileObj["name"] = file.name();
        fileObj["size"] = file.size();
      }
      file = root.openNextFile();
    }
    root.close();
  }
  
  String response;
  serializeJson(doc, response);
  server.send(200, "application/json", response);
}

void handleApiStatus() {
  DynamicJsonDocument doc(2048);
  JsonArray sensorsArr = doc.createNestedArray("sensors");
  for (int i = 0; i < NUM_SENSORS; i++) {
    JsonObject so = sensorsArr.createNestedObject();
    so["connected"] = sensorPresent[i];
    if (sensorPresent[i]) {
      float t = NAN, h = NAN;
      if (i == 0 && sht.read(true)) {
        t = sht.getTemperature();
        h = sht.getHumidity();
      } else if (i == 1 && sht1.read(true)) {
        t = sht1.getTemperature();
        h = sht1.getHumidity();
      } else if (i == 2) {
        readSHT85Soft(I2C2_SDA, I2C2_SCL, &t, &h);
      } else if (i == 3) {
        readSHT85Soft(I2C3_SDA, I2C3_SCL, &t, &h);
      }
      if (isfinite(t) && isfinite(h)) {
        so["temperature"] = t;
        so["humidity"] = h;
      } else {
        so["temperature"] = (float)((double)0.0 / 0.0);
        so["humidity"] = (float)((double)0.0 / 0.0);
      }
    }
  }
  doc["sensor"]["connected"] = sensorPresent[0];
  if (sensorPresent[0] && sensorsArr.size() > 0) {
    doc["sensor"]["temperature"] = sensorsArr[0]["temperature"];
    doc["sensor"]["humidity"] = sensorsArr[0]["humidity"];
  }
  doc["time"]["iso"] = getISOTimestamp();
  doc["time"]["set"] = config.time_set;
  doc["time"]["epoch"] = (long)time(nullptr);
  doc["config"]["sample_period_s"] = config.sample_period_sensor[0];
  JsonArray spArr = doc["config"].createNestedArray("sample_period_sensor");
  for (size_t i = 0; i < 4; i++) spArr.add(config.sample_period_sensor[i]);
  doc["config"]["device_id"] = config.device_id;
  doc["heating"]["mode"] = config.heating_mode_sensor[0];
  JsonArray hmArr = doc["heating"].createNestedArray("mode_sensor");
  for (size_t i = 0; i < 4; i++) hmArr.add(config.heating_mode_sensor[i]);
  doc["heating"]["duration_s"] = config.heating_duration_sensor[0];
  doc["heating"]["interval_s"] = config.heating_interval_sensor[0];
  JsonArray hdArr = doc["heating"].createNestedArray("duration_sensor");
  JsonArray hiArr = doc["heating"].createNestedArray("interval_sensor");
  for (size_t i = 0; i < 4; i++) {
    hdArr.add(config.heating_duration_sensor[i]);
    hiArr.add(config.heating_interval_sensor[i]);
  }
  // Heater status: report per sensor. For soft I2C sensors we expose our scheduled state.
  JsonArray honArr = doc["heating"].createNestedArray("on_sensor");
  for (int i = 0; i < NUM_SENSORS; i++) {
    bool on = false;
    if (!sensorPresent[i]) {
      on = false;
    } else if (i == 0) {
      on = sht.isHeaterOn();
    } else if (i == 1) {
      on = sht1.isHeaterOn();
    } else {
      on = heater[i].active;
    }
    honArr.add(on);
  }
  doc["heating"]["on"] = honArr.size() > 0 ? (bool)honArr[0] : false;
  
  // Storage (ESP32: totalBytes/usedBytes)
  doc["storage"]["lfs_used"] = LittleFS.usedBytes();
  doc["storage"]["lfs_total"] = LittleFS.totalBytes();
  doc["storage"]["sd_present"] = sdPresent;
  
  // Log files and ring status (ESP32: openNextFile)
  // Don't use exists() or open() for missing files - ESP32 VFS logs "no permits for creation" on both
  String currentLog = getLogFilename();
  String currentLogBase = currentLog;
  if (currentLog.lastIndexOf('/') >= 0) {
    currentLogBase = currentLog.substring(currentLog.lastIndexOf('/') + 1);
  }
  bool currentLogExists = false;

  JsonArray files = doc.createNestedArray("log_files");
  File root = LittleFS.open("/logs");
  if (root && root.isDirectory()) {
    File file = root.openNextFile();
    while (file) {
      if (String(file.name()).endsWith(".csv")) {
        JsonObject f = files.createNestedObject();
        f["name"] = file.name();
        f["size"] = file.size();
        String fn = file.name();
        if (fn == currentLog || fn.endsWith("/" + currentLogBase) || fn == currentLogBase) {
          currentLogExists = true;
        }
      }
      file = root.openNextFile();
    }
    root.close();
  }

  JsonObject ring = doc.createNestedObject("ring");
  String ringPath = currentLogExists ? currentLog : getSparseLogPath();
  ring["file"] = ringPath;
  File file = LittleFS.open(ringPath, "r");
  if (file) {
    file.seek(0);
    String magic = file.readStringUntil('\n');
    file.seek(0);
    size_t dataStart = 0;
    unsigned long offset = 0;
    unsigned long count = 0;
    size_t slots = 0;
    size_t recordLen = LFS_RING_RECORD_LEN;
    bool ok = (magic == "RB2" && lfsRingReadHeaderSparse(file, dataStart, offset, count, slots, recordLen)) ||
              (magic == "RB1" && lfsRingReadHeader(file, dataStart, offset, count, slots, recordLen));
    ring["valid"] = ok;
    if (ok) {
      ring["data_start"] = (unsigned long)dataStart;
      ring["offset"] = offset;
      ring["count"] = count;
      ring["slots"] = (unsigned long)slots;
      ring["sparse"] = (magic == "RB2");
    }
    file.close();
  } else {
    ring["valid"] = false;
    ring["error"] = currentLogExists ? "open_failed" : "missing_file";
  }
  
  String response;
  serializeJson(doc, response);
  server.send(200, "application/json", response);
}

// ---- WebSocket protocol ----
// Device -> client messages:
//  {"type":"sample","ts":"...","sensor":0,"t":23.1,"h":45.0,"heater":true}
//  {"type":"status", ... optional snapshot ...}
// Client -> device messages:
//  {"type":"set","sample_period_sensor":[...]} 
//  {"type":"set","heating_duration_sensor":[...],"heating_interval_sensor":[...]}
// Replies:
//  {"type":"ack","ok":true}
//  {"type":"err","message":"..."}

static void wsSendJsonToAll(const String& json) {
  // WebSockets library API expects a non-const String&
  String tmp = json;
  ws.broadcastTXT(tmp);
}

static void wsSendJson(uint8_t clientNum, const String& json) {
  String tmp = json;
  ws.sendTXT(clientNum, tmp);
}

static void wsSendError(uint8_t clientNum, const char* msg) {
  DynamicJsonDocument doc(256);
  doc["type"] = "err";
  doc["message"] = msg;
  String out;
  serializeJson(doc, out);
  ws.sendTXT(clientNum, out);
}

static void wsSendAck(uint8_t clientNum, bool ok = true) {
  DynamicJsonDocument doc(128);
  doc["type"] = "ack";
  doc["ok"] = ok;
  String out;
  serializeJson(doc, out);
  ws.sendTXT(clientNum, out);
}

static bool applyWsConfigPatch(const JsonDocument& doc) {
  bool changed = false;
  bool changedSampling = false;
  bool changedHeating = false;

  if (doc.containsKey("sample_period_sensor") && doc["sample_period_sensor"].is<JsonArray>() && doc["sample_period_sensor"].size() >= 4) {
    for (size_t i = 0; i < 4; i++) {
      uint32_t period = doc["sample_period_sensor"][i] | config.sample_period_sensor[i];
      if (period < 10) period = 10;
      if (period > 604800) period = 604800;
      if (config.sample_period_sensor[i] != period) {
        config.sample_period_sensor[i] = period;
        changed = true;
        changedSampling = true;
      }
    }
    config.sample_period_s = config.sample_period_sensor[0];
  }

  if (doc.containsKey("heating_duration_sensor") && doc["heating_duration_sensor"].is<JsonArray>() && doc["heating_duration_sensor"].size() >= 4 &&
      doc.containsKey("heating_interval_sensor") && doc["heating_interval_sensor"].is<JsonArray>() && doc["heating_interval_sensor"].size() >= 4) {
    for (size_t i = 0; i < 4; i++) {
      uint32_t interval = doc["heating_interval_sensor"][i] | config.heating_interval_sensor[i];
      uint32_t duration = doc["heating_duration_sensor"][i] | config.heating_duration_sensor[i];
      if (interval == 0) duration = 0;
      else if (duration > interval) duration = interval;
      if (config.heating_interval_sensor[i] != interval || config.heating_duration_sensor[i] != duration) {
        config.heating_interval_sensor[i] = interval;
        config.heating_duration_sensor[i] = duration;
        changed = true;
        changedHeating = true;
      }
    }
    // keep legacy mode strings derived
    for (size_t i = 0; i < 4; i++) {
      uint32_t d = config.heating_duration_sensor[i];
      uint32_t inv = config.heating_interval_sensor[i];
      if (inv == 0) config.heating_mode_sensor[i] = "off";
      else if (d == 10 && inv == 300) config.heating_mode_sensor[i] = "10s_5min";
      else if (d == 60 && inv == 3600) config.heating_mode_sensor[i] = "1min_1hr";
      else if (d == 60 && inv == 86400) config.heating_mode_sensor[i] = "1min_1day";
      else config.heating_mode_sensor[i] = "off";
    }
    config.heating_mode = config.heating_mode_sensor[0];
  }

  if (changed) {
    saveConfig();
    // Reset per-sensor scheduler so new sampling periods take effect immediately
    for (int i = 0; i < 4; i++) nextReadAtMs[i] = 0;

    if (changedSampling) {
      Serial.printf("[WS] Updated sampling: S0=%lus S1=%lus S2=%lus S3=%lus\n",
                    (unsigned long)config.sample_period_sensor[0], (unsigned long)config.sample_period_sensor[1],
                    (unsigned long)config.sample_period_sensor[2], (unsigned long)config.sample_period_sensor[3]);
    }
    if (changedHeating) {
      Serial.printf("[WS] Updated heating (duration/interval): S0=%lu/%lu S1=%lu/%lu S2=%lu/%lu S3=%lu/%lu\n",
                    (unsigned long)config.heating_duration_sensor[0], (unsigned long)config.heating_interval_sensor[0],
                    (unsigned long)config.heating_duration_sensor[1], (unsigned long)config.heating_interval_sensor[1],
                    (unsigned long)config.heating_duration_sensor[2], (unsigned long)config.heating_interval_sensor[2],
                    (unsigned long)config.heating_duration_sensor[3], (unsigned long)config.heating_interval_sensor[3]);
    }
  }
  return changed;
}

static String wsMakeConfigJson() {
  DynamicJsonDocument doc(768);
  doc["type"] = "config";
  JsonArray sp = doc.createNestedArray("sample_period_sensor");
  for (int i = 0; i < 4; i++) sp.add(config.sample_period_sensor[i]);
  JsonArray hd = doc.createNestedArray("heating_duration_sensor");
  JsonArray hi = doc.createNestedArray("heating_interval_sensor");
  for (int i = 0; i < 4; i++) { hd.add(config.heating_duration_sensor[i]); hi.add(config.heating_interval_sensor[i]); }
  String out;
  serializeJson(doc, out);
  return out;
}

static void wsBroadcastConfig() {
  wsSendJsonToAll(wsMakeConfigJson());
}

static void wsSendConfig(uint8_t clientNum) {
  wsSendJson(clientNum, wsMakeConfigJson());
}

void onWsEvent(uint8_t num, WStype_t type, uint8_t * payload, size_t length) {
  switch (type) {
    case WStype_CONNECTED: {
      // Send current config immediately to this client
      wsSendAck(num, true);
      wsSendConfig(num);
      break;
    }
    case WStype_TEXT: {
      DynamicJsonDocument doc(768);
      DeserializationError err = deserializeJson(doc, payload, length);
      if (err) {
        wsSendError(num, "invalid_json");
        return;
      }
      const char* t = doc["type"] | "";
      String tt = String(t);
      if (tt == "set") {
        bool changed = applyWsConfigPatch(doc);
        wsSendAck(num, true);
        if (changed) wsBroadcastConfig();
      } else if (tt == "get") {
        // Client requests current config
        wsSendAck(num, true);
        wsSendConfig(num);
      } else {
        wsSendError(num, "unknown_type");
      }
      break;
    }
    default:
      break;
  }
}

void handleApiTestSD() {
  DynamicJsonDocument doc(512);
  
  if (!sdPresent) {
    doc["success"] = false;
    doc["error"] = "SD card not detected";
    String response;
    serializeJson(doc, response);
    server.send(200, "application/json", response);
    return;
  }
  
  // Try to write a test file
  String testFile = "/sd_test.txt";
  String testData = "FireBeetle2 SD Test - " + getISOTimestamp();
  
  File file = SD.open(testFile, FILE_WRITE);
  if (!file) {
    doc["success"] = false;
    doc["error"] = "Failed to open file for writing";
    String response;
    serializeJson(doc, response);
    server.send(200, "application/json", response);
    return;
  }
  
  file.println(testData);
  file.close();
  
  // Try to read it back
  file = SD.open(testFile, FILE_READ);
  if (!file) {
    doc["success"] = false;
    doc["error"] = "Failed to open file for reading";
    String response;
    serializeJson(doc, response);
    server.send(200, "application/json", response);
    return;
  }
  
  String readBack = file.readStringUntil('\n');
  file.close();
  
  // Delete test file
  SD.remove(testFile);
  
  if (readBack.startsWith("FireBeetle2 SD Test")) {
    doc["success"] = true;
    doc["message"] = "SD card read/write test passed";
  } else {
    doc["success"] = false;
    doc["error"] = "Data verification failed";
  }
  
  String response;
  serializeJson(doc, response);
  server.send(200, "application/json", response);
}

void handleStaticFile() {
  String path = server.uri();
  if (path == "/") path = "/index.html";
  
  // Security: prevent directory traversal
  if (path.indexOf("..") >= 0 || path.indexOf("//") >= 0) {
    server.send(403, "text/plain", "Forbidden");
    return;
  }
  
  // Ensure path starts with / for LittleFS
  if (!path.startsWith("/")) {
    path = "/" + path;
  }
  
  // Determine MIME type
  String contentType = "text/plain";
  if (path.endsWith(".html")) contentType = "text/html";
  else if (path.endsWith(".js")) contentType = "application/javascript";
  else if (path.endsWith(".css")) contentType = "text/css";
  else if (path.endsWith(".json")) contentType = "application/json";
  else if (path.endsWith(".ttf")) contentType = "font/ttf";
  
  // Static files (js, css) served without auth for login page to work
  File file = LittleFS.open(path, "r");
  if (!file) {
    // #region agent log
    Serial.printf("[Web] Static 404 %s\n", path.c_str());
    // #endregion
    server.send(404, "text/plain", "File not found");
    return;
  }
  server.streamFile(file, contentType);
  file.close();
}

void handleNotFound() {
  // Try to serve as static file first
  String path = server.uri();
  if (path.endsWith(".js") || path.endsWith(".css") || path.endsWith(".html")) {
    handleStaticFile();
    return;
  }
  // Otherwise redirect to root
  server.sendHeader("Location", "/");
  server.send(302, "text/plain", "");
}

// LED indicator (FireBeetle 2: active HIGH) — non-blocking
// Heartbeat: short pulse every HEARTBEAT_INTERVAL.
static bool hbPulseActive = false;
static unsigned long hbPulseStartMs = 0;
// Sample flash: 3 pulses.
static int sampleFlashPhase = 0; // 0=idle, 1..6 phases (ON/OFF)
static unsigned long sampleFlashPhaseMs = 0;

static void ledTick(unsigned long now) {
  // Heartbeat pulse handling
  if (hbPulseActive) {
    if (now - hbPulseStartMs >= 50) {
      digitalWrite(LED_PIN, LOW);
      hbPulseActive = false;
    }
  }
  // Sample flash handling (3 pulses: ON80/OFF80 repeated)
  if (sampleFlashPhase != 0) {
    const unsigned long phaseDur = 80;
    if (now - sampleFlashPhaseMs >= phaseDur) {
      sampleFlashPhaseMs = now;
      sampleFlashPhase++;
      if (sampleFlashPhase > 6) {
        sampleFlashPhase = 0;
        digitalWrite(LED_PIN, LOW);
      } else {
        // odd phases = ON, even phases = OFF
        digitalWrite(LED_PIN, (sampleFlashPhase % 2) ? HIGH : LOW);
      }
    }
  }
}

static void ledStartHeartbeatPulse(unsigned long now) {
  digitalWrite(LED_PIN, HIGH);
  hbPulseActive = true;
  hbPulseStartMs = now;
}

static void ledStartSampleFlash(unsigned long now) {
  if (sampleFlashPhase != 0) return; // don't stack flashes
  sampleFlashPhase = 1;
  sampleFlashPhaseMs = now;
  digitalWrite(LED_PIN, HIGH);
}

void setup() {
  Serial.begin(115200);
  delay(500);
  
  // Initialize LED indicator (FireBeetle 2: GPIO 21, active HIGH)
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);  // LED OFF
  
  Serial.println("\n\nFireBeetle 2 SHT85 Temperature/Humidity Logger");
  Serial.println("================================================");
  
  // Initialize LittleFS; format on corrupt (e.g. first boot or different board)
  if (!LittleFS.begin(false)) {  // false = do not format on first try
    Serial.println("LittleFS mount failed, formatting...");
    if (!LittleFS.begin(true)) {  // true = format then mount
      Serial.println("LittleFS still failed after format!");
      return;
    }
    Serial.println("LittleFS formatted and mounted");
  } else {
    Serial.println("LittleFS mounted");
  }
  
  // Ensure /logs/ directory exists
  if (!LittleFS.exists("/logs")) {
    LittleFS.mkdir("/logs");
    Serial.println("Created /logs directory");
  }
  
  // Initialize SD card (optional) - FireBeetle 2 SD_CS = 9
  if (SD.begin(SD_CS)) {
    sdPresent = true;
    Serial.println("SD card initialized");
  } else {
    Serial.println("SD card not present or failed");
  }
  
  // I2C for SHT85 - Bus 1 & 2 = hardware (Wire, Wire1); Bus 3 & 4 = software I2C
  Wire.begin(I2C_SDA, I2C_SCL);
  Wire1.begin(I2C1_SDA, I2C1_SCL);
  if (!sht.begin()) {
    Serial.println("SHT85 sensor 0 not found (bus 1)");
    sensorPresent[0] = false;
  } else {
    sensorPresent[0] = true;
    Serial.println("SHT85 sensor 0 OK (bus 1)");
  }
  if (!sht1.begin()) {
    Serial.println("SHT85 sensor 1 not found (bus 2)");
    sensorPresent[1] = false;
  } else {
    sensorPresent[1] = true;
    Serial.println("SHT85 sensor 1 OK (bus 2)");
  }
  float t2dummy, h2dummy;
  if (!readSHT85Soft(I2C2_SDA, I2C2_SCL, &t2dummy, &h2dummy)) {
    Serial.println("SHT85 sensor 2 not found (bus 3, soft I2C)");
    sensorPresent[2] = false;
  } else {
    sensorPresent[2] = true;
    Serial.println("SHT85 sensor 2 OK (bus 3, soft I2C)");
  }
  if (!readSHT85Soft(I2C3_SDA, I2C3_SCL, &t2dummy, &h2dummy)) {
    Serial.println("SHT85 sensor 3 not found (bus 4, soft I2C)");
    sensorPresent[3] = false;
  } else {
    sensorPresent[3] = true;
    Serial.println("SHT85 sensor 3 OK (bus 4, soft I2C)");
  }
  // #region agent log
  Serial.printf("[Sensors] present: %d %d %d %d (expect 4)\n", sensorPresent[0], sensorPresent[1], sensorPresent[2], sensorPresent[3]);
  // #endregion

  // Load or create config FIRST (before RTC check)
  if (!loadConfig()) {
    Serial.println("Creating default config");
    getDefaultConfig();
    saveConfig();
  }
  Serial.println("Config loaded: " + config.device_id);
  
  // Initialize RTC (optional) - AFTER loading config
  if (rtc.begin()) {
    rtcPresent = true;
    if (rtc.initialized() && !rtc.lostPower()) {
      // Set system time from RTC
      DateTime now = rtc.now();
      time_t rtcTime = now.unixtime();
      timeval tv = {rtcTime, 0};
      settimeofday(&tv, nullptr);
      if (!config.time_set) {
        config.time_set = true;
        saveConfig();  // Save the updated time_set flag
      }
      Serial.println("RTC initialized and time set from RTC");
    } else if (config.time_set) {
      // Config says time was set before, but RTC lost power
      Serial.println("RTC lost power, time needs to be set again");
    }
  }
  
  // Setup WiFi AP (WPA2 requires password length 8-63)
  if (config.ap_password.length() < 8 || config.ap_password.length() > 63) {
    config.ap_password = "logger123";
    saveConfig();
  }
  WiFi.mode(WIFI_AP);
  WiFi.softAPConfig(apIP, apIP, IPAddress(255, 255, 255, 0));
  delay(100);
  // Explicit channel 6, max 4 clients, WPA2-PSK (avoids ESP32-S3 default-channel issues)
  bool apOk = WiFi.softAP(config.ap_ssid.c_str(), config.ap_password.c_str(), 6, false, 4);
  if (!apOk) {
    Serial.println("ERROR: WiFi AP failed to start. Try power cycle or check password (8-63 chars).");
  }
  delay(100);
  Serial.println("AP started: " + config.ap_ssid);
  Serial.println("AP password: " + config.ap_password);
  Serial.println("AP channel: 6");
  Serial.println("AP IP: " + apIP.toString());
  
  // Setup web server routes
  server.on("/", handleRoot);
  server.on("/login", handleLogin);
  server.on("/logout", handleLogout);
  server.on("/api/config", handleApiConfig);
  server.on("/api/time", handleApiTime);
  server.on("/api/storage", handleApiStorage);
  server.on("/api/prune", handleApiPrune);
  server.on("/api/data", handleApiData);
  server.on("/api/download", handleApiDownload);
  server.on("/api/files", handleApiFiles);
  server.on("/api/status", handleApiStatus);
  server.on("/api/test-sd", handleApiTestSD);
  // Explicit static file handlers so these are not "not found" (reliable delivery)
  server.on("/index.html", HTTP_GET, handleStaticFile);
  server.on("/styles.css", HTTP_GET, handleStaticFile);
  server.on("/app.js", HTTP_GET, handleStaticFile);
  server.on("/apexcharts.min.js", HTTP_GET, handleStaticFile);
  server.on("/apexcharts-sync-plugin.js", HTTP_GET, handleStaticFile);
  server.on("/ModernDOS8x16.ttf", HTTP_GET, handleStaticFile);

  // Static files (catch-all for any other path)
  // #region agent log
  server.onNotFound([]() {
    String path = server.uri();
    const char* methodStr = (server.method() == HTTP_GET) ? "GET" : (server.method() == HTTP_POST) ? "POST" : (server.method() == 0) ? "OPTIONS" : "OTHER";
    Serial.printf("[Web] onNotFound %s %s\n", methodStr, path.c_str());
    if (server.method() == HTTP_OPTIONS) {
      server.sendHeader("Access-Control-Allow-Origin", "*");
      server.sendHeader("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
      server.sendHeader("Access-Control-Allow-Headers", "Content-Type");
      server.send(200, "text/plain", "");
      return;
    }
    if (path.startsWith("/api/") || path == "/login" || path == "/logout") {
      handleNotFound();
    } else {
      handleStaticFile();
    }
  });
  // #endregion
  
  server.begin();
  Serial.println("Web server started");

  // WebSocket server (port 81)
  ws.begin();
  ws.onEvent(onWsEvent);
  Serial.println("WebSocket server started on port 81");
  
  // Randomness: use esp_random() directly for tokens/salts (no RNG seeding required)

  Serial.println("\nSetup complete!");
  Serial.println("Connect to: " + config.ap_ssid);
  Serial.println("Password: " + config.ap_password);
  Serial.println("Sample intervals (s): S0=" + String(config.sample_period_sensor[0]) + " S1=" + String(config.sample_period_sensor[1]) + " S2=" + String(config.sample_period_sensor[2]) + " S3=" + String(config.sample_period_sensor[3]));
  Serial.println("Current time: " + getISOTimestamp());
  Serial.println("Time set: " + String(config.time_set ? "YES" : "NO"));
  Serial.println("\nDebug: http://192.168.4.1/api/status");
}

void loop() {
  server.handleClient();
  ws.loop();
  
  unsigned long now = millis();
  
  // LED tick (non-blocking)
  ledTick(now);

  // LED heartbeat pulse every HEARTBEAT_INTERVAL
  if (now - lastHeartbeat >= HEARTBEAT_INTERVAL) {
    ledStartHeartbeatPulse(now);
    lastHeartbeat = now;
  }

  // SHT85 heater state machines (non-blocking) — per sensor
  // interval = total cycle period, duration = on-time, off-time = interval - duration
  for (int i = 0; i < NUM_SENSORS; i++) {
    uint32_t durationSec = config.heating_duration_sensor[i];
    uint32_t intervalSec = config.heating_interval_sensor[i];
    if (!sensorPresent[i] || durationSec == 0 || intervalSec == 0) {
      // Ensure OFF
      if (heater[i].active) {
        if (i == 0) sht.heatOff();
        else if (i == 1) sht1.heatOff();
        else if (i == 2) softSht85HeaterOff(I2C2_SDA, I2C2_SCL);
        else if (i == 3) softSht85HeaterOff(I2C3_SDA, I2C3_SCL);
        heater[i].active = false;
      }
      continue;
    }

    const unsigned long durationMs = (unsigned long)durationSec * 1000UL;

    auto doHeatOn = [&](int idx) -> bool {
      if (idx == 0) {
        sht.setHeatTimeout(255);
        return sht.heatOn();
      } else if (idx == 1) {
        sht1.setHeatTimeout(255);
        return sht1.heatOn();
      } else if (idx == 2) {
        return softSht85HeaterOn(I2C2_SDA, I2C2_SCL);
      } else {
        return softSht85HeaterOn(I2C3_SDA, I2C3_SCL);
      }
    };

    auto doHeatOff = [&](int idx) {
      if (idx == 0) sht.heatOff();
      else if (idx == 1) sht1.heatOff();
      else if (idx == 2) softSht85HeaterOff(I2C2_SDA, I2C2_SCL);
      else softSht85HeaterOff(I2C3_SDA, I2C3_SCL);
    };

    // 100% duty cycle (duration >= interval): keep heater always on
    if (durationSec >= intervalSec) {
      if (!heater[i].active) {
        if (doHeatOn(i)) {
          heater[i].startMs = now;
          heater[i].lastRetriggerMs = now;
          heater[i].active = true;
          Serial.printf("Heater S%d ON (100%% duty, interval=%lus)\n", i, (unsigned long)intervalSec);
        }
      } else {
        // Re-trigger for library-controlled heaters (avoid timeout). For soft buses it is harmless.
        if (now - heater[i].lastRetriggerMs >= 200000UL) {
          doHeatOn(i);
          heater[i].lastRetriggerMs = now;
        }
      }
      continue;
    }

    // Normal duty cycle
    const unsigned long offTimeMs = (unsigned long)(intervalSec - durationSec) * 1000UL;

    if (heater[i].active) {
      if (now - heater[i].lastRetriggerMs >= 200000UL && (now - heater[i].startMs) < durationMs) {
        doHeatOn(i);
        heater[i].lastRetriggerMs = now;
      }
      if (now - heater[i].startMs >= durationMs) {
        doHeatOff(i);
        Serial.printf("Heater S%d OFF (ran %lus), next in %lus\n", i, (unsigned long)((now - heater[i].startMs) / 1000UL), (unsigned long)(intervalSec - durationSec));
        heater[i].lastCycleEndMs = now;
        heater[i].active = false;
      }
    } else {
      if (heater[i].lastCycleEndMs == 0 || (now - heater[i].lastCycleEndMs >= offTimeMs)) {
        if (doHeatOn(i)) {
          heater[i].startMs = now;
          heater[i].lastRetriggerMs = now;
          heater[i].active = true;
          Serial.printf("Heater S%d ON duration=%lus interval=%lus (duty=%lu%%)\n", i, (unsigned long)durationSec, (unsigned long)intervalSec, (unsigned long)(durationSec * 100UL / intervalSec));
        } else {
          heater[i].lastCycleEndMs = now; // avoid tight failure loop
        }
      }
    }
  }
  
  // Per-sensor sampling: independent schedules using nextReadAtMs[]
  // Each sensor can run at a different interval without being quantized by a global tick.
  if (sensorPresentAny()) {
    bool anyRead = false;
    bool readThisLoop[4] = { false, false, false, false };

    for (int i = 0; i < NUM_SENSORS; i++) {
      if (!sensorPresent[i]) continue;

      unsigned long periodMs = (unsigned long)config.sample_period_sensor[i] * 1000UL;
      if (periodMs < 10000UL) periodMs = 10000UL;

      if (nextReadAtMs[i] == 0) {
        // schedule first read immediately
        nextReadAtMs[i] = now;
      }

      if ((long)(now - nextReadAtMs[i]) >= 0) {
        bool ok = false;
        if (i == 0) {
          ok = sht.read(true);
          if (ok) { lastT[0] = sht.getTemperature(); lastH[0] = sht.getHumidity(); }
        } else if (i == 1) {
          ok = sht1.read(true);
          if (ok) { lastT[1] = sht1.getTemperature(); lastH[1] = sht1.getHumidity(); }
        } else if (i == 2) {
          ok = readSHT85Soft(I2C2_SDA, I2C2_SCL, &lastT[2], &lastH[2]);
        } else if (i == 3) {
          ok = readSHT85Soft(I2C3_SDA, I2C3_SCL, &lastT[3], &lastH[3]);
        }

        if (ok) {
          lastReadTime[i] = now;
        }

        // Reduce drift: keep cadence by advancing from previous schedule.
        // If we fell behind (e.g., long handler), catch up by skipping missed slots.
        unsigned long next = nextReadAtMs[i] + periodMs;
        if (nextReadAtMs[i] == 0 || (long)(now - nextReadAtMs[i]) >= 0) {
          // if nextReadAtMs was 'now' for first schedule, next becomes now+period
          next = now + periodMs;
        }
        while ((long)(now - next) >= 0) {
          next += periodMs;
        }
        nextReadAtMs[i] = next;

        if (ok) {
          readThisLoop[i] = true;
          anyRead = true;
        }
      }
    }

    if (anyRead) {
      String timestamp = getISOTimestamp();

      // Persist (LittleFS sparse ring) per sensor read
      bool lfsSuccess = false;
      bool canLfs = (LittleFS.exists("/logs") || LittleFS.mkdir("/logs"));
      String sparsePath;
      if (canLfs) sparsePath = getSparseLogPath();

      for (int i = 0; i < NUM_SENSORS; i++) {
        if (!readThisLoop[i]) continue;
        if (!isfinite(lastT[i]) || !isfinite(lastH[i])) continue;

        if (canLfs) {
          if (writeLfsRingRecordSparse(sparsePath, timestamp, i, lastT[i], lastH[i])) {
            lfsSuccess = true;
          }
        }

        // Push live sample to WebSocket clients for fast chart updates (independent of storage)
        DynamicJsonDocument d(256);
        d["type"] = "sample";
        d["ts"] = timestamp;
        d["sensor"] = i;
        d["t"] = lastT[i];
        d["h"] = lastH[i];
        bool hon = false;
        if (i == 0) hon = sht.isHeaterOn();
        else if (i == 1) hon = sht1.isHeaterOn();
        else hon = heater[i].active;
        d["heater"] = hon;
        String out;
        serializeJson(d, out);
        wsSendJsonToAll(out);
      }

      // Optional SD write: writes a full row with last-known values
      bool sdSuccess = writeSDDataPoint(timestamp, lastT[0], lastH[0], lastT[1], lastH[1], lastT[2], lastH[2], lastT[3], lastH[3]);

      if (lfsSuccess || sdSuccess) {
        ledStartSampleFlash(now);
      } else {
        writeErrors++;
      }

      lastSampleTime = now;
    }
  }
  
  yield();
}
