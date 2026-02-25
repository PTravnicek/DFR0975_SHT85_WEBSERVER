#include <Arduino.h>
#include <WiFi.h>
#include <AsyncTCP.h>
#include <ESPAsyncWebServer.h>
#include <LittleFS.h>
#include <SD.h>
#include <SPI.h>
#include <Wire.h>
#include <ArduinoJson.h>
#include <SHT85.h>
#include <RTClib.h>
#include <time.h>
#include <mbedtls/sha256.h>
#ifdef ESP32
#include <esp_heap_caps.h>
#endif
#include <esp_task_wdt.h>

#include <freertos/FreeRTOS.h>
#include <freertos/semphr.h>

#include "time_util.h"
#include "soft_i2c_sht85.h"
#include "i2c_recovery.h"

// Hardware objects (SHT85 I2C default address 0x44)
// ESP32-S3 has only 2 I2C peripherals: Wire and Wire1. Sensors 2 and 3 use software I2C
// (bit-bang) on their own SDA/SCL pairs so all 4 buses work with your existing wiring.
SHT85 sht(0x44, &Wire);
SHT85 sht1(0x44, &Wire1);

// (soft I2C SHT85 helpers moved to soft_i2c_sht85.cpp)
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
AsyncWebServer server(80);
AsyncWebSocket ws("/ws");
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
uint16_t sensorFailStreak[4] = { 0, 0, 0, 0 };
unsigned long sensorOfflineUntilMs[4] = { 0, 0, 0, 0 };
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
// Max bytes reserved for telemetry ring buffer inside LittleFS.
// With 16MB flash + single factory app, LittleFS is much larger; default to 10MB.
#define LFS_RING_MAX_BYTES_DEFAULT (10 * 1024 * 1024)
#endif
#ifndef LFS_RING_RECORD_LEN_DEFAULT
#define LFS_RING_RECORD_LEN_DEFAULT 96
#endif
#define LFS_RING_RECORD_LEN_EXT 128  // 4-sensor format (timestamp,device_id,t0,h0,t1,h1,t2,h2,t3,h3)
#define LFS_RING_RECORD_LEN_SPARSE 48  // RB2: one record per sensor read (timestamp,sensor_id,t,h)
const size_t LFS_RING_MAX_BYTES = LFS_RING_MAX_BYTES_DEFAULT;
const size_t LFS_RING_RECORD_LEN = LFS_RING_RECORD_LEN_DEFAULT;

// Helper functions

// LittleFS mount status (so we don't call LittleFS.begin() multiple times)
static bool g_lfsOk = false;

// LittleFS is not safe to access concurrently from multiple contexts (loop + async_tcp callbacks).
// Serialize all LittleFS operations to avoid deadlocks/corruption that can wedge async_tcp and trip WDT.
static SemaphoreHandle_t g_fsMutex = nullptr;

struct FsGuard {
  bool locked = false;
  FsGuard(uint32_t timeoutMs = 2000) {
    if (!g_fsMutex) return;
    locked = (xSemaphoreTake(g_fsMutex, pdMS_TO_TICKS(timeoutMs)) == pdTRUE);
  }
  ~FsGuard() {
    if (locked && g_fsMutex) xSemaphoreGive(g_fsMutex);
  }
};

// In-RAM ring buffer for recent samples — serves /api/recent without touching LittleFS.
// Each entry holds one sparse record (timestamp, sensor_id, temperature, humidity).
static const int RAM_RING_SIZE = 512;
struct RamSample {
  char ts[28];     // ISO timestamp
  int8_t sensor;   // 0–3 or -1 if unused
  float t, h;
};
static RamSample g_ramRing[RAM_RING_SIZE];
static volatile int g_ramHead = 0;   // next write position
static volatile int g_ramCount = 0;  // number of valid entries (≤ RAM_RING_SIZE)
static portMUX_TYPE g_ramSpinlock = portMUX_INITIALIZER_UNLOCKED;

static void ramRingPush(const char* ts, int sensor, float t, float h) {
  portENTER_CRITICAL(&g_ramSpinlock);
  RamSample& s = g_ramRing[g_ramHead];
  strncpy(s.ts, ts, sizeof(s.ts) - 1);
  s.ts[sizeof(s.ts) - 1] = '\0';
  s.sensor = (int8_t)sensor;
  s.t = t;
  s.h = h;
  g_ramHead = (g_ramHead + 1) % RAM_RING_SIZE;
  if (g_ramCount < RAM_RING_SIZE) g_ramCount++;
  portEXIT_CRITICAL(&g_ramSpinlock);
}

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

String getSessionTokenFromCookieHeader(const String& cookie) {
  if (cookie.length() == 0) return "";
  int start = cookie.indexOf("SESSION=");
  if (start == -1) return "";
  start += 8;
  int end = cookie.indexOf(";", start);
  if (end == -1) end = cookie.length();
  return cookie.substring(start, end);
}

// Legacy helpers for WebServer were removed; auth is currently not enforced on APIs.
String getSessionToken() {
  return "";
}

String getSessionTokenFromRequest(AsyncWebServerRequest* request) {
  if (!request) return "";
  if (!request->hasHeader("Cookie")) return "";
  const AsyncWebHeader* h = request->getHeader("Cookie");
  if (!h) return "";
  return getSessionTokenFromCookieHeader(h->value());
}

bool isAuthenticated(AsyncWebServerRequest* request) {
  String token = getSessionTokenFromRequest(request);
  if (token.length() == 0) return false;
  unsigned long now = millis();
  for (int i = 0; i < sessionCount; i++) {
    if (authSessions[i].token == token && authSessions[i].expires > now) {
      return true;
    }
  }
  return false;
}

// Back-compat: keep old isAuthenticated() signature unused
bool isAuthenticated() {
  return false;
}

// Session management helpers (auth currently not enforced in Async handlers)
void addSession(const String& token) {
  if (sessionCount >= 10) {
    for (int i = 0; i < 9; i++) authSessions[i] = authSessions[i + 1];
    sessionCount = 9;
  }
  authSessions[sessionCount].token = token;
  authSessions[sessionCount].expires = millis() + SESSION_TIMEOUT;
  sessionCount++;
}

void removeSession(const String& token) {
  for (int i = 0; i < sessionCount; i++) {
    if (authSessions[i].token == token) {
      for (int j = i; j < sessionCount - 1; j++) authSessions[j] = authSessions[j + 1];
      sessionCount--;
      break;
    }
  }
}

static bool requireAuth(AsyncWebServerRequest* request) {
  if (!isAuthenticated(request)) {
    request->redirect("/login");
    return false;
  }
  return true;
}

static bool requireApiAuth(AsyncWebServerRequest* request) {
  if (!isAuthenticated(request)) {
    request->send(401, "application/json", "{\"error\":\"Unauthorized\"}");
    return false;
  }
  return true;
}

// Forward declarations (needed for lambdas in setup/loop)
static void wsSendJsonToAll(const String& json);
static bool applyWsConfigPatch(const JsonDocument& doc);
static void onWsEvent(AsyncWebSocket* serverPtr, AsyncWebSocketClient* client, AwsEventType type,
                      void* arg, uint8_t* data, size_t len);

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
  
  DynamicJsonDocument doc(2048);
  DeserializationError error = deserializeJson(doc, content);
  if (error) {
    // #region agent log
    Serial.printf("[DBG] loadConfig FAILED: %s content_len=%u\n", error.c_str(), (unsigned)content.length());
    // #endregion
    return false;
  }
  // #region agent log
  Serial.printf("[DBG] loadConfig OK: json_mem=%u/%u content_len=%u\n", (unsigned)doc.memoryUsage(), 2048U, (unsigned)content.length());
  // #endregion
  
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
  DynamicJsonDocument doc(2048);
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
// (time helpers moved to time_util.cpp)

// Forward declaration (used by getSparseLogPath before the implementation below)
void lfsRingWriteHeaderSparse(File& file, unsigned long offset, unsigned long count);
static bool g_sparseFileVerified = false;

// Path for sparse (RB2) writes.
// NOTE: On ESP32 VFS, checking existence/opening missing files can spam logs.
// We therefore cache the chosen path and only probe/create once per month.
String getSparseLogPath() {
  // This function touches LittleFS; guard it because it can be called from loop (writer)
  // and from async handlers (readers).
  FsGuard g(500);
  // If we cannot lock quickly, fall back to a deterministic path without probing/creating.
  if (!g.locked) {
    return getLogFilename();
  }

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
  g_sparseFileVerified = false;
  return cachedPath;
}

// normalizeMonthStart moved to time_util.cpp

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
  // Keep this lightweight: getSparseLogPath() tries to create once/month,
  // but we still must handle cases where /logs is missing or the file was deleted.

  // Ensure /logs exists (some VFS implementations fail file creation if parent dir is missing).
  if (!LittleFS.exists("/logs")) {
    LittleFS.mkdir("/logs");
  }

  File r = LittleFS.open(path, "r");
  if (r) {
    r.close();
    return true;
  }

  // Create new file with header.
  File w = LittleFS.open(path, "w");
  if (!w) {
    // Retry once after ensuring directory.
    LittleFS.mkdir("/logs");
    w = LittleFS.open(path, "w");
    if (!w) return false;
  }
  lfsRingWriteHeaderSparse(w, 0, 0);
  w.close();
  return true;
}

// Write one sparse record (RB2): timestamp,sensor_id,t,h. Returns true on success.
// Optimized: single open/close, cached existence check.
static bool writeLfsRingRecordSparse(const String& path, const String& timestamp, int sensorId, float t, float h) {
  FsGuard g(500);
  if (!g.locked) return false;

  if (!g_sparseFileVerified) {
    if (!lfsEnsureSparseLogFileExists(path)) return false;
    g_sparseFileVerified = true;
  }

  File file = LittleFS.open(path, "r+");
  if (!file) {
    g_sparseFileVerified = false;
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
  if (written != recordLen) {
    file.close();
    return false;
  }

  offset = (offset + 1) % slots;
  if (count < slots) count++;

  lfsRingWriteHeaderSparse(file, offset, count);
  file.close();
  return true;
}

// Write up to 4 sparse records in one open/close to avoid ~1.5s per close (flash sync).
// writeSensor[i] and finite lastT[i]/lastH[i] determine which sensors to append.
// Returns true if at least one record was written.
static bool writeLfsRingRecordsSparseBatch(const String& path, const String& timestamp,
    const bool writeSensor[4], const float lastT[4], const float lastH[4], int* numWritten) {
  if (numWritten) *numWritten = 0;
  FsGuard g(500);
  if (!g.locked) return false;
  if (!g_sparseFileVerified) {
    if (!lfsEnsureSparseLogFileExists(path)) return false;
    g_sparseFileVerified = true;
  }
  File file = LittleFS.open(path, "r+");
  if (!file) {
    g_sparseFileVerified = false;
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
  int written = 0;
  for (int i = 0; i < 4; i++) {
    if (!writeSensor[i] || !isfinite(lastT[i]) || !isfinite(lastH[i])) continue;
    int n = snprintf(buf, sizeof(buf), "%-24s,%d,%6.2f,%6.2f\n",
                     timestamp.c_str(), i, (double)lastT[i], (double)lastH[i]);
    if (n < 0 || (size_t)n >= recordLen) continue;
    while ((size_t)n < recordLen - 1) buf[n++] = ' ';
    buf[recordLen - 1] = '\n';
    buf[recordLen] = '\0';
    size_t pos = dataStart + (offset * recordLen);
    file.seek(pos);
    if (file.print(buf) != recordLen) break;
    offset = (offset + 1) % slots;
    if (count < slots) count++;
    written++;
  }
  if (written == 0) {
    file.close();
    return false;
  }
  lfsRingWriteHeaderSparse(file, offset, count);
  file.close();
  if (numWritten) *numWritten = written;
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

// Combined sampling rate across all sensors (records/second).
// Each sensor produces 1 record per its sampling period.
static double combinedSampleRateSps() {
  double sps = 0.0;
  for (int i = 0; i < 4; i++) {
    // If a sensor is not present, it won't produce records.
    if (!sensorPresent[i]) continue;
    const uint32_t p = config.sample_period_sensor[i];
    if (p == 0) continue;
    sps += 1.0 / (double)p;
  }
  return sps;
}

// API Handlers
#if 0
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
  
  const size_t lfsTotal = LittleFS.totalBytes();
  const size_t lfsUsed = LittleFS.usedBytes();
  const size_t freeBytes = (lfsTotal > lfsUsed) ? (lfsTotal - lfsUsed) : 0;

  const size_t bytesPerSample = estimateSampleBytes();

  // Capacity is limited by (a) ring cap, and (b) what is actually free right now.
  size_t capacityBytes = freeBytes;
  if (capacityBytes > LFS_RING_MAX_BYTES) capacityBytes = LFS_RING_MAX_BYTES;

  // Estimate records + retention time based on *combined* sampling rate across sensors.
  unsigned long estSamples = 0;     // "samples" here = log records written
  unsigned long estDuration = 0;    // seconds

  const double sps = combinedSampleRateSps();
  if (bytesPerSample > 0 && sps > 0.0) {
    estSamples = (unsigned long)(capacityBytes / bytesPerSample);
    estDuration = (unsigned long)((double)estSamples / sps);
  }

  DynamicJsonDocument doc(1024);
  doc["lfs"]["total"] = lfsTotal;
  doc["lfs"]["used"] = lfsUsed;
  doc["lfs"]["free"] = freeBytes;
  doc["sd"]["present"] = sdPresent;
  doc["write_errors"] = writeErrors;

  // Back-compat: keep sample_period_s, but report sensor0 period (UI uses per-sensor arrays elsewhere)
  doc["sample_period_s"] = config.sample_period_sensor[0];

  doc["retention"]["bytes_per_sample"] = bytesPerSample;
  doc["retention"]["capacity_bytes"] = capacityBytes;
  doc["retention"]["sample_rate_sps"] = sps;
  JsonArray sp = doc["retention"].createNestedArray("sample_period_sensor");
  for (int i = 0; i < 4; i++) sp.add(config.sample_period_sensor[i]);
  JsonArray present = doc["retention"].createNestedArray("sensor_present");
  for (int i = 0; i < 4; i++) present.add(sensorPresent[i]);

  doc["retention"]["est_samples"] = estSamples;
  doc["retention"]["est_duration_s"] = estDuration;
  
    // SD card size info not always available
    doc["sd"]["total"] = 0;
    doc["sd"]["used"] = 0;
    doc["sd"]["free"] = 0;
  
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

// (parseISO8601 moved to time_util.cpp)
#endif

#if 0 // legacy synchronous handlers (WebServer-style). Async versions are registered in setup().
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

#endif // legacy synchronous handlers

// (time parsing helpers moved to time_util.cpp)
// ---- WebSocket protocol (AsyncWebSocket) ----
// Device -> client messages:
//  {"type":"sample","ts":"...","sensor":0,"t":23.1,"h":45.0,"heater":true}
//  {"type":"config","sample_period_sensor":[...],"heating_duration_sensor":[...],"heating_interval_sensor":[...]}
// Client -> device messages:
//  {"type":"set", ...}
//  {"type":"get"}
// Replies:
//  {"type":"ack","ok":true}
//  {"type":"err","message":"..."}

static void wsSendJsonToAll(const String& json) {
  ws.textAll(json);
}

static void wsSendJson(AsyncWebSocketClient* client, const String& json) {
  if (!client) return;
  client->text(json);
}

static void wsSendError(AsyncWebSocketClient* client, const char* msg) {
  DynamicJsonDocument doc(256);
  doc["type"] = "err";
  doc["message"] = msg;
  String out;
  serializeJson(doc, out);
  wsSendJson(client, out);
}

static void wsSendAck(AsyncWebSocketClient* client, bool ok = true) {
  DynamicJsonDocument doc(128);
  doc["type"] = "ack";
  doc["ok"] = ok;
  String out;
  serializeJson(doc, out);
  wsSendJson(client, out);
}

static bool applyWsConfigPatch(const JsonDocument& doc) {
  bool changed = false;
  bool changedSampling = false;
  bool changedHeating = false;

  if (doc.containsKey("sample_period_sensor")) {
    // #region agent log
    Serial.printf("[CFG] patch has sample_period_sensor: isArrayConst=%d size=%d\n",
                  (int)doc["sample_period_sensor"].is<JsonArrayConst>(),
                  (int)(doc["sample_period_sensor"].is<JsonArrayConst>() ? doc["sample_period_sensor"].size() : -1));
    // #endregion
  }

  if (doc.containsKey("sample_period_sensor") && doc["sample_period_sensor"].is<JsonArrayConst>() && doc["sample_period_sensor"].size() >= 4) {
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

  if (doc.containsKey("heating_duration_sensor") && doc["heating_duration_sensor"].is<JsonArrayConst>() && doc["heating_duration_sensor"].size() >= 4 &&
      doc.containsKey("heating_interval_sensor") && doc["heating_interval_sensor"].is<JsonArrayConst>() && doc["heating_interval_sensor"].size() >= 4) {
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

static void wsSendConfig(AsyncWebSocketClient* client) {
  wsSendJson(client, wsMakeConfigJson());
}

static void onWsEvent(AsyncWebSocket* serverPtr, AsyncWebSocketClient* client, AwsEventType type,
                      void* arg, uint8_t* data, size_t len) {
  (void)serverPtr;
  if (!client) return;

  if (type == WS_EVT_CONNECT) {
    wsSendAck(client, true);
    wsSendConfig(client);
    return;
  }

  if (type != WS_EVT_DATA) return;

  AwsFrameInfo* info = (AwsFrameInfo*)arg;
  if (!info || !info->final || info->index != 0 || info->len != len || info->opcode != WS_TEXT) {
    return;
  }

  DynamicJsonDocument doc(768);
  DeserializationError err = deserializeJson(doc, data, len);
  if (err) {
    wsSendError(client, "invalid_json");
    return;
  }

  const char* t = doc["type"] | "";
  String tt = String(t);
  if (tt == "set") {
    bool changed = applyWsConfigPatch(doc);
    wsSendAck(client, true);
    if (changed) wsBroadcastConfig();
  } else if (tt == "get") {
    wsSendAck(client, true);
    wsSendConfig(client);
  } else {
    wsSendError(client, "unknown_type");
  }
}

#if 0
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
#endif

#if 0
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
#endif

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

// Simple AP health/restart (best-effort)
static unsigned long apLastCheckMs = 0;   // set to millis() after AP start
static unsigned long apRestartAfterMs = 0;
static unsigned long apBackoffMs = 1000;

void setup() {
  Serial.begin(115200);
  delay(500);

  // LittleFS GC can block for 2-5 s, which starves async_tcp and trips the
  // default 5 s task WDT.  Increase to 30 s so the WDT only fires on genuine hangs.
  esp_task_wdt_init(30, true);
  
  // Initialize LED indicator (FireBeetle 2: GPIO 21, active HIGH)
  pinMode(LED_PIN, OUTPUT);
  digitalWrite(LED_PIN, LOW);  // LED OFF
  
  Serial.println("\n\nFireBeetle 2 SHT85 Temperature/Humidity Logger");
  Serial.println("================================================");
  
  // Establish non-empty defaults early so WiFi AP can still come up even if FS fails.
  // (FS is used for persistence + serving UI, but AP should be recoverable.)
  getDefaultConfig();

  // Serialize LittleFS access across tasks/cores.
  g_fsMutex = xSemaphoreCreateMutex();
  if (!g_fsMutex) {
    Serial.println("[ERR] FS mutex create failed (LittleFS concurrency may WDT)");
  }
  
  // Initialize LittleFS; format on corrupt (e.g. first boot or different board)
  // NOTE: ESP32 partition CSV tooling commonly supports subtype "spiffs" only; LittleFS uses that partition.
  // The Arduino LittleFS implementation defaults to partition label "spiffs" unless overridden.
  g_lfsOk = LittleFS.begin(false, "/littlefs", 10, "spiffs"); // false = do not format on first try
  if (!g_lfsOk) {
    Serial.println("LittleFS mount failed, formatting...");
    g_lfsOk = LittleFS.begin(true, "/littlefs", 10, "spiffs"); // true = format then mount
    if (!g_lfsOk) {
      Serial.println("LittleFS still failed after format! Continuing with defaults (no persistence, no web UI files).");
    } else {
    Serial.println("LittleFS formatted and mounted");
    }
  } else {
    Serial.println("LittleFS mounted");
  }
  
  if (g_lfsOk) {
  // Ensure /logs/ directory exists
  if (!LittleFS.exists("/logs")) {
    LittleFS.mkdir("/logs");
    Serial.println("Created /logs directory");
    }
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
  #if defined(ESP32)
  Wire.setTimeOut(50);
  Wire1.setTimeOut(50);
  #endif
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
  // Only attempt persistence when LittleFS is mounted; otherwise keep defaults.
  if (g_lfsOk) {
  if (!loadConfig()) {
    Serial.println("Creating default config");
      // defaults were already set; just persist them
    saveConfig();
  }
  Serial.println("Config loaded: " + config.device_id);
  } else {
    Serial.println("Config using defaults (LittleFS not available): " + config.device_id);
  }
  
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

  // WiFi robustness: log events + avoid power-save surprises
  WiFi.setSleep(false);
  WiFi.onEvent([](arduino_event_id_t event, arduino_event_info_t info) {
    switch (event) {
      case ARDUINO_EVENT_WIFI_AP_START:
        Serial.println("[WiFi] AP_START");
        break;
      case ARDUINO_EVENT_WIFI_AP_STOP:
        Serial.println("[WiFi] AP_STOP");
        break;
      case ARDUINO_EVENT_WIFI_AP_STACONNECTED:
        Serial.printf("[WiFi] STA_CONNECTED: %02x:%02x:%02x:%02x:%02x:%02x\n",
                      info.wifi_ap_staconnected.mac[0], info.wifi_ap_staconnected.mac[1], info.wifi_ap_staconnected.mac[2],
                      info.wifi_ap_staconnected.mac[3], info.wifi_ap_staconnected.mac[4], info.wifi_ap_staconnected.mac[5]);
        break;
      case ARDUINO_EVENT_WIFI_AP_STADISCONNECTED:
        Serial.printf("[WiFi] STA_DISCONNECTED: %02x:%02x:%02x:%02x:%02x:%02x\n",
                      info.wifi_ap_stadisconnected.mac[0], info.wifi_ap_stadisconnected.mac[1], info.wifi_ap_stadisconnected.mac[2],
                      info.wifi_ap_stadisconnected.mac[3], info.wifi_ap_stadisconnected.mac[4], info.wifi_ap_stadisconnected.mac[5]);
        break;
      default:
        break;
    }
  });

  WiFi.mode(WIFI_AP);
  WiFi.softAPConfig(apIP, apIP, IPAddress(255, 255, 255, 0));
  delay(100);
  // Explicit channel 6, max 4 clients
  bool apOk = WiFi.softAP(config.ap_ssid.c_str(), config.ap_password.c_str(), 6, false, 4);
  if (!apOk) {
    Serial.println("ERROR: WiFi AP failed to start. Try power cycle or check password (8-63 chars).");
  }
  delay(100);
  Serial.println("AP started: " + config.ap_ssid);
  Serial.println("AP password: " + config.ap_password);
  Serial.println("AP channel: 6");
  Serial.println("AP IP: " + apIP.toString());
  apLastCheckMs = millis();  // suppress health-check for the first 5 s after AP start
  
  // (Async migration) old synchronous WebServer route setup removed
  
  // Web server routes

  // WebSocket
  ws.onEvent(onWsEvent);
  server.addHandler(&ws);

  // /health (tiny)
  server.on("/health", HTTP_GET, [](AsyncWebServerRequest* request) {
    // Keep this small and deterministic.
    DynamicJsonDocument doc(768);
    doc["ok"] = true;
    doc["uptime_ms"] = (unsigned long)millis();
    doc["heap_free"] = (unsigned long)ESP.getFreeHeap();
    #ifdef ESP32
    // largest free block helps detect fragmentation
    doc["heap_largest_free"] = (unsigned long)heap_caps_get_largest_free_block(MALLOC_CAP_8BIT);
    #endif
    doc["lfs_total"] = (unsigned long)LittleFS.totalBytes();
    doc["lfs_used"] = (unsigned long)LittleFS.usedBytes();
    doc["sd_present"] = sdPresent;
    doc["ws_clients"] = (unsigned long)ws.count();
    doc["ap_clients"] = (unsigned long)WiFi.softAPgetStationNum();
    doc["wifi_channel"] = (unsigned long)WiFi.channel();
    doc["time_iso"] = getISOTimestamp();

    // Sensor ages (ms since last read), null if never
    JsonArray age = doc.createNestedArray("sensor_age_ms");
    unsigned long now = millis();
    for (int i = 0; i < NUM_SENSORS; i++) {
      if (!sensorPresent[i] || lastReadTime[i] == 0) age.add(nullptr);
      else age.add((unsigned long)(now - lastReadTime[i]));
    }

    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // API: status
  server.on("/api/status", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(2048);
    JsonArray sensorsArr = doc.createNestedArray("sensors");
    for (int i = 0; i < NUM_SENSORS; i++) {
      JsonObject so = sensorsArr.createNestedObject();
      so["connected"] = sensorPresent[i];
      if (sensorPresent[i]) {
        so["temperature"] = (isfinite(lastT[i]) ? lastT[i] : (float)((double)0.0 / 0.0));
        so["humidity"] = (isfinite(lastH[i]) ? lastH[i] : (float)((double)0.0 / 0.0));
      }
    }
    doc["time"]["iso"] = getISOTimestamp();
    doc["time"]["set"] = config.time_set;
    doc["time"]["epoch"] = (long)time(nullptr);

    doc["config"]["sample_period_s"] = config.sample_period_sensor[0];
    JsonArray spArr = doc["config"].createNestedArray("sample_period_sensor");
    for (int i = 0; i < 4; i++) spArr.add(config.sample_period_sensor[i]);

    JsonArray honArr = doc["heating"].createNestedArray("on_sensor");
    for (int i = 0; i < NUM_SENSORS; i++) {
      bool on = false;
      if (!sensorPresent[i]) on = false;
      else if (i == 0) on = sht.isHeaterOn();
      else if (i == 1) on = sht1.isHeaterOn();
      else on = heater[i].active;
      honArr.add(on);
    }

    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // API: config (GET)
  server.on("/api/config", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(768);
    doc["device_id"] = config.device_id;
    doc["sample_period_s"] = config.sample_period_sensor[0];
    JsonArray sp = doc.createNestedArray("sample_period_sensor");
    for (int i = 0; i < 4; i++) sp.add(config.sample_period_sensor[i]);
    doc["log_timezone_offset_min"] = config.log_timezone_offset_min;
    doc["file_rotation"] = config.file_rotation;
    doc["time_set"] = config.time_set;
    doc["heating_mode"] = config.heating_mode_sensor[0];
    JsonArray hm = doc.createNestedArray("heating_mode_sensor");
    for (int i = 0; i < 4; i++) hm.add(config.heating_mode_sensor[i]);
    JsonArray hd = doc.createNestedArray("heating_duration_sensor");
    JsonArray hi = doc.createNestedArray("heating_interval_sensor");
    for (int i = 0; i < 4; i++) { hd.add(config.heating_duration_sensor[i]); hi.add(config.heating_interval_sensor[i]); }
    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // API: config (POST) body handler
  server.on("/api/config", HTTP_POST,
    [](AsyncWebServerRequest* request) {},
    NULL,
    [](AsyncWebServerRequest* request, uint8_t* data, size_t len, size_t index, size_t total) {
      static String body;
      if (index == 0) {
        body = "";
        body.reserve(total);
      }
      // IMPORTANT: data is not null-terminated; append exact bytes
      body.concat((const char*)data, len);
      if (index + len != total) return;

      DynamicJsonDocument doc(768);
      DeserializationError derr = deserializeJson(doc, body);
      if (derr) {
        // #region agent log
        Serial.printf("[HTTP] /api/config invalid_json: %s body_len=%u\n", derr.c_str(), (unsigned)body.length());
        // #endregion
        request->send(400, "application/json", "{\"error\":\"Invalid JSON\"}");
        return;
      }
      // #region agent log
      Serial.printf("[HTTP] /api/config POST body_len=%u\n", (unsigned)body.length());
      // #endregion
      bool changed = applyWsConfigPatch(doc);
      // #region agent log
      Serial.printf("[HTTP] /api/config applied changed=%d now S0=%lus S1=%lus S2=%lus S3=%lus\n",
                    (int)changed,
                    (unsigned long)config.sample_period_sensor[0], (unsigned long)config.sample_period_sensor[1],
                    (unsigned long)config.sample_period_sensor[2], (unsigned long)config.sample_period_sensor[3]);
      // #endregion
      DynamicJsonDocument resp(256);
      resp["success"] = true;
      resp["changed"] = changed;
      JsonArray sp = resp.createNestedArray("sample_period_sensor");
      for (int i = 0; i < 4; i++) sp.add(config.sample_period_sensor[i]);
      String out;
      serializeJson(resp, out);
      request->send(200, "application/json", out);
    }
  );

  // API: time
  server.on("/api/time", HTTP_GET, [](AsyncWebServerRequest* request) {
    time_t now = time(nullptr);
    DynamicJsonDocument doc(256);
    doc["epoch"] = now;
    doc["time_set"] = config.time_set && (now >= 946684800);
    doc["iso"] = getISOTimestamp();
    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  server.on("/api/time", HTTP_POST,
    [](AsyncWebServerRequest* request) {},
    NULL,
    [](AsyncWebServerRequest* request, uint8_t* data, size_t len, size_t index, size_t total) {
      static String body;
      if (index == 0) {
        body = "";
        body.reserve(total);
      }
      // IMPORTANT: data is not null-terminated; append exact bytes
      body.concat((const char*)data, len);
      if (index + len != total) return;

      DynamicJsonDocument doc(256);
      DeserializationError derr = deserializeJson(doc, body);
      if (derr) {
        // #region agent log
        Serial.printf("[HTTP] /api/time invalid_json: %s body_len=%u\n", derr.c_str(), (unsigned)body.length());
        // #endregion
        request->send(400, "application/json", "{\"error\":\"Invalid JSON\"}");
        return;
      }
      if (!doc.containsKey("epoch")) {
        request->send(400, "application/json", "{\"error\":\"Missing epoch\"}");
        return;
      }
      time_t epoch = doc["epoch"];
      timeval tv = {epoch, 0};
      settimeofday(&tv, nullptr);
      config.time_set = true;
      saveConfig();
      if (rtcPresent) {
        struct tm* timeinfo = localtime(&epoch);
        rtc.adjust(DateTime(timeinfo->tm_year + 1900, timeinfo->tm_mon + 1,
                           timeinfo->tm_mday, timeinfo->tm_hour,
                           timeinfo->tm_min, timeinfo->tm_sec));
      }
      request->send(200, "application/json", "{\"success\":true}");
    }
  );

  // ---- Migrated Async APIs ----

  // /api/storage
  server.on("/api/storage", HTTP_GET, [](AsyncWebServerRequest* request) {
    const size_t lfsTotal = LittleFS.totalBytes();
    const size_t lfsUsed = LittleFS.usedBytes();
    const size_t freeBytes = (lfsTotal > lfsUsed) ? (lfsTotal - lfsUsed) : 0;

    const size_t bytesPerSample = estimateSampleBytes();

    // Capacity for *new* telemetry, based on currently free space and ring cap.
    size_t capacityBytes = freeBytes;
    if (capacityBytes > LFS_RING_MAX_BYTES) capacityBytes = LFS_RING_MAX_BYTES;

    unsigned long estSamples = 0;   // records
    unsigned long estDuration = 0;  // seconds

    const double sps = combinedSampleRateSps();
    if (bytesPerSample > 0 && sps > 0.0) {
      estSamples = (unsigned long)(capacityBytes / bytesPerSample);
      estDuration = (unsigned long)((double)estSamples / sps);
    }

    DynamicJsonDocument doc(1024);
    doc["lfs"]["total"] = lfsTotal;
    doc["lfs"]["used"] = lfsUsed;
    doc["lfs"]["free"] = freeBytes;
    doc["sd"]["present"] = sdPresent;
    doc["write_errors"] = writeErrors;

    // Back-compat
    doc["sample_period_s"] = config.sample_period_sensor[0];

    doc["retention"]["bytes_per_sample"] = bytesPerSample;
    doc["retention"]["capacity_bytes"] = capacityBytes;
    doc["retention"]["sample_rate_sps"] = sps;
    JsonArray sp = doc["retention"].createNestedArray("sample_period_sensor");
    for (int i = 0; i < 4; i++) sp.add(config.sample_period_sensor[i]);
    JsonArray present = doc["retention"].createNestedArray("sensor_present");
    for (int i = 0; i < 4; i++) present.add(sensorPresent[i]);

    doc["retention"]["est_samples"] = estSamples;
    doc["retention"]["est_duration_s"] = estDuration;

    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // /api/files
  server.on("/api/files", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(2048);
    JsonArray files = doc.createNestedArray("files");

    File root = LittleFS.open("/logs");
    if (root && root.isDirectory()) {
      File file = root.openNextFile();
      while (file) {
        String fn = file.name();
        if (fn.endsWith(".csv")) {
          JsonObject fo = files.createNestedObject();
          fo["name"] = fn;
          fo["size"] = (unsigned long)file.size();
        }
        file = root.openNextFile();
      }
      root.close();
    }

    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // /api/lfs/delete?name=/logs/2026-02.csv -> delete one LittleFS log file
  server.on("/api/lfs/delete", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(512);

    if (!request->hasParam("name")) {
      doc["success"] = false;
      doc["error"] = "Missing name";
      String out; serializeJson(doc, out);
      request->send(400, "application/json", out);
      return;
    }

    String name = request->getParam("name")->value();
    if (!name.startsWith("/logs/") || !name.endsWith(".csv") || name.indexOf("..") >= 0) {
      doc["success"] = false;
      doc["error"] = "Invalid file name";
      String out; serializeJson(doc, out);
      request->send(400, "application/json", out);
      return;
    }

    FsGuard fs(2000);
    if (!fs.locked) {
      doc["success"] = false;
      doc["error"] = "Filesystem busy";
      String out; serializeJson(doc, out);
      request->send(503, "application/json", out);
      return;
    }

    if (!LittleFS.exists(name)) {
      doc["success"] = false;
      doc["error"] = "File not found";
      String out; serializeJson(doc, out);
      request->send(404, "application/json", out);
      return;
    }

    bool ok = LittleFS.remove(name);
    doc["success"] = ok;
    doc["name"] = name;
    if (!ok) doc["error"] = "Delete failed";

    String out; serializeJson(doc, out);
    request->send(ok ? 200 : 500, "application/json", out);
  });

  // /api/lfs/clear -> delete ALL /logs/*.csv in LittleFS
  server.on("/api/lfs/clear", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(512);

    FsGuard fs(5000);
    if (!fs.locked) {
      doc["success"] = false;
      doc["error"] = "Filesystem busy";
      String out; serializeJson(doc, out);
      request->send(503, "application/json", out);
      return;
    }

    int deletedFiles = 0;
    File root = LittleFS.open("/logs");
    if (root && root.isDirectory()) {
      File f = root.openNextFile();
      while (f) {
        String fn = f.name();
        f.close();
        if (fn.endsWith(".csv")) {
          if (!fn.startsWith("/")) fn = "/logs/" + fn;
          if (LittleFS.remove(fn)) deletedFiles++;
        }
        f = root.openNextFile();
      }
      root.close();
    }

    doc["success"] = true;
    doc["deleted_files"] = deletedFiles;
    String out; serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // /api/lfs/export?sensor=0..3 -> export all available LittleFS data for one sensor
  // Output: timestamp,t_c,h_rh
  server.on("/api/lfs/export", HTTP_GET, [](AsyncWebServerRequest* request) {
    if (!request->hasParam("sensor")) {
      request->send(400, "text/plain", "Missing sensor parameter");
      return;
    }
    int sensor = request->getParam("sensor")->value().toInt();
    if (sensor < 0 || sensor > 3) {
      request->send(400, "text/plain", "Invalid sensor");
      return;
    }

    // Serialize FS access for the duration of export.
    FsGuard fs(5000);
    if (!fs.locked) {
      request->send(503, "text/plain", "Filesystem busy, try again");
      return;
    }

    AsyncResponseStream* response = request->beginResponseStream("text/csv");
    response->addHeader("Cache-Control", "no-store");
    String fn = config.device_id + "_LFS_ALL_sensor" + String(sensor) + ".csv";
    response->addHeader("Content-Disposition", "attachment; filename=\"" + fn + "\"");
    response->print("timestamp,t_c,h_rh\n");

    // Collect /logs/*.csv filenames and sort them (YYYY-MM) for stable chronological export.
    String names[48];
    int nNames = 0;
    File root = LittleFS.open("/logs");
    if (root && root.isDirectory()) {
      File f = root.openNextFile();
      while (f && nNames < 48) {
        String nm = f.name();
        if (nm.endsWith(".csv")) {
          if (!nm.startsWith("/")) nm = "/logs/" + nm;
          names[nNames++] = nm;
        }
        f = root.openNextFile();
      }
      root.close();
    }
    // Simple sort
    for (int i = 0; i < nNames; i++) {
      for (int j = i + 1; j < nNames; j++) {
        if (names[j] < names[i]) {
          String tmp = names[i];
          names[i] = names[j];
          names[j] = tmp;
        }
      }
    }

    for (int fi = 0; fi < nNames; fi++) {
      File f = LittleFS.open(names[fi], "r");
      if (!f) continue;
      f.seek(0);
      String magic = f.readStringUntil('\n');
      f.seek(0);

      size_t dataStart = 0;
      unsigned long offset = 0;
      unsigned long count = 0;
      size_t slots = 0;
      size_t recordLen = LFS_RING_RECORD_LEN;

      if (magic == "RB2" && lfsRingReadHeaderSparse(f, dataStart, offset, count, slots, recordLen)) {
        // Sparse records: ts,sensor_id,t,h
        unsigned long startIndex = (offset + slots - count) % slots;
        for (unsigned long i = 0; i < count; i++) {
          unsigned long idx = (startIndex + i) % slots;
          size_t pos = dataStart + (idx * recordLen);
          f.seek(pos);
          char buf[LFS_RING_RECORD_LEN_SPARSE + 1];
          size_t nread = f.readBytes(buf, recordLen);
          buf[nread] = '\0';
          String line = lfsRingTrimRecord(buf, nread);
          if (line.length() < 20) continue;
          int c1 = line.indexOf(',');
          int c2 = line.indexOf(',', c1 + 1);
          int c3 = line.indexOf(',', c2 + 1);
          if (c1 <= 0 || c2 <= 0 || c3 <= 0) continue;
          int sid = line.substring(c1 + 1, c2).toInt();
          if (sid != sensor) continue;
          String ts = line.substring(0, c1); ts.trim();
          String tt = line.substring(c2 + 1, c3); tt.trim();
          String hh = line.substring(c3 + 1); hh.trim();
          response->print(ts); response->print(','); response->print(tt); response->print(','); response->print(hh); response->print('\n');
        }
      } else if (magic == "RB1" && lfsRingReadHeader(f, dataStart, offset, count, slots, recordLen)) {
        // Extended records: ts,device_id,t0,h0,t1,h1,t2,h2,t3,h3
        const int tCol = 2 + sensor * 2;
        const int hCol = 3 + sensor * 2;
        unsigned long startIndex = (offset + slots - count) % slots;
        for (unsigned long i = 0; i < count; i++) {
          unsigned long idx = (startIndex + i) % slots;
          size_t pos = dataStart + (idx * recordLen);
          f.seek(pos);
          char buf[LFS_RING_RECORD_LEN_EXT + 1];
          size_t nread = f.readBytes(buf, recordLen);
          buf[nread] = '\0';
          String line = lfsRingTrimRecord(buf, nread);
          if (line.length() < 20) continue;

          int col = 0;
          int start = 0;
          int tsS = -1, tsE = -1;
          int tS = -1, tE = -1;
          int hS = -1, hE = -1;
          for (int k = 0; k <= (int)line.length(); k++) {
            bool atEnd = (k == (int)line.length());
            char c = atEnd ? ',' : line.charAt(k);
            if (c == ',') {
              int end = k;
              if (col == 0) { tsS = start; tsE = end; }
              if (col == tCol) { tS = start; tE = end; }
              if (col == hCol) { hS = start; hE = end; }
              col++;
              start = k + 1;
              if (col > hCol) break;
            }
          }
          if (tsS >= 0 && tS >= 0 && hS >= 0) {
            String ts = line.substring(tsS, tsE); ts.trim();
            String tt = line.substring(tS, tE); tt.trim();
            String hh = line.substring(hS, hE); hh.trim();
            if (ts.length() > 0) {
              response->print(ts); response->print(','); response->print(tt); response->print(','); response->print(hh); response->print('\n');
            }
          }
        }
      }
      f.close();
    }

    request->send(response);
  });

  // /api/test-sd
  server.on("/api/test-sd", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(512);
    if (!sdPresent) {
      doc["success"] = false;
      doc["error"] = "SD card not detected";
      String out; serializeJson(doc, out);
      request->send(200, "application/json", out);
      return;
    }

    String testFile = "/sd_test.txt";
    String testData = "FireBeetle2 SD Test - " + getISOTimestamp();

    File f = SD.open(testFile, FILE_WRITE);
    if (!f) {
      doc["success"] = false;
      doc["error"] = "Failed to open file for writing";
      String out; serializeJson(doc, out);
      request->send(200, "application/json", out);
      return;
    }
    f.println(testData);
    f.close();

    f = SD.open(testFile, FILE_READ);
    if (!f) {
      doc["success"] = false;
      doc["error"] = "Failed to open file for reading";
      String out; serializeJson(doc, out);
      request->send(200, "application/json", out);
      return;
    }
    String readBack = f.readStringUntil('\n');
    f.close();
    SD.remove(testFile);

    if (readBack.startsWith("FireBeetle2 SD Test")) {
      doc["success"] = true;
      doc["message"] = "SD card read/write test passed";
    } else {
      doc["success"] = false;
      doc["error"] = "Data verification failed";
    }

    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // ---- SD bulk download APIs ----

  // /api/sd/files  -> list SD CSV log files (typically /logs/YYYY-MM.csv)
  server.on("/api/sd/files", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(4096);
    doc["present"] = sdPresent;
    JsonArray files = doc.createNestedArray("files");

    if (sdPresent) {
      File root = SD.open("/logs");
      if (root && root.isDirectory()) {
        File f = root.openNextFile();
        while (f) {
          String name = f.name();
          if (name.endsWith(".csv")) {
            JsonObject fo = files.createNestedObject();
            fo["name"] = name;
            fo["size"] = (unsigned long)f.size();
          }
          f = root.openNextFile();
        }
        root.close();
      }
    }

    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // /api/sd/download?name=/logs/2026-02.csv  -> download one whole SD file directly
  server.on("/api/sd/download", HTTP_GET, [](AsyncWebServerRequest* request) {
    if (!sdPresent) {
      request->send(404, "text/plain", "SD card not detected");
      return;
    }
    if (!request->hasParam("name")) {
      request->send(400, "text/plain", "Missing name parameter");
      return;
    }

    String name = request->getParam("name")->value();
    // Basic path hardening: only allow /logs/*.csv
    if (!name.startsWith("/logs/") || !name.endsWith(".csv") || name.indexOf("..") >= 0) {
      request->send(400, "text/plain", "Invalid file name");
      return;
    }

    if (!SD.exists(name)) {
      request->send(404, "text/plain", "File not found");
      return;
    }

    // Stream file from SD. `true` => download (Content-Disposition: attachment)
    AsyncWebServerResponse* resp = request->beginResponse(SD, name, "text/csv", true);
    // Ensure browser can see filename; ESPAsyncWebServer will set Content-Disposition but be explicit.
    int slash = name.lastIndexOf('/');
    String base = (slash >= 0) ? name.substring(slash + 1) : name;
    resp->addHeader("Content-Disposition", "attachment; filename=\"" + base + "\"");
    resp->addHeader("Cache-Control", "no-store");
    request->send(resp);
  });

  // /api/sd/export?sensor=0..3  -> export *all* SD data for one sensor into a single CSV
  // Output: timestamp,t_c,h_rh
  server.on("/api/sd/export", HTTP_GET, [](AsyncWebServerRequest* request) {
    if (!sdPresent) {
      request->send(404, "text/plain", "SD card not detected");
      return;
    }
    if (!request->hasParam("sensor")) {
      request->send(400, "text/plain", "Missing sensor parameter");
      return;
    }
    int sensor = request->getParam("sensor")->value().toInt();
    if (sensor < 0 || sensor > 3) {
      request->send(400, "text/plain", "Invalid sensor");
      return;
    }

    AsyncResponseStream* response = request->beginResponseStream("text/csv");
    response->addHeader("Cache-Control", "no-store");
    String fn = config.device_id + "_SD_ALL_sensor" + String(sensor) + ".csv";
    response->addHeader("Content-Disposition", "attachment; filename=\"" + fn + "\"");

    // Header for per-sensor export
    response->print("timestamp,t_c,h_rh\n");

    File root = SD.open("/logs");
    if (!root || !root.isDirectory()) {
      if (root) root.close();
      request->send(response);
      return;
    }

    // SD CSV format: timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh
    const int tCol = 2 + sensor * 2;
    const int hCol = 3 + sensor * 2;

    File f = root.openNextFile();
    while (f) {
      String name = f.name();
      if (name.endsWith(".csv")) {
        // Stream file line-by-line
        while (f.available()) {
          String line = f.readStringUntil('\n');
          line.trim();
          if (line.length() == 0) continue;
          if (line.startsWith("timestamp")) continue; // skip header

          // Extract columns 0, tCol, hCol without allocating an array
          int col = 0;
          int start = 0;
          int want0s = -1, want0e = -1;
          int wantTs = -1, wantTe = -1;
          int wantHs = -1, wantHe = -1;

          for (int i = 0; i <= (int)line.length(); i++) {
            bool atEnd = (i == (int)line.length());
            char c = atEnd ? ',' : line.charAt(i);
            if (c == ',') {
              int end = i;
              if (col == 0) { want0s = start; want0e = end; }
              if (col == tCol) { wantTs = start; wantTe = end; }
              if (col == hCol) { wantHs = start; wantHe = end; }
              col++;
              start = i + 1;
              // Early exit once we captured humidity column
              if (col > hCol) break;
            }
          }

          if (want0s >= 0 && wantTs >= 0 && wantHs >= 0) {
            String ts = line.substring(want0s, want0e);
            String tt = line.substring(wantTs, wantTe);
            String hh = line.substring(wantHs, wantHe);
            ts.trim(); tt.trim(); hh.trim();
            if (ts.length() > 0) {
              response->print(ts);
              response->print(',');
              response->print(tt);
              response->print(',');
              response->print(hh);
              response->print('\n');
            }
          }
        }
      }
      f = root.openNextFile();
    }
    root.close();

    request->send(response);
  });

  // /api/sd/delete?name=/logs/2026-02.csv -> delete one SD log file
  server.on("/api/sd/delete", HTTP_GET, [](AsyncWebServerRequest* request) {
    DynamicJsonDocument doc(512);
    doc["present"] = sdPresent;

    if (!sdPresent) {
      doc["success"] = false;
      doc["error"] = "SD card not detected";
      String out; serializeJson(doc, out);
      request->send(404, "application/json", out);
      return;
    }
    if (!request->hasParam("name")) {
      doc["success"] = false;
      doc["error"] = "Missing name";
      String out; serializeJson(doc, out);
      request->send(400, "application/json", out);
      return;
    }

    String name = request->getParam("name")->value();
    if (!name.startsWith("/logs/") || !name.endsWith(".csv") || name.indexOf("..") >= 0) {
      doc["success"] = false;
      doc["error"] = "Invalid file name";
      String out; serializeJson(doc, out);
      request->send(400, "application/json", out);
      return;
    }

    if (!SD.exists(name)) {
      doc["success"] = false;
      doc["error"] = "File not found";
      String out; serializeJson(doc, out);
      request->send(404, "application/json", out);
      return;
    }

    bool ok = SD.remove(name);
    doc["success"] = ok;
    doc["name"] = name;
    if (!ok) doc["error"] = "Delete failed";

    String out; serializeJson(doc, out);
    request->send(ok ? 200 : 500, "application/json", out);
  });

  // /api/recent?n=50  (fast path: serve from in-RAM ring — zero LittleFS access)
  server.on("/api/recent", HTTP_GET, [](AsyncWebServerRequest* request) {
    // #region agent log
    unsigned long _t0 = millis();
    // #endregion

    int n = 50;
    if (request->hasParam("n")) {
      n = request->getParam("n")->value().toInt();
    }
    if (n <= 0) n = 1;
    if (n > RAM_RING_SIZE) n = RAM_RING_SIZE;

    // Snapshot ring state under spinlock (fast, < 1 µs)
    int head, count;
    portENTER_CRITICAL(&g_ramSpinlock);
    head = g_ramHead;
    count = g_ramCount;
    portEXIT_CRITICAL(&g_ramSpinlock);

    int take = (n < count) ? n : count;

    AsyncResponseStream* response = request->beginResponseStream("application/json");
    response->addHeader("Connection", "close");
    response->print("{\"points\":[");
    int pointCount = 0;

    for (int i = 0; i < take; i++) {
      int idx = (head - 1 - i + RAM_RING_SIZE) % RAM_RING_SIZE;
      const RamSample& s = g_ramRing[idx];
      if (s.sensor < 0 || s.sensor > 3) continue;
      if (s.ts[0] == '\0') continue;

      if (pointCount > 0) response->print(',');
      response->print("[\"");
      response->print(s.ts);
      response->print("\"");
      for (int sid = 0; sid < 4; sid++) {
        if (sid == s.sensor) {
          char tmp[24];
          snprintf(tmp, sizeof(tmp), ",%.2f,%.2f", (double)s.t, (double)s.h);
          response->print(tmp);
        } else {
          response->print(",null,null");
        }
      }
      response->print(']');
      pointCount++;
    }

    response->print("]");
    response->print(",\"count\":");
    response->print(pointCount);
    response->print(",\"order\":\"desc\"}");
    // #region agent log
    Serial.printf("[PERF] /api/recent done: %d pts in %lums (RAM)\n", pointCount, millis() - _t0);
    // #endregion
    request->send(response);
  });

  // /api/data (stream JSON points)
  server.on("/api/data", HTTP_GET, [](AsyncWebServerRequest* request) {
    // #region agent log
    unsigned long _t0 = millis();
    // #endregion
    // Serialize LittleFS access (this handler scans files and can otherwise wedge async_tcp).
    FsGuard fs(200);
    if (!fs.locked) {
      // #region agent log
      Serial.printf("[PERF] /api/data fs_busy after %lums\n", millis() - _t0);
      // #endregion
      request->send(503, "application/json", "{\"points\":[],\"count\":0,\"order\":\"desc\",\"error\":\"fs_busy\"}");
      return;
    }

    if (!request->hasParam("from") || !request->hasParam("to")) {
      request->send(400, "application/json", "{\"error\":\"Missing from/to parameters\"}");
      return;
    }
    String fromStr = request->getParam("from")->value();
    String toStr = request->getParam("to")->value();
    time_t fromTime = parseISO8601(fromStr);
    time_t toTime = parseISO8601(toStr);
    if (fromTime == 0 || toTime == 0) {
      request->send(400, "application/json", "{\"error\":\"Invalid timestamp format\"}");
      return;
    }

    // Cap points so TCP transmission completes in well under 30s; large payloads block
    // async_tcp in send path and trigger task WDT (see [WDT-DBG] evidence).
    const int MAX_POINTS = 500;
    int pointCount = 0;

    AsyncResponseStream* response = request->beginResponseStream("application/json");
    response->addHeader("Connection", "close");
    response->print("{\"points\":[");
    bool firstPoint = true;

    File dataRoot = LittleFS.open("/logs");
    if (dataRoot && dataRoot.isDirectory()) {
      File file = dataRoot.openNextFile();
      while (file && pointCount < MAX_POINTS) {
        String name = file.name();
        if (name.endsWith(".csv")) {
          file.seek(0);
          String magic = file.readStringUntil('\n');
          file.seek(0);

          size_t dataStart = 0;
          unsigned long offset = 0;
          unsigned long count = 0;
          size_t slots = 0;
          size_t recordLen = LFS_RING_RECORD_LEN;

          if (magic == "RB2" && lfsRingReadHeaderSparse(file, dataStart, offset, count, slots, recordLen)) {
            // Iterate newest->oldest to return recent data fast (helps avoid async_tcp WDT on large ranges)
            // Note: points will be in descending timestamp order; UI should sort if it needs ascending.
            unsigned long startIndex = (offset + slots - 1) % slots;
            for (unsigned long i = 0; i < count && pointCount < MAX_POINTS; i++) {
              unsigned long idx = (startIndex + slots - i) % slots;
              size_t pos = dataStart + (idx * recordLen);
              file.seek(pos);
              char buf[LFS_RING_RECORD_LEN_SPARSE + 1];
              size_t nread = file.readBytes(buf, recordLen);
              buf[nread] = '\0';
              String line = lfsRingTrimRecord(buf, nread);
              if (line.length() < 28) continue;
              int c1 = line.indexOf(',');
              int c2 = line.indexOf(',', c1 + 1);
              int c3 = line.indexOf(',', c2 + 1);
              if (c1 <= 0 || c2 <= 0 || c3 <= 0) continue;
              String tsStr = line.substring(0, c1);
              tsStr.trim();
              time_t tsTime = parseISO8601(tsStr);
              // We iterate newest->oldest. Once we go older than `from`, we can stop scanning this file.
              if (tsTime != 0 && tsTime < fromTime) break;
              if (tsTime == 0 || tsTime > toTime) continue;
              int sensorId = line.substring(c1 + 1, c2).toInt();
              if (sensorId < 0 || sensorId > 3) continue;
              String tStr = line.substring(c2 + 1, c3);
              String hStr = line.substring(c3 + 1);

              if (!firstPoint) response->print(',');
              firstPoint = false;

              response->print("[\""); response->print(tsStr); response->print("\"");
              for (int s = 0; s < 4; s++) {
                if (s == sensorId) {
                  response->print(','); response->print(tStr);
                  response->print(','); response->print(hStr);
                } else {
                  response->print(",null,null");
                }
              }
              response->print(']');
              pointCount++;
            }
          } else if (magic == "RB1" && lfsRingReadHeader(file, dataStart, offset, count, slots, recordLen)) {
            // Iterate newest->oldest to return recent data fast (helps avoid async_tcp WDT on large ranges)
            // Note: points will be in descending timestamp order; UI should sort if it needs ascending.
            unsigned long startIndex = (offset + slots - 1) % slots;
            for (unsigned long i = 0; i < count && pointCount < MAX_POINTS; i++) {
              unsigned long idx = (startIndex + slots - i) % slots;
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
              // We iterate newest->oldest. Once we go older than `from`, we can stop scanning this file.
              if (tsTime != 0 && tsTime < fromTime) break;
              if (tsTime == 0 || tsTime > toTime) continue;
              String rest = line.substring(comma2 + 1);

              if (!firstPoint) response->print(',');
              firstPoint = false;
              response->print("[\""); response->print(tsStr); response->print("\",");
              response->print(rest);
              response->print(']');
              pointCount++;
            }
          }
        }
        file = dataRoot.openNextFile();
      }
      dataRoot.close();
    }

    response->print("]");
    response->print(",\"count\":");
    response->print(pointCount);
    response->print(",\"order\":\"desc\"");
    if (pointCount >= MAX_POINTS) {
      response->print(",\"warning\":\"Result truncated to 500 points\"");
    }
    response->print("}");
    // #region agent log
    Serial.printf("[PERF] /api/data done: %d pts in %lums\n", pointCount, millis() - _t0);
    // #endregion
    request->send(response);
  });

  // /api/download (CSV). Optional offset/limit for chunked fetch to avoid WDT on large sends.
  static const int DOWNLOAD_CHUNK_LINES = 500;
  server.on("/api/download", HTTP_GET, [](AsyncWebServerRequest* request) {
    if (!request->hasParam("from") || !request->hasParam("to")) {
      request->send(400, "text/plain", "Missing from/to parameters");
      return;
    }

    FsGuard fs(500);
    if (!fs.locked) {
      request->send(503, "text/plain", "Filesystem busy, try again");
      return;
    }

    // #region agent log
    unsigned long _dlStart = millis();
    // #endregion

    String fromStr = request->getParam("from")->value();
    String toStr = request->getParam("to")->value();
    time_t fromTime = parseISO8601(fromStr);
    time_t toTime = parseISO8601(toStr);
    if (fromTime == 0 || toTime == 0) {
      request->send(400, "text/plain", "Invalid timestamp format");
      return;
    }

    int chunkOffset = 0;
    int chunkLimit = 0;
    if (request->hasParam("offset")) chunkOffset = request->getParam("offset")->value().toInt();
    if (request->hasParam("limit")) chunkLimit = request->getParam("limit")->value().toInt();
    if (chunkOffset < 0) chunkOffset = 0;
    if (chunkLimit < 0) chunkLimit = 0;
    bool chunked = (chunkLimit > 0);
    // Cap single response at DOWNLOAD_CHUNK_LINES to avoid async_tcp WDT (e.g. old client or direct link).
    if (!chunked) {
      chunkLimit = DOWNLOAD_CHUNK_LINES;
      chunked = true;
      chunkOffset = 0;
    }

    String filename = config.device_id + "_" + fromStr.substring(0, 10) + "_" + toStr.substring(0, 10) + ".csv";

    AsyncResponseStream* response = request->beginResponseStream("text/csv");
    if (!chunked) {
      response->addHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    }

    // For chunked downloads, interpret `offset` as: "number of matching data rows to skip"
    // (after applying the from/to filter). This makes the client stateless and resilient.
    unsigned long matchIndex = 0;
    unsigned long outCount = 0;
    bool hasMore = false;

    // Always attach a filename on the first chunk (helps when user hits the endpoint directly).
    if (chunkOffset == 0) {
      response->addHeader("Content-Disposition", "attachment; filename=\"" + filename + "\"");
    }

    // If SD present, serve SD monthly files; else LittleFS ring (RB2)
    if (sdPresent) {
      if (chunkOffset == 0) {
        response->print("timestamp,device_id,t0_c,h0_rh,t1_c,h1_rh,t2_c,h2_rh,t3_c,h3_rh\n");
      }
      int startYear = 0, startMonth = 0;
      int endYear = 0, endMonth = 0;
      normalizeMonthStart(fromTime, startYear, startMonth);
      normalizeMonthStart(toTime, endYear, endMonth);
      int year = startYear;
      int month = startMonth;
      while (!hasMore && (year < endYear || (year == endYear && month <= endMonth))) {
        String fileName = getLogFilenameForMonth(year, month);
        File f = SD.open(fileName, FILE_READ);
        if (f) {
          String header = f.readStringUntil('\n');
          if (!header.startsWith("timestamp")) f.seek(0);
          while (f.available() && !hasMore) {
            String line = f.readStringUntil('\n');
            if (line.length() == 0) break;
            int comma1 = line.indexOf(',');
            if (comma1 <= 0) continue;
            String tsStr = line.substring(0, comma1);
            time_t tsTime = parseISO8601(tsStr);
            if (tsTime < fromTime || tsTime > toTime) continue;

            // This line matches the range.
            if (matchIndex < (unsigned long)chunkOffset) {
              matchIndex++;
              continue;
            }

            if (outCount < (unsigned long)chunkLimit) {
              response->print(line);
              response->print('\n');
              outCount++;
              matchIndex++;
            } else {
              // We already filled this chunk; signal that more matching data exists.
              hasMore = true;
              break;
            }
          }
          f.close();
        }
        month++;
        if (month > 12) { month = 1; year++; }
      }
    } else {
      // LittleFS
      bool headerSent = false;
      File dlRoot = LittleFS.open("/logs");
      if (dlRoot && dlRoot.isDirectory()) {
        File f = dlRoot.openNextFile();
        while (f && !hasMore) {
          String name = f.name();
          if (name.endsWith(".csv")) {
            f.seek(0);
            String magic = f.readStringUntil('\n');
            f.seek(0);
            size_t dataStart = 0;
            unsigned long offset = 0;
            unsigned long count = 0;
            size_t slots = 0;
            size_t recordLen = LFS_RING_RECORD_LEN;

            if (magic == "RB2" && lfsRingReadHeaderSparse(f, dataStart, offset, count, slots, recordLen)) {
              if (!headerSent && chunkOffset == 0) {
                response->print("timestamp,sensor_id,t_c,h_rh\n");
                headerSent = true;
              }

              unsigned long startIndex = (offset + slots - count) % slots;
              for (unsigned long i = 0; i < count && !hasMore; i++) {
                unsigned long idx = (startIndex + i) % slots;
                size_t pos = dataStart + (idx * recordLen);
                f.seek(pos);
                char buf[LFS_RING_RECORD_LEN_SPARSE + 1];
                size_t nread = f.readBytes(buf, recordLen);
                buf[nread] = '\0';
                String line = lfsRingTrimRecord(buf, nread);
                if (line.length() < 20) continue;
                int c1 = line.indexOf(',');
                if (c1 <= 0) continue;
                String tsStr = line.substring(0, c1);
                time_t tsTime = parseISO8601(tsStr);
                if (tsTime < fromTime || tsTime > toTime) continue;

                // This line matches the range.
                if (matchIndex < (unsigned long)chunkOffset) {
                  matchIndex++;
                  continue;
                }

                if (outCount < (unsigned long)chunkLimit) {
                  response->print(line);
                  response->print('\n');
                  outCount++;
                  matchIndex++;
                } else {
                  hasMore = true;
                  break;
                }
              }
            }
          }
          f = dlRoot.openNextFile();
        }
        dlRoot.close();
      }
      if (chunkOffset == 0 && !headerSent) {
        // Always return a header on the first chunk even if no data.
        response->print("timestamp,sensor_id,t_c,h_rh\n");
      }
    }

    if (chunked) {
      response->addHeader("X-More", hasMore ? "1" : "0");
      response->addHeader("Access-Control-Expose-Headers", "X-More");
    }

    // #region agent log
    Serial.printf("[PERF] /api/download done in %lums (chunked=%d out=%lu)\n", millis() - _dlStart, chunked ? 1 : 0, chunked ? outCount : 0);
    // #endregion
    request->send(response);
  });

  // /api/prune (Async)
  server.on("/api/prune", HTTP_GET, [](AsyncWebServerRequest* request) {
    if (!request->hasParam("days")) {
      request->send(400, "application/json", "{\"error\":\"Missing days parameter\"}");
      return;
    }

    int days = request->getParam("days")->value().toInt();
    if (days <= 0) {
      request->send(400, "application/json", "{\"error\":\"Invalid days value\"}");
      return;
    }

    time_t now = time(nullptr);
    if (now < 946684800) {
      request->send(400, "application/json", "{\"error\":\"Time not set\"}");
      return;
    }

    time_t cutoff = now - (time_t)days * 86400;

    int deletedSamples = 0;
    int keptSamples = 0;
    int deletedFiles = 0;
    size_t freedBytes = 0;

    File root = LittleFS.open("/logs");
    if (!root || !root.isDirectory()) {
      if (root) root.close();
      DynamicJsonDocument doc(256);
      doc["deleted_samples"] = 0;
      doc["kept_samples"] = 0;
      doc["deleted_files"] = 0;
      doc["freed_bytes"] = 0;
      String out; serializeJson(doc, out);
      request->send(200, "application/json", out);
      return;
    }

    File file = root.openNextFile();
    while (file) {
      String fileName = file.name();
      if (!fileName.endsWith(".csv")) {
        file = root.openNextFile();
        continue;
      }
      if (!fileName.startsWith("/")) fileName = "/logs/" + fileName;

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

        if (isSparse) lfsRingWriteHeaderSparse(tmp, 0, 0);
        else if (recordLen >= LFS_RING_RECORD_LEN_EXT) lfsRingWriteHeaderExt(tmp, 0, 0);
        else lfsRingWriteHeader(tmp, 0, 0);

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
          if (line.length() == 0) continue;

          int comma1 = line.indexOf(',');
          if (comma1 <= 0) { deletedSamples++; continue; }
          String tsStr = line.substring(0, comma1);
          time_t tsTime = parseISO8601(tsStr);

          if (tsTime != 0 && tsTime >= cutoff) {
            String record = line;
            if (record.endsWith("\n")) record.remove(record.length() - 1);
            size_t maxRec = recordLen - 1;
            if (record.length() > maxRec) record = record.substring(0, maxRec);
            while (record.length() < maxRec) record += " ";
            record += "\n";

            size_t wpos = tmpDataStart + (keptInFile * recordLen);
            tmp.seek(wpos);
            size_t written = tmp.print(record);
            if (written != recordLen) break;

            keptSamples++;
            keptInFile++;
          } else {
            deletedSamples++;
          }
        }

        unsigned long newCount = keptInFile;
        unsigned long newOffset = (tmpSlots > 0) ? (newCount % tmpSlots) : 0;
        if (isSparse) lfsRingWriteHeaderSparse(tmp, newOffset, newCount);
        else if (recordLen >= LFS_RING_RECORD_LEN_EXT) lfsRingWriteHeaderExt(tmp, newOffset, newCount);
        else lfsRingWriteHeader(tmp, newOffset, newCount);
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
          if (tmpRead) { newSize = tmpRead.size(); tmpRead.close(); }
          LittleFS.remove(fileName);
          LittleFS.rename(tmpName, fileName);
          if (originalSize > newSize) freedBytes += (originalSize - newSize);
        }

        file = root.openNextFile();
        continue;
      }

      // Legacy plain CSV prune (best-effort)
      File tmp = LittleFS.open(tmpName, "w");
      if (!tmp) {
        file.close();
        file = root.openNextFile();
        continue;
      }

      String line = file.readStringUntil('\n');
      if (line.startsWith("timestamp")) tmp.print(line + "\n");
      else file.seek(0);

      int fileKept = 0;
      while (file.available()) {
        line = file.readStringUntil('\n');
        if (line.length() == 0) break;
        int comma1 = line.indexOf(',');
        if (comma1 <= 0) { deletedSamples++; continue; }
        String tsStr = line.substring(0, comma1);
        time_t tsTime = parseISO8601(tsStr);
        if (tsTime != 0 && tsTime >= cutoff) {
          tmp.print(line + "\n");
          keptSamples++;
          fileKept++;
        } else {
          deletedSamples++;
        }
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
        if (tmpRead) { newSize = tmpRead.size(); tmpRead.close(); }
        LittleFS.remove(fileName);
        LittleFS.rename(tmpName, fileName);
        if (originalSize > newSize) freedBytes += (originalSize - newSize);
      }

      file = root.openNextFile();
    }

    root.close();

    DynamicJsonDocument doc(512);
    doc["deleted_samples"] = deletedSamples;
    doc["kept_samples"] = keptSamples;
    doc["deleted_files"] = deletedFiles;
    doc["freed_bytes"] = freedBytes;

    String out;
    serializeJson(doc, out);
    request->send(200, "application/json", out);
  });

  // Serve static assets from LittleFS — MUST be after all server.on() routes
  // so that /api/* and other explicit handlers take priority over the catch-all.
  // Prefer uncompressed files so Safari (and strict clients) can decode; gzip can cause "cannot decode raw data".
  server.serveStatic("/", LittleFS, "/").setDefaultFile("index.html").setCacheControl("max-age=86400").setTryGzipFirst(false);
  
  server.begin();
  Serial.println("Async web server started on port 80");
  Serial.println("Async WebSocket available at ws://192.168.4.1/ws");
  
  // Randomness: use esp_random() directly for tokens/salts (no RNG seeding required)

  Serial.println("\nSetup complete!");
  Serial.println("Connect to: " + config.ap_ssid);
  Serial.println("Password: " + config.ap_password);
  Serial.println("Sample intervals (s): S0=" + String(config.sample_period_sensor[0]) + " S1=" + String(config.sample_period_sensor[1]) + " S2=" + String(config.sample_period_sensor[2]) + " S3=" + String(config.sample_period_sensor[3]));
  Serial.println("Current time: " + getISOTimestamp());
  Serial.println("Time set: " + String(config.time_set ? "YES" : "NO"));
  Serial.println("\nDebug: http://192.168.4.1/api/status");
  // #region agent log
  Serial.printf("[DBG] setup done: ssid_len=%u ssid='%s' apLastCheckMs=%lu\n", (unsigned)config.ap_ssid.length(), config.ap_ssid.c_str(), apLastCheckMs);
  // #endregion
}

void loop() {
  // #region agent log
  static bool firstLoopDump = true;
  if (firstLoopDump && millis() > 3000) {
    firstLoopDump = false;
    Serial.println("=== POST-SETUP DUMP (delayed 3s for USB CDC) ===");
    Serial.printf("[DBG] ssid_len=%u ssid='%s'\n", (unsigned)config.ap_ssid.length(), config.ap_ssid.c_str());
    Serial.printf("[DBG] device_id='%s'\n", config.device_id.c_str());
    Serial.printf("[DBG] wifi_mode=%d ap_ip=%s\n", (int)WiFi.getMode(), WiFi.softAPIP().toString().c_str());
    Serial.printf("[DBG] heap_free=%u\n", (unsigned)ESP.getFreeHeap());
  }
  // #endregion
  unsigned long now = millis();
  
  // Periodic AP health check + restart with backoff if needed
  if (now - apLastCheckMs >= 5000) {
    apLastCheckMs = now;
    // #region agent log
    int wifiMode = (int)WiFi.getMode();
    int staCnt = WiFi.softAPgetStationNum();
    Serial.printf("[DBG] AP health: mode=%d staCnt=%d ssid_len=%u ssid='%s'\n", wifiMode, staCnt, (unsigned)config.ap_ssid.length(), config.ap_ssid.c_str());
    // #endregion
    if (WiFi.getMode() != WIFI_AP || WiFi.softAPgetStationNum() < 0) {
      // #region agent log
      Serial.printf("[DBG] AP health TRIGGERED: mode=%d staCnt=%d\n", wifiMode, staCnt);
      // #endregion
      if (apRestartAfterMs == 0) apRestartAfterMs = now + apBackoffMs;
    }
  }
  if (apRestartAfterMs != 0 && (long)(now - apRestartAfterMs) >= 0) {
    if (config.ap_ssid.length() == 0) {
      Serial.println("[WiFi] AP restart skipped: SSID is empty");
      apRestartAfterMs = 0;
    } else {
      Serial.println("[WiFi] Restarting AP (best-effort)");
      WiFi.softAPdisconnect(true);
      delay(50);
      WiFi.mode(WIFI_AP);
      WiFi.softAPConfig(apIP, apIP, IPAddress(255, 255, 255, 0));
      delay(50);
      WiFi.softAP(config.ap_ssid.c_str(), config.ap_password.c_str(), 6, false, 4);
      apBackoffMs = (apBackoffMs < 60000UL) ? (apBackoffMs * 2) : 60000UL;
      apRestartAfterMs = 0;
    }
  }
  
  // LED tick (non-blocking)
  ledTick(now);

  // Periodically clean up dead WebSocket clients to avoid queue buildup.
  static unsigned long wsCleanupMs = 0;
  if (now - wsCleanupMs >= 5000) {
    wsCleanupMs = now;
    ws.cleanupClients();
  }

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

      // Persist (LittleFS sparse ring) per sensor read.
      // Skip LFS writes when the clock is not set — bogus 1970 timestamps waste space.
      bool lfsSuccess = false;
      time_t epochNow = time(nullptr);
      bool canLfs = (epochNow >= 946684800) && (LittleFS.exists("/logs") || LittleFS.mkdir("/logs"));
      String sparsePath;
      if (canLfs) sparsePath = getSparseLogPath();

      for (int i = 0; i < NUM_SENSORS; i++) {
        if (!readThisLoop[i]) continue;
        if (!isfinite(lastT[i]) || !isfinite(lastH[i])) continue;
        if (epochNow >= 946684800) {
          ramRingPush(timestamp.c_str(), i, lastT[i], lastH[i]);
        }
      }
      if (canLfs) {
        // #region agent log
        unsigned long _wt0 = millis();
        // #endregion
        int numWritten = 0;
        if (writeLfsRingRecordsSparseBatch(sparsePath, timestamp, readThisLoop, lastT, lastH, &numWritten)) {
          lfsSuccess = true;
        }
        // #region agent log
        unsigned long _wdt = millis() - _wt0;
        if (_wdt > 100) Serial.printf("[PERF] writeSparseBatch %d sensors took %lums\n", numWritten, _wdt);
        // #endregion
      }

      // Push live samples to WebSocket clients (batched + throttled)
      // Motivation: browsers can stall / reconnect; per-sensor spam can overflow WS queues and wedge async_tcp.
      if (ws.count() > 0) {
        static unsigned long lastWsPushMs = 0;
        const unsigned long WS_PUSH_MIN_INTERVAL_MS = 1000;
        if (lastWsPushMs == 0 || (long)(now - lastWsPushMs) >= (long)WS_PUSH_MIN_INTERVAL_MS) {
          bool hon[NUM_SENSORS] = {false, false, false, false};
          for (int i = 0; i < NUM_SENSORS; i++) {
            if (i == 0) hon[i] = sht.isHeaterOn();
            else if (i == 1) hon[i] = sht1.isHeaterOn();
            else hon[i] = heater[i].active;
          }

          char buf[384];
          // Send last-known values for all 4 sensors; "updated" tells UI which sensors were read in this cycle.
          int n = snprintf(buf, sizeof(buf),
                           "{\"type\":\"sample4\",\"ts\":\"%s\",\"t\":[%.2f,%.2f,%.2f,%.2f],\"h\":[%.2f,%.2f,%.2f,%.2f],\"heater\":[%s,%s,%s,%s],\"updated\":[%s,%s,%s,%s]}",
                           timestamp.c_str(),
                           (double)lastT[0], (double)lastT[1], (double)lastT[2], (double)lastT[3],
                           (double)lastH[0], (double)lastH[1], (double)lastH[2], (double)lastH[3],
                           hon[0] ? "true" : "false", hon[1] ? "true" : "false", hon[2] ? "true" : "false", hon[3] ? "true" : "false",
                           readThisLoop[0] ? "true" : "false", readThisLoop[1] ? "true" : "false", readThisLoop[2] ? "true" : "false", readThisLoop[3] ? "true" : "false");
          if (n > 0 && (size_t)n < sizeof(buf)) {
            // #region agent log
            unsigned long _wst0 = millis();
            // #endregion
            ws.textAll(buf);
            // #region agent log
            unsigned long _wsdt = millis() - _wst0;
            if (_wsdt > 50) Serial.printf("[PERF] ws.textAll took %lums clients=%u\n", _wsdt, (unsigned)ws.count());
            // #endregion
          }

          lastWsPushMs = now;
        }
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
