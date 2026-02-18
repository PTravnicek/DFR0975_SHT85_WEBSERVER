#include "soft_i2c_sht85.h"

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

static inline void softI2CDelay() { delayMicroseconds(3); }

static void softI2CSetSda(int pin, int level) {
  if (level) pinMode(pin, INPUT);
  else { pinMode(pin, OUTPUT); digitalWrite(pin, LOW); }
}

static void softI2CSetScl(int pin, int level) {
  if (level) pinMode(pin, INPUT);
  else { pinMode(pin, OUTPUT); digitalWrite(pin, LOW); }
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

bool softSht85HeaterOn(int sda, int scl) {
  return softSht85WriteCmd(sda, scl, 0x306D);
}

bool softSht85HeaterOff(int sda, int scl) {
  return softSht85WriteCmd(sda, scl, 0x3066);
}

bool readSHT85Soft(int sda, int scl, float* temp, float* hum) {
  const uint8_t addr = 0x44;
  const uint16_t cmd = 0x2416; // FAST
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
  for (int i = 0; i < 6; i++) buf[i] = softI2CReadByte(sda, scl, (i < 5));
  softI2CStop(sda, scl);

  if (buf[2] != sht85SoftCrc8(buf, 2) || buf[5] != sht85SoftCrc8(buf + 3, 2)) {
    return false;
  }
  uint16_t rawT = (uint16_t)buf[0] << 8 | buf[1];
  uint16_t rawH = (uint16_t)buf[3] << 8 | buf[4];
  *temp = rawT * (175.0f / 65535.0f) - 45.0f;
  *hum  = rawH * (100.0f / 65535.0f);
  return true;
}
