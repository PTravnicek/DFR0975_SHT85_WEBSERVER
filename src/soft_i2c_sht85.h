#pragma once

#include <Arduino.h>

// Soft I2C + SHT85 measurement/heater helpers.
// These are used for sensors on buses without hardware I2C.

bool softSht85HeaterOn(int sda, int scl);
bool softSht85HeaterOff(int sda, int scl);

// Returns true if read OK; fills temp/hum.
bool readSHT85Soft(int sda, int scl, float* temp, float* hum);
