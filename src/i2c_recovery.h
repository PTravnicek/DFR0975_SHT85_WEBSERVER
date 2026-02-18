#pragma once

#include <Arduino.h>
#include <Wire.h>

// Best-effort I2C bus recovery by pulsing SCL and issuing STOP.
// Use when a sensor read fails repeatedly (possible bus stuck low).

void i2cRecoverPins(int sdaPin, int sclPin);
void i2cRecoverWire(TwoWire& wire, int sdaPin, int sclPin);
