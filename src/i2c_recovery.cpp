#include "i2c_recovery.h"

static void i2cReleaseLine(int pin) {
  pinMode(pin, INPUT_PULLUP);
}

static void i2cDriveLow(int pin) {
  pinMode(pin, OUTPUT);
  digitalWrite(pin, LOW);
}

void i2cRecoverPins(int sdaPin, int sclPin) {
  // Release lines first
  i2cReleaseLine(sdaPin);
  i2cReleaseLine(sclPin);
  delayMicroseconds(5);

  // If SDA stuck low, toggle SCL up to 9 times to advance any slave state machine.
  for (int i = 0; i < 9; i++) {
    if (digitalRead(sdaPin) == HIGH) break;
    i2cDriveLow(sclPin);
    delayMicroseconds(5);
    i2cReleaseLine(sclPin);
    delayMicroseconds(5);
  }

  // Generate a STOP condition: SDA low while SCL high, then release SDA.
  i2cDriveLow(sdaPin);
  delayMicroseconds(5);
  i2cReleaseLine(sclPin);
  delayMicroseconds(5);
  i2cReleaseLine(sdaPin);
  delayMicroseconds(5);
}

void i2cRecoverWire(TwoWire& wire, int sdaPin, int sclPin) {
  // End the driver, recover pins, re-init.
  wire.end();
  delay(5);
  i2cRecoverPins(sdaPin, sclPin);
  delay(5);
  wire.begin(sdaPin, sclPin);
  // Default timeouts help prevent long hangs (supported on Arduino-ESP32)
  #if defined(ESP32)
  wire.setTimeOut(50);
  #endif
}
