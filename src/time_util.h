#pragma once

#include <Arduino.h>
#include <time.h>

// Time helpers (UTC)
String getISOTimestamp();
time_t parseISO8601(const String& iso);

// Log filename helpers (UTC monthly files)
String getLogFilename();
String getLogFilenameForMonth(int year, int month);
void normalizeMonthStart(time_t epoch, int& year, int& month);
