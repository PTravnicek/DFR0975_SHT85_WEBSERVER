#include "time_util.h"

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
  int year = t.tm_year + 1900;
  int month = t.tm_mon + 1;
  int day = t.tm_mday;

  if (year < 1970 || month < 1 || month > 12 || day < 1 || day > 31) return 0;

  int64_t days = 0;
  for (int y = 1970; y < year; y++) days += isLeapYear(y) ? 366 : 365;
  for (int m = 1; m < month; m++) days += daysInMonth(year, m);
  days += (day - 1);

  int64_t seconds = days * 86400LL + (int64_t)t.tm_hour * 3600LL + (int64_t)t.tm_min * 60LL + (int64_t)t.tm_sec;
  if (seconds < 0) return 0;
  return (time_t)seconds;
}

String getISOTimestamp() {
  time_t now = time(nullptr);
  if (now < 946684800) {
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

// Parse ISO8601 timestamp to epoch (UTC). Accepts: YYYY-MM-DDTHH:MM:SSZ
// (We accept missing trailing Z as long as format matches; parseISO8601 uses first 19 chars.)
time_t parseISO8601(const String& iso) {
  if (iso.length() < 19) return 0;
  if (iso.charAt(4) != '-' || iso.charAt(7) != '-' || iso.charAt(10) != 'T' ||
      iso.charAt(13) != ':' || iso.charAt(16) != ':') {
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

void normalizeMonthStart(time_t epoch, int& year, int& month) {
  struct tm* t = gmtime(&epoch);
  year = t->tm_year + 1900;
  month = t->tm_mon + 1;
}
