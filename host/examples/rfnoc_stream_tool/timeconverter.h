// timeconverter.h
#ifndef DASSCORE_TIME_CONVERTER_H
#define DASSCORE_TIME_CONVERTER_H

#include <string>
#include <ctime>
#include <iomanip>
#include <sstream>


// namespace DASSCore {
    
    class TimeConverter {
    public:
        static time_t ComponentToTimeT(int year, int month, int day, int hour, int minute, int second) {
            struct tm timeinfo = {};
            timeinfo.tm_year = year - 1900;  // Years since 1900
            timeinfo.tm_mon = month - 1;     // Months are 0-11
            timeinfo.tm_mday = day;
            timeinfo.tm_hour = hour;
            timeinfo.tm_min = minute;
            timeinfo.tm_sec = second;
            return mktime(&timeinfo);
        }
        
        static std::string TimeTToString(const std::string& format, time_t time) {
            char buffer[80];
            struct tm* timeinfo = localtime(&time);
            strftime(buffer, sizeof(buffer), format.c_str(), timeinfo);
            return std::string(buffer);
        }
    };
// }

#endif // DASSCORE_TIME_CONVERTER_H