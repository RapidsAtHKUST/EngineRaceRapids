//
// Created by yche on 10/28/18.
//

#ifndef ENGINE_UTIL_H
#define ENGINE_UTIL_H

#include <string>

#include <iomanip>
#include <locale>
#include <sstream>

using namespace std;

template<class T>
std::string FormatWithCommas(T value) {
    std::stringstream ss;
    ss.imbue(std::locale(""));
    ss << std::fixed << value;
    return ss.str();
}

#endif //ENGINE_UTIL_H
