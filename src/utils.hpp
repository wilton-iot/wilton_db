/* 
 * File:   utils.hpp
 * Author: alex
 *
 * Created on June 10, 2017, 2:38 PM
 */

#ifndef WILTON_DB_UTILS_HPP
#define	WILTON_DB_UTILS_HPP

#include <cstring>

#include "wilton/wilton.h"

namespace wilton {
namespace db {

inline char* alloc_copy(const std::string& str) {
    char* out = wilton_alloc(static_cast<int> (str.length() + 1));
    if (nullptr != out) {
        std::memcpy(out, str.data(), str.length());
        out[str.length()] = '\0';
    }
    return out;
}

} // namespace
}


#endif	/* WILTON_DB_UTILS_HPP */

