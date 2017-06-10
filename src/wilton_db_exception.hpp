/* 
 * File:   wilton_db_exception.hpp
 * Author: alex
 *
 * Created on June 10, 2017, 2:38 PM
 */

#ifndef WILTON_DB_WILTON_DB_EXCEPTION_HPP
#define	WILTON_DB_WILTON_DB_EXCEPTION_HPP

#include <string>

#include "staticlib/support/exception.hpp"

namespace wilton {
namespace db {

/**
 * Module specific exception
 */
class wilton_db_exception : public sl::support::exception {
public:
    /**
     * Default constructor
     */
    wilton_db_exception() = default;

    /**
     * Constructor with message
     * 
     * @param msg error message
     */
    wilton_db_exception(const std::string& msg) :
    sl::support::exception(msg) { }

};

} //namespace
}

#endif	/* WILTON_DB_WILTON_DB_EXCEPTION_HPP */

