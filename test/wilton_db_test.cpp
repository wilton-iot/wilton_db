/* 
 * File:   wilton_db_test.cpp
 * Author: alex
 *
 * Created on June 12, 2017, 5:17 PM
 */

#include <iostream>
#include <string>

#include "wilton/wilton.h"
#include "wilton/wiltoncall.h"

#include "wilton/support/wilton_support_exception.hpp"

class errcheck {
public:
    void operator=(char* err) {
        if (nullptr != err) {
            auto msg = std::string(err);
            wilton_free(err);
            throw wilton::support::wilton_support_exception(msg);
        }
    }
};

void test_db() {
    errcheck err;
    char* out = nullptr;
    int out_len = 0;
    
    std::string lconf = R"({
          "appenders": [{
            "appenderType" : "CONSOLE",
            "thresholdLevel" : "WARN"
          }],
          "loggers": [{
            "name": "staticlib",
            "level": "WARN"
          }]
        })";
    
    err = wilton_logger_initialize(lconf.c_str(), static_cast<int>(lconf.length()));
    
    std::string conf = R"({
          "defaultScriptEngine": "duktape",
          "requireJsDirPath": "../../wilton-requirejs",
          "requireJsConfig": {
            "waitSeconds": 0,
            "enforceDefine": true,
            "nodeIdCompat": true,
            "baseUrl": "../../modules"
          }
        })";
    
    err = wiltoncall_init(conf.c_str(), static_cast<int>(conf.length()));
    std::string name = "dyload_shared_library";
    std::string dconf = R"({
          "path": "libwilton_db.so"
        })";
    err = wiltoncall(name.c_str(), static_cast<int>(name.length()), dconf.c_str(), static_cast<int>(dconf.length()),
            std::addressof(out), std::addressof(out_len));
    std::string rconf = R"({
          "module": "wilton/test/db/index",
          "func": "main"
        })";
    err = wiltoncall_runscript_duktape(rconf.c_str(), static_cast<int> (rconf.length()), 
            std::addressof(out), std::addressof(out_len));
}

int main() {
    try {
        test_db();
    } catch (const std::exception& e) {
        std::cout << e.what() << std::endl;
        return 1;
    }
    return 0;
}
