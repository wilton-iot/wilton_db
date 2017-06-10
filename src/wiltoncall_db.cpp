/* 
 * File:   wiltoncall_db.cpp
 * Author: alex
 *
 * Created on January 10, 2017, 6:14 PM
 */


#include <cstdint>
#include <mutex>
#include <string>
#include <unordered_set>

#include "staticlib/config.hpp"
#include "staticlib/io.hpp"
#include "staticlib/json.hpp"
#include "staticlib/support.hpp"

#include "wilton/wilton.h"
#include "wilton/wiltoncall.h"
#include "wilton/wilton_db.h"

#include "utils.hpp"
#include "wilton_db_exception.hpp"

namespace wilton {
namespace db {

namespace { //anonymous

const std::string& empty_string() {
    static std::string empty;
    return empty;
}

void throw_wilton_error(char* err, const std::string& msg) {
    wilton_free(err);
    throw wilton_db_exception(msg);
}

std::string wrap_wilton_output(char* out, int out_len) {
    std::string res{out, static_cast<std::string::size_type> (out_len)};
    wilton_free(out);
    return res;
}

void register_call(const std::string& name, std::string (*fun)(const char*, uint32_t)) {
    auto err = wiltoncall_register(name.c_str(), static_cast<int> (name.length()), reinterpret_cast<void*>(fun),
        [] (void* vfun, const char* json_in, int json_in_len, char** json_out, int* json_out_len) -> char* {
            if (nullptr == vfun) return wilton::db::alloc_copy(TRACEMSG("Null 'vfun' parameter specified"));
            if (nullptr == json_in) return wilton::db::alloc_copy(TRACEMSG("Null 'json_in' parameter specified"));
            if (!sl::support::is_uint32_positive(json_in_len)) return wilton::db::alloc_copy(TRACEMSG(
                    "Invalid 'json_in_len' parameter specified: [" + sl::support::to_string(json_in_len) + "]"));
            try {
                auto fun = reinterpret_cast<std::string(*)(const char*, uint32_t)>(vfun);
                std::string out = (*fun)(json_in, static_cast<uint32_t> (json_in_len));
                *json_out = wilton::db::alloc_copy(out);
                *json_out_len = static_cast<int> (out.length());
                return nullptr;
            } catch (const std::exception& e) {
                return wilton::db::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
            }
        });
    if (nullptr != err) {
        throw_wilton_error(err, TRACEMSG(err));
    }
}

template<typename T>
class handle_registry {
    std::unordered_set<T*> registry;
    std::mutex mutex;

public:
    int64_t put(T* ptr) {
        std::lock_guard<std::mutex> lock{mutex};
        auto pair = registry.insert(ptr);
        return pair.second ? reinterpret_cast<int64_t> (ptr) : 0;
    }

    T* remove(int64_t handle) {
        std::lock_guard<std::mutex> lock{mutex};
        T* ptr = reinterpret_cast<T*> (handle);
        auto erased = registry.erase(ptr);
        return 1 == erased ? ptr : nullptr;
    }

    T* peek(int64_t handle) {
        std::lock_guard<std::mutex> lock{mutex};
        T* ptr = reinterpret_cast<T*> (handle);
        auto exists = registry.count(ptr);
        return 1 == exists ? ptr : nullptr;
    }
};

handle_registry<wilton_DBConnection>& static_conn_registry() {
    static handle_registry<wilton_DBConnection> registry;
    return registry;
}

handle_registry<wilton_DBTransaction>& static_tran_registry() {
    static handle_registry<wilton_DBTransaction> registry;
    return registry;
}

} // namespace

// calls

std::string db_connection_open(const char* data, uint32_t data_len) {
    wilton_DBConnection* conn;
    char* err = wilton_DBConnection_open(std::addressof(conn), data, static_cast<int>(data_len));
    if (nullptr != err) throw_wilton_error(err, TRACEMSG(err));
    int64_t handle = static_conn_registry().put(conn);
    return sl::json::dumps({
        { "connectionHandle", handle}
    });
}

std::string db_connection_query(const char* data, uint32_t data_len) {
    // json parse
    auto src = sl::io::array_source(data, data_len);
    sl::json::value json = sl::json::loads(data);
    int64_t handle = -1;
    auto rsql = std::ref(empty_string());
    std::string params = empty_string();
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else if ("sql" == name) {
            rsql = fi.as_string_nonempty_or_throw(name);
        } else if ("params" == name) {
            params = fi.val().dumps();
        } else {
            throw wilton_db_exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    if (rsql.get().empty()) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'sql' not specified"));
    const std::string& sql = rsql.get();
    if (params.empty()) {
        params = "{}";
    }
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw wilton_db_exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* out;
    int out_len;
    char* err = wilton_DBConnection_query(conn, sql.c_str(), static_cast<int>(sql.length()),
            params.c_str(), static_cast<int>(params.length()),
            std::addressof(out), std::addressof(out_len));
    static_conn_registry().put(conn);
    if (nullptr != err) throw_wilton_error(err, TRACEMSG(err));
    return wrap_wilton_output(out, out_len);
}

std::string db_connection_execute(const char* data, uint32_t data_len) {
    // json parse
    auto src = sl::io::array_source(data, data_len);
    sl::json::value json = sl::json::loads(data);
    int64_t handle = -1;
    auto rsql = std::ref(empty_string());
    std::string params = empty_string();
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else if ("sql" == name) {
            rsql = fi.as_string_nonempty_or_throw(name);
        } else if ("params" == name) {
            params = fi.val().dumps();
        } else {
            throw wilton_db_exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    if (rsql.get().empty()) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'sql' not specified"));
    const std::string& sql = rsql.get();
    if (params.empty()) {
        params = "{}";
    }
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw wilton_db_exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_DBConnection_execute(conn, sql.c_str(), static_cast<int>(sql.length()),
            params.c_str(), static_cast<int>(params.length()));
    static_conn_registry().put(conn);
    if (nullptr != err) throw_wilton_error(err, TRACEMSG(err));
    return "{}";
}

std::string db_connection_close(const char* data, uint32_t data_len) {
    // json parse
    auto src = sl::io::array_source(data, data_len);
    sl::json::value json = sl::json::loads(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw wilton_db_exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw wilton_db_exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_DBConnection_close(conn);
    if (nullptr != err) {
        static_conn_registry().put(conn);
        throw_wilton_error(err, TRACEMSG(err));
    }
    return "{}";
}

std::string db_transaction_start(const char* data, uint32_t data_len) {
    // json parse
    auto src = sl::io::array_source(data, data_len);
    sl::json::value json = sl::json::loads(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw wilton_db_exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw wilton_db_exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    wilton_DBTransaction* tran;
    char* err = wilton_DBTransaction_start(conn, std::addressof(tran));
    static_conn_registry().put(conn);
    if (nullptr != err) throw_wilton_error(err, TRACEMSG(err +
            "\ndb_transaction_start error for input data"));
    int64_t thandle = static_tran_registry().put(tran);
    return sl::json::dumps({
        { "transactionHandle", thandle}
    });
}

std::string db_transaction_commit(const char* data, uint32_t data_len) {
    // json parse
    auto src = sl::io::array_source(data, data_len);
    sl::json::value json = sl::json::loads(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("transactionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw wilton_db_exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'transactionHandle' not specified"));
    // get handle
    wilton_DBTransaction* tran = static_tran_registry().remove(handle);
    if (nullptr == tran) throw wilton_db_exception(TRACEMSG(
            "Invalid 'transactionHandle' parameter specified"));
    char* err = wilton_DBTransaction_commit(tran);
    if (nullptr != err) {
        static_tran_registry().put(tran);
        throw_wilton_error(err, TRACEMSG(err));
    }
    return "{}";
}

std::string db_transaction_rollback(const char* data, uint32_t data_len) {
    // json parse
    auto src = sl::io::array_source(data, data_len);
    sl::json::value json = sl::json::loads(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("transactionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw wilton_db_exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw wilton_db_exception(TRACEMSG(
            "Required parameter 'transactionHandle' not specified"));
    // get handle
    wilton_DBTransaction* tran = static_tran_registry().remove(handle);
    if (nullptr == tran) throw wilton_db_exception(TRACEMSG(
            "Invalid 'transactionHandle' parameter specified"));
    char* err = wilton_DBTransaction_rollback(tran);
    if (nullptr != err) {
        static_tran_registry().put(tran);
        throw_wilton_error(err, TRACEMSG(err));
    }
    return "{}";
}

} // namespace
}

extern "C" WILTON_EXPORT char* wilton_module_init() {
    try {
        wilton::db::register_call("db_connection_open", wilton::db::db_connection_open);
        wilton::db::register_call("db_connection_query", wilton::db::db_connection_query);
        wilton::db::register_call("db_connection_execute", wilton::db::db_connection_execute);
        wilton::db::register_call("db_connection_close", wilton::db::db_connection_close);
        wilton::db::register_call("db_connection_open", wilton::db::db_connection_open);
        wilton::db::register_call("db_transaction_start", wilton::db::db_transaction_start);
        wilton::db::register_call("db_transaction_commit", wilton::db::db_transaction_commit);
        wilton::db::register_call("db_transaction_rollback", wilton::db::db_transaction_rollback);
    } catch (const std::exception& e) {
        return wilton::db::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}
