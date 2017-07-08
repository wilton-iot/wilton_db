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

#include "wilton/support/handle_registry.hpp"
#include "wilton/support/span_operations.hpp"
#include "wilton/support/registrar.hpp"

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

support::handle_registry<wilton_DBConnection>& static_conn_registry() {
    static support::handle_registry<wilton_DBConnection> registry {
        [] (wilton_DBConnection* conn) STATICLIB_NOEXCEPT {
            wilton_DBConnection_close(conn);
        }};
    return registry;
}

support::handle_registry<wilton_DBTransaction>& static_tran_registry() {
    static support::handle_registry<wilton_DBTransaction> registry {
        [] (wilton_DBTransaction* tran) STATICLIB_NOEXCEPT {
            wilton_DBTransaction_rollback(tran);
        }};
    return registry;
}

} // namespace

// calls

sl::support::optional<sl::io::span<char>> db_connection_open(sl::io::span<const char> data) {
    wilton_DBConnection* conn;
    char* err = wilton_DBConnection_open(std::addressof(conn), data.data(), static_cast<int>(data.size()));
    if (nullptr != err) throw_wilton_error(err, TRACEMSG(err));
    int64_t handle = static_conn_registry().put(conn);
    return support::json_span({
        { "connectionHandle", handle}
    });
}

sl::support::optional<sl::io::span<char>> db_connection_query(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
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
    return support::buffer_span(out, out_len);
}

sl::support::optional<sl::io::span<char>> db_connection_execute(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
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
    return support::empty_span();
}

sl::support::optional<sl::io::span<char>> db_connection_close(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
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
    return support::empty_span();
}

sl::support::optional<sl::io::span<char>> db_transaction_start(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
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
    return support::json_span({
        { "transactionHandle", thandle}
    });
}

sl::support::optional<sl::io::span<char>> db_transaction_commit(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
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
    return support::empty_span();
}

sl::support::optional<sl::io::span<char>> db_transaction_rollback(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
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
    return support::empty_span();
}

} // namespace
}

extern "C" WILTON_DB_EXPORT char* wilton_module_init() {
    try {
        wilton::support::register_wiltoncall("db_connection_open", wilton::db::db_connection_open);
        wilton::support::register_wiltoncall("db_connection_query", wilton::db::db_connection_query);
        wilton::support::register_wiltoncall("db_connection_execute", wilton::db::db_connection_execute);
        wilton::support::register_wiltoncall("db_connection_close", wilton::db::db_connection_close);
        wilton::support::register_wiltoncall("db_transaction_start", wilton::db::db_transaction_start);
        wilton::support::register_wiltoncall("db_transaction_commit", wilton::db::db_transaction_commit);
        wilton::support::register_wiltoncall("db_transaction_rollback", wilton::db::db_transaction_rollback);
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}
