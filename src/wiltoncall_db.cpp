/* 
 * File:   wiltoncall_db.cpp
 * Author: alex
 *
 * Created on January 10, 2017, 6:14 PM
 */

#include <cstdint>
#include <string>

#include "staticlib/config.hpp"
#include "staticlib/io.hpp"
#include "staticlib/json.hpp"
#include "staticlib/support.hpp"
#include "staticlib/utils.hpp"

#include "wilton/wilton.h"
#include "wilton/wiltoncall.h"
#include "wilton/wilton_db.h"

#include "wilton/support/handle_registry.hpp"
#include "wilton/support/buffer.hpp"
#include "wilton/support/registrar.hpp"

namespace wilton {
namespace db {

namespace { //anonymous

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

support::buffer db_connection_open(sl::io::span<const char> data) {
    wilton_DBConnection* conn;
    char* err = wilton_DBConnection_open(std::addressof(conn), data.data(), static_cast<int>(data.size()));
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    int64_t handle = static_conn_registry().put(conn);
    return support::make_json_buffer({
        { "connectionHandle", handle}
    });
}

support::buffer db_connection_query(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    int64_t handle = -1;
    auto rsql = std::ref(sl::utils::empty_string());
    auto params = std::string();
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else if ("sql" == name) {
            rsql = fi.as_string_nonempty_or_throw(name);
        } else if ("params" == name) {
            params = fi.val().dumps();
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw support::exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    if (rsql.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'sql' not specified"));
    const std::string& sql = rsql.get();
    if (params.empty()) {
        params = "{}";
    }
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* out = nullptr;
    int out_len = 0;
    char* err = wilton_DBConnection_query(conn, sql.c_str(), static_cast<int>(sql.length()),
            params.c_str(), static_cast<int>(params.length()),
            std::addressof(out), std::addressof(out_len));
    static_conn_registry().put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    return support::wrap_wilton_buffer(out, out_len);
}

support::buffer db_connection_execute(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    int64_t handle = -1;
    auto rsql = std::ref(sl::utils::empty_string());
    auto params = std::string();
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else if ("sql" == name) {
            rsql = fi.as_string_nonempty_or_throw(name);
        } else if ("params" == name) {
            params = fi.val().dumps();
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw support::exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    if (rsql.get().empty()) throw support::exception(TRACEMSG(
            "Required parameter 'sql' not specified"));
    const std::string& sql = rsql.get();
    if (params.empty()) {
        params = "{}";
    }
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_DBConnection_execute(conn, sql.c_str(), static_cast<int>(sql.length()),
            params.c_str(), static_cast<int>(params.length()));
    static_conn_registry().put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    return support::make_empty_buffer();
}

support::buffer db_connection_close(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw support::exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_DBConnection_close(conn);
    if (nullptr != err) {
        static_conn_registry().put(conn);
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_empty_buffer();
}

support::buffer db_transaction_start(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("connectionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw support::exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));
    // get handle
    wilton_DBConnection* conn = static_conn_registry().remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    wilton_DBTransaction* tran;
    char* err = wilton_DBTransaction_start(conn, std::addressof(tran));
    static_conn_registry().put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err +
            "\ndb_transaction_start error for input data"));
    int64_t thandle = static_tran_registry().put(tran);
    return support::make_json_buffer({
        { "transactionHandle", thandle}
    });
}

support::buffer db_transaction_commit(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("transactionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw support::exception(TRACEMSG(
            "Required parameter 'transactionHandle' not specified"));
    // get handle
    wilton_DBTransaction* tran = static_tran_registry().remove(handle);
    if (nullptr == tran) throw support::exception(TRACEMSG(
            "Invalid 'transactionHandle' parameter specified"));
    char* err = wilton_DBTransaction_commit(tran);
    if (nullptr != err) {
        static_tran_registry().put(tran);
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_empty_buffer();
}

support::buffer db_transaction_rollback(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    int64_t handle = -1;
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("transactionHandle" == name) {
            handle = fi.as_int64_or_throw(name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (-1 == handle) throw support::exception(TRACEMSG(
            "Required parameter 'transactionHandle' not specified"));
    // get handle
    wilton_DBTransaction* tran = static_tran_registry().remove(handle);
    if (nullptr == tran) throw support::exception(TRACEMSG(
            "Invalid 'transactionHandle' parameter specified"));
    char* err = wilton_DBTransaction_rollback(tran);
    if (nullptr != err) {
        static_tran_registry().put(tran);
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_empty_buffer();
}

} // namespace
}

extern "C" char* wilton_module_init() {
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
