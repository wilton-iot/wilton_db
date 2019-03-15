/*
 * Copyright 2017, alex at staticlibs.net
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/* 
 * File:   wiltoncall_db.cpp
 * Author: alex
 *
 * Created on January 10, 2017, 6:14 PM
 */

#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "staticlib/config.hpp"
#include "staticlib/io.hpp"
#include "staticlib/json.hpp"
#include "staticlib/support.hpp"
#include "staticlib/utils.hpp"

#include "wilton/wilton.h"
#include "wilton/wiltoncall.h"
#include "wilton/wilton_db.h"
#include "wilton/wilton_db_psql.h"

#include "wilton/support/unique_handle_registry.hpp"
#include "wilton/support/buffer.hpp"
#include "wilton/support/registrar.hpp"

namespace wilton {
namespace db {

namespace { //anonymous

// initialized from wilton_module_init
std::shared_ptr<support::unique_handle_registry<wilton_DBConnection>> conn_registry() {
    static auto registry = std::make_shared<
            support::unique_handle_registry<wilton_DBConnection>>(wilton_DBConnection_close);
    return registry;
}

// initialized from wilton_module_init
std::shared_ptr<support::unique_handle_registry<wilton_DBTransaction>> tran_registry() {
    static auto registry = std::make_shared<
            support::unique_handle_registry<wilton_DBTransaction>>(wilton_DBTransaction_rollback);
    return registry;
}

// initialized from wilton_module_init
std::shared_ptr<support::unique_handle_registry<wilton_PGConnection>> psql_conn_registry() {
    static auto registry = std::make_shared<
            support::unique_handle_registry<wilton_PGConnection>>(wilton_PGConnection_close);
    return registry;
}

} // namespace

// calls

support::buffer connection_open(sl::io::span<const char> data) {
    wilton_DBConnection* conn;
    char* err = wilton_DBConnection_open(std::addressof(conn), data.data(), data.size_int());
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    auto reg = conn_registry();
    int64_t handle = reg->put(conn);
    return support::make_json_buffer({
        { "connectionHandle", handle}
    });
}

support::buffer connection_query(sl::io::span<const char> data) {
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
    auto reg = conn_registry();
    wilton_DBConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* out = nullptr;
    int out_len = 0;
    char* err = wilton_DBConnection_query(conn, sql.c_str(), static_cast<int>(sql.length()),
            params.c_str(), static_cast<int>(params.length()),
            std::addressof(out), std::addressof(out_len));
    reg->put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    return support::wrap_wilton_buffer(out, out_len);
}

support::buffer connection_execute(sl::io::span<const char> data) {
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
    auto reg = conn_registry();
    wilton_DBConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_DBConnection_execute(conn, sql.c_str(), static_cast<int>(sql.length()),
            params.c_str(), static_cast<int>(params.length()));
    reg->put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    return support::make_null_buffer();
}

support::buffer connection_close(sl::io::span<const char> data) {
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
    auto reg = conn_registry();
    wilton_DBConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_DBConnection_close(conn);
    if (nullptr != err) {
        reg->put(conn);
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_null_buffer();
}

support::buffer transaction_start(sl::io::span<const char> data) {
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
    auto creg = conn_registry();
    wilton_DBConnection* conn = creg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    wilton_DBTransaction* tran;
    char* err = wilton_DBTransaction_start(conn, std::addressof(tran));
    creg->put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err +
            "\ndb_transaction_start error for input data"));
    auto treg = tran_registry();
    int64_t thandle = treg->put(tran);
    return support::make_json_buffer({
        { "transactionHandle", thandle}
    });
}

support::buffer transaction_commit(sl::io::span<const char> data) {
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
    auto treg = tran_registry();
    wilton_DBTransaction* tran = treg->remove(handle);
    if (nullptr == tran) throw support::exception(TRACEMSG(
            "Invalid 'transactionHandle' parameter specified"));
    char* err = wilton_DBTransaction_commit(tran);
    if (nullptr != err) {
        treg->put(tran);
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_null_buffer();
}

support::buffer transaction_rollback(sl::io::span<const char> data) {
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
    auto treg = tran_registry();
    wilton_DBTransaction* tran = treg->remove(handle);
    if (nullptr == tran) throw support::exception(TRACEMSG(
            "Invalid 'transactionHandle' parameter specified"));
    char* err = wilton_DBTransaction_rollback(tran);
    if (nullptr != err) {
        treg->put(tran);
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_null_buffer();
}


support::buffer db_pgsql_connection_open(sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    auto parameters = std::string{};
    for (const sl::json::field& fi : json.as_object()) {
        auto& name = fi.name();
        if ("parameters" == name) {
            parameters = fi.as_string_nonempty_or_throw(name);
        } else  {
            throw support::exception(TRACEMSG("Unknown data field: [" + name + "]"));
        }
    }
    if (parameters.empty()) throw support::exception(TRACEMSG(
            "Required parameter 'parameters' not specified"));

    wilton_PGConnection* conn;
    char* err = wilton_PGConnection_open(std::addressof(conn), parameters.c_str(), static_cast<int>(parameters.size()));
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    auto reg = psql_conn_registry();
    int64_t handle = reg->put(conn);
    return support::make_json_buffer({
        { "connectionHandle", handle}
    });
}

support::buffer db_pgsql_connection_close(sl::io::span<const char> data) {
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
    auto reg = psql_conn_registry();
    wilton_PGConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_PGConnection_close(conn);
    if (nullptr != err) {
        reg->put(conn);
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_null_buffer();
}

support::buffer db_pgsql_connection_execute_sql (sl::io::span<const char> data) {
    // json parse
    auto json = sl::json::load(data);
    int64_t handle = -1;
    auto sql_text = std::string{};
    auto params = std::string{"{}"}; // empty json by default
    bool cache_flag = true; // ON by default
    for (const sl::json::field& fi : json.as_object()) {
        auto& field_name = fi.name();
        if ("connectionHandle" == field_name) {
            handle = fi.as_int64_or_throw(field_name);
        } else if ("sql" == field_name) {
            sql_text = fi.as_string_nonempty_or_throw(field_name);
        } else if ("params" == field_name) {
            params = fi.val().dumps();
        } else if ("cache" == field_name) {
            cache_flag = fi.as_bool_or_throw(field_name);
        } else {
            throw support::exception(TRACEMSG("Unknown data field: [" + field_name + "]"));
        }
    }

    if (-1 == handle) throw support::exception(TRACEMSG(
            "Required parameter 'connectionHandle' not specified"));

    // get handle
    auto reg = psql_conn_registry();
    wilton_PGConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* out = nullptr;
    int out_len = 0;
    char* err = wilton_PGConnection_execute_sql(conn,
            sql_text.c_str(), static_cast<int>(sql_text.length()),
            params.c_str(), static_cast<int>(params.length()), cache_flag,
            std::addressof(out), std::addressof(out_len));
    reg->put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    return support::wrap_wilton_buffer(out, out_len);
}

support::buffer db_pgsql_transaction_begin(sl::io::span<const char> data) {
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
    auto reg = psql_conn_registry();
    wilton_PGConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_PGConnection_transaction_begin(conn);
    reg->put(conn);
    if (nullptr != err) {
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_null_buffer();
}

support::buffer db_pgsql_transaction_commit(sl::io::span<const char> data) {
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
    auto reg = psql_conn_registry();
    wilton_PGConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_PGConnection_transaction_commit(conn);
    reg->put(conn);
    if (nullptr != err) {
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_null_buffer();
}

support::buffer db_pgsql_transaction_rollback(sl::io::span<const char> data) {
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
    auto reg = psql_conn_registry();
    wilton_PGConnection* conn = reg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    // call wilton
    char* err = wilton_PGConnection_transaction_rollback(conn);
    reg->put(conn);
    if (nullptr != err) {
        support::throw_wilton_error(err, TRACEMSG(err));
    }
    return support::make_null_buffer();
}


} // namespace
}

extern "C" char* wilton_module_init() {
    try {
        wilton::db::conn_registry();
        wilton::db::tran_registry();
        wilton::db::psql_conn_registry();
        auto err = wilton_DBConnection_initialize_backends();
        if (nullptr != err) wilton::support::throw_wilton_error(err, TRACEMSG(err));

        wilton::support::register_wiltoncall("db_connection_open", wilton::db::connection_open);
        wilton::support::register_wiltoncall("db_connection_query", wilton::db::connection_query);
        wilton::support::register_wiltoncall("db_connection_execute", wilton::db::connection_execute);
        wilton::support::register_wiltoncall("db_connection_close", wilton::db::connection_close);
        wilton::support::register_wiltoncall("db_transaction_start", wilton::db::transaction_start);
        wilton::support::register_wiltoncall("db_transaction_commit", wilton::db::transaction_commit);
        wilton::support::register_wiltoncall("db_transaction_rollback", wilton::db::transaction_rollback);

        // postgresql
        wilton::support::register_wiltoncall("db_pgsql_connection_open", wilton::db::db_pgsql_connection_open);
        wilton::support::register_wiltoncall("db_pgsql_connection_close", wilton::db::db_pgsql_connection_close);
        wilton::support::register_wiltoncall("db_pgsql_connection_execute_sql", wilton::db::db_pgsql_connection_execute_sql);

        wilton::support::register_wiltoncall("db_pgsql_transaction_begin", wilton::db::db_pgsql_transaction_begin);
        wilton::support::register_wiltoncall("db_pgsql_transaction_commit", wilton::db::db_pgsql_transaction_commit);
        wilton::support::register_wiltoncall("db_pgsql_transaction_rollback", wilton::db::db_pgsql_transaction_rollback);

        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}
