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

#include "wilton/support/handle_registry.hpp"
#include "wilton/support/buffer.hpp"
#include "wilton/support/registrar.hpp"

namespace wilton {
namespace db {

namespace { //anonymous

// initialized from wilton_module_init
std::shared_ptr<support::handle_registry<wilton_DBConnection>> shared_conn_registry() {
    static auto registry = std::make_shared<support::handle_registry<wilton_DBConnection>>(
        [] (wilton_DBConnection* conn) STATICLIB_NOEXCEPT {
            wilton_DBConnection_close(conn);
        });
    return registry;
}

// initialized from wilton_module_init
std::shared_ptr<support::handle_registry<wilton_DBTransaction>> shared_tran_registry() {
    static auto registry = std::make_shared<support::handle_registry<wilton_DBTransaction>>(
        [] (wilton_DBTransaction* tran) STATICLIB_NOEXCEPT {
            wilton_DBTransaction_rollback(tran);
        });
    return registry;
}

} // namespace

// calls

support::buffer connection_open(sl::io::span<const char> data) {
    wilton_DBConnection* conn;
    char* err = wilton_DBConnection_open(std::addressof(conn), data.data(), data.size_int());
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err));
    auto reg = shared_conn_registry();
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
    auto reg = shared_conn_registry();
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
    auto reg = shared_conn_registry();
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
    auto reg = shared_conn_registry();
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
    auto creg = shared_conn_registry();
    wilton_DBConnection* conn = creg->remove(handle);
    if (nullptr == conn) throw support::exception(TRACEMSG(
            "Invalid 'connectionHandle' parameter specified"));
    wilton_DBTransaction* tran;
    char* err = wilton_DBTransaction_start(conn, std::addressof(tran));
    creg->put(conn);
    if (nullptr != err) support::throw_wilton_error(err, TRACEMSG(err +
            "\ndb_transaction_start error for input data"));
    auto treg = shared_tran_registry();
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
    auto treg = shared_tran_registry();
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
    auto treg = shared_tran_registry();
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

} // namespace
}

extern "C" char* wilton_module_init() {
    try {
        wilton::db::shared_conn_registry();
        wilton::db::shared_tran_registry();
        wilton::support::register_wiltoncall("db_connection_open", wilton::db::connection_open);
        wilton::support::register_wiltoncall("db_connection_query", wilton::db::connection_query);
        wilton::support::register_wiltoncall("db_connection_execute", wilton::db::connection_execute);
        wilton::support::register_wiltoncall("db_connection_close", wilton::db::connection_close);
        wilton::support::register_wiltoncall("db_transaction_start", wilton::db::transaction_start);
        wilton::support::register_wiltoncall("db_transaction_commit", wilton::db::transaction_commit);
        wilton::support::register_wiltoncall("db_transaction_rollback", wilton::db::transaction_rollback);
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}
