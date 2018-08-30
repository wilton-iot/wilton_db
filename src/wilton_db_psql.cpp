/*
 * Copyright 2017, alex at staticlibs.net
 * Copyright 2018, mike at myasnikov.mike@gmail.com
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

#include "wilton/wilton_db_psql.h"

#include <string>
#include <vector>

#include "staticlib/config.hpp"
#include "staticlib/io.hpp"
#include "staticlib/orm.hpp"
#include "staticlib/json.hpp"

#include "wilton/support/alloc.hpp"
#include "wilton/support/buffer.hpp"
#include "wilton/support/handle_registry.hpp"
#include "wilton/support/logging.hpp"

#include <libpq-fe.h>
#include "psql_functions.hpp"

namespace { // anonymous

const std::string logger = std::string("wilton.PGConnection");

} // namespace

struct wilton_PGConnection {
private:
    psql_handler conn;

public:
    wilton_PGConnection(psql_handler&& conn) :
    conn(std::move(conn)) { }

    psql_handler& impl() {
        return conn;
    }
};

char* wilton_PGConnection_open(wilton_PGConnection** conn_out,
        const char* conn_url,
        int conn_url_len,
        int is_ping_on) /* noexcept */ {
    if (nullptr == conn_out) return wilton::support::alloc_copy(TRACEMSG("Null 'conn_out' parameter specified"));
    if (nullptr == conn_url) return wilton::support::alloc_copy(TRACEMSG("Null 'conn_url' parameter specified"));
    if (!sl::support::is_uint16_positive(conn_url_len)) return wilton::support::alloc_copy(TRACEMSG(
            "Invalid 'conn_url_len' parameter specified: [" + sl::support::to_string(conn_url_len) + "]"));
    try {
        uint16_t conn_url_len_u16 = static_cast<uint16_t> (conn_url_len);
        std::string conn_url_str{conn_url, conn_url_len_u16};
        psql_handler conn{conn_url_str, is_ping_on};
        bool res = conn.connect();
        if (!res) {
            return wilton::support::alloc_copy(TRACEMSG(conn.get_last_error()));
        }
        wilton::support::log_debug(logger, "Creating connection by psql, parameters: [" + conn_url_str + "] ...");
        wilton_PGConnection* conn_ptr = new wilton_PGConnection{std::move(conn)};
        *conn_out = conn_ptr;
        wilton::support::log_debug(logger, "Connection created by psql, handle: [" + wilton::support::strhandle(conn_ptr) + "]");
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}

char* wilton_PGConnection_execute_sql(wilton_PGConnection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len,
        int cache_flag,
        char** result_set_out,
        int* result_set_len_out) {
    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
    if (nullptr == sql_text) return wilton::support::alloc_copy(TRACEMSG("Null 'sql_text' parameter specified"));
    if (!sl::support::is_uint32_positive(sql_text_len)) return wilton::support::alloc_copy(TRACEMSG(
        "Invalid 'sql_text_len' parameter specified: [" + sl::support::to_string(sql_text_len) + "]"));
    if (nullptr == params_json) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
    if (!sl::support::is_uint32(params_json_len)) return wilton::support::alloc_copy(TRACEMSG(
        "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(params_json_len) + "]"));
    if (nullptr == result_set_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_out' parameter specified"));
    if (nullptr == result_set_len_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_len_out' parameter specified"));
    try {
        uint32_t sql_text_len_u32 = static_cast<uint32_t> (sql_text_len);
        std::string sql_text_str{sql_text, sql_text_len_u32};
        uint32_t json_text_len_u32 = static_cast<uint32_t> (params_json_len);
        std::string json_text_str{params_json, json_text_len_u32};
        wilton::support::log_debug(logger, "Executing  SQL: [" + sql_text_str + "], parameters:" +
                json_text_str + "], handle: [" + wilton::support::strhandle(conn) + "] ...");
        sl::json::value rs = conn->impl().execute_with_parameters(sql_text_str, sl::json::loads(json_text_str), cache_flag);
        auto span = wilton::support::make_json_buffer(rs);
        *result_set_out = span.data();
        *result_set_len_out = span.size_int();
        wilton::support::log_debug(logger, "Execution complete, result: [" + rs.dumps() + "]");
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}


char* wilton_PGConnection_close(
        wilton_PGConnection* conn) {
    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
    try {
        wilton::support::log_debug(logger, "Closing connection, handle: [" + wilton::support::strhandle(conn) + "] ...");
        conn->impl().close();
        delete conn;
        wilton::support::log_debug(logger, "Connection closed");
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    } 
}

 char* wilton_PGConnection_transaction_begin(
         wilton_PGConnection* conn) {
    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
    try {
        wilton::support::log_debug(logger, "Starting transaction, connection handle: [" + wilton::support::strhandle(conn) + "] ...");
        conn->impl().begin();
        wilton::support::log_debug(logger, "Transaction started, handle: [" + wilton::support::strhandle(conn) + "]");
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
 }

 char* wilton_PGConnection_transaction_commit(
         wilton_PGConnection* conn) {
     if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
     try {
         wilton::support::log_debug(logger, "Committing transaction, handle: [" + wilton::support::strhandle(conn) + "] ...");
         conn->impl().commit();
         wilton::support::log_debug(logger, "Transaction committed");
         return nullptr;
     } catch (const std::exception& e) {
         return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
     }
 }

char* wilton_PGConnection_transaction_rollback(
        wilton_PGConnection* conn) {
    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
    try {
        wilton::support::log_debug(logger, "Rolling back transaction, handle: [" + wilton::support::strhandle(conn) + "] ...");
        conn->impl().rollback();
        wilton::support::log_debug(logger, "Transaction rolled back");
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}

