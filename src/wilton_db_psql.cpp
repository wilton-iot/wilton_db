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
#include "psql_functions.h"

namespace { // anonymous

const std::string logger = std::string("wilton.DBConnection_psql");

} // namespace

struct wilton_db_psql_connection {
private:
    psql_handler conn;

public:
    wilton_db_psql_connection(psql_handler&& conn) :
    conn(std::move(conn)) { }

    psql_handler& impl() {
        return conn;
    }
};

char* wilton_db_psql_connection_open(
        wilton_db_psql_connection** conn_out,
        const char* conn_url,
        int conn_url_len,
        bool is_ping_on) /* noexcept */ {
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
        wilton_db_psql_connection* conn_ptr = new wilton_db_psql_connection{std::move(conn)};
        *conn_out = conn_ptr;
        wilton::support::log_debug(logger, "Connection created by psql, handle: [" + wilton::support::strhandle(conn_ptr) + "]");
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}


//char* wilton_db_psql_connection_prepare(
//        wilton_db_psql_connection* conn,
//        const char* sql_text,
//        int sql_text_len,
//        const char* prepare_name,
//        int prepare_name_len,
//        char** result_set_out,
//        int* result_set_len_out) {
//    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
//    if (nullptr == sql_text) return wilton::support::alloc_copy(TRACEMSG("Null 'sql_text' parameter specified"));
//    if (!sl::support::is_uint32_positive(sql_text_len)) return wilton::support::alloc_copy(TRACEMSG(
//         "Invalid 'sql_text_len' parameter specified: [" + sl::support::to_string(sql_text_len) + "]"));
//    if (nullptr == prepare_name) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
//    if (!sl::support::is_uint32(prepare_name_len)) return wilton::support::alloc_copy(TRACEMSG(
//         "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(prepare_name_len) + "]"));
//    if (nullptr == result_set_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_out' parameter specified"));
//    if (nullptr == result_set_len_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_len_out' parameter specified"));
//    try {
//        uint32_t sql_text_len_u32 = static_cast<uint32_t> (sql_text_len);
//        std::string sql_text_str{sql_text, sql_text_len_u32};
//        std::string name = prepare_name_len > 0 ? std::string{prepare_name, static_cast<size_t>(prepare_name_len)} : std::string{""};
//        wilton::support::log_debug(logger, "prepare SQL: [" + sql_text_str + "]," +
//                " name : [" + name + "], handle: [" + wilton::support::strhandle(conn) + "] ...");
//        std::string rs = conn->impl().prepare(sql_text_str, name);
//        auto span = wilton::support::make_string_buffer(rs);
//        *result_set_out = span.data();
//        *result_set_len_out = span.size_int();
//        wilton::support::log_debug(logger, "prepare complete, result: [" + rs + "]");
//        return nullptr;
//    } catch (const std::exception& e) {
//        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
//    }
//}


//char* wilton_db_psql_connection_get_prepare_info(
//        wilton_db_psql_connection* conn,
//        const char* prepare_name,
//        int prepare_name_len,
//        char** result_set_out,
//        int* result_set_len_out) {
//    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
//    if (nullptr == prepare_name) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
//    if (!sl::support::is_uint32(prepare_name_len)) return wilton::support::alloc_copy(TRACEMSG(
//         "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(prepare_name_len) + "]"));
//    if (nullptr == result_set_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_out' parameter specified"));
//    if (nullptr == result_set_len_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_len_out' parameter specified"));
//    try {
//        std::string name = prepare_name_len > 0 ? std::string{prepare_name, static_cast<size_t>(prepare_name_len)} : std::string{""};
//        wilton::support::log_debug(logger, "Get prepare info: name : [" +
//                name + "], handle: [" + wilton::support::strhandle(conn) + "] ...");
//        std::string rs = conn->impl().get_prepared_info(name);
//        auto span = wilton::support::make_string_buffer(rs);
//        *result_set_out = span.data();
//        *result_set_len_out = span.size_int();
//        wilton::support::log_debug(logger, "Prepare info recieved: [" + rs + "]");
//        return nullptr;
//    } catch (const std::exception& e) {
//        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
//    }
//}


//char* wilton_db_psql_connection_execute_prepared(
//        wilton_db_psql_connection* conn,
//        const char* prepare_name,
//        int prepare_name_len,
//        char** result_set_out,
//        int* result_set_len_out){
//    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
//    if (nullptr == prepare_name) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
//    if (!sl::support::is_uint32(prepare_name_len)) return wilton::support::alloc_copy(TRACEMSG(
//         "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(prepare_name_len) + "]"));
//    if (nullptr == result_set_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_out' parameter specified"));
//    if (nullptr == result_set_len_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_len_out' parameter specified"));
//    try {
//        std::string name = prepare_name_len > 0 ? std::string{prepare_name, static_cast<size_t>(prepare_name_len)} : std::string{""};
//        wilton::support::log_debug(logger, "Executing prepared SQL: [" + name + "], handle: [" + wilton::support::strhandle(conn) + "] ...");
//        std::string rs = conn->impl().execute_prepared(name);
//        auto span = wilton::support::make_string_buffer(rs);
//        *result_set_out = span.data();
//        *result_set_len_out = span.size_int();
//        wilton::support::log_debug(logger, "Execution complete, result: [" + rs + "]");
//        return nullptr;
//    } catch (const std::exception& e) {
//        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
//    }
//}

//char* wilton_db_psql_connection_execute_prepared_with_parameters(
//        wilton_db_psql_connection* conn,
//        const char* prepare_name,
//        int prepare_name_len,
//        const char* params_json,
//        int params_json_len,
//        char** result_set_out,
//        int* result_set_len_out){
//    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
//    if (nullptr == prepare_name) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
//    if (!sl::support::is_uint32(prepare_name_len)) return wilton::support::alloc_copy(TRACEMSG(
//         "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(prepare_name_len) + "]"));
//    if (nullptr == params_json) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
//    if (!sl::support::is_uint32(params_json_len)) return wilton::support::alloc_copy(TRACEMSG(
//        "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(params_json_len) + "]"));
//    if (nullptr == result_set_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_out' parameter specified"));
//    if (nullptr == result_set_len_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_len_out' parameter specified"));
//    try {
//        std::string name = prepare_name_len > 0 ? std::string{prepare_name, static_cast<size_t>(prepare_name_len)} : std::string{""};
//        uint32_t json_text_len_u32 = static_cast<uint32_t> (params_json_len);
//        std::string json_text_str{params_json, json_text_len_u32};
//        wilton::support::log_debug(logger, "Executing prepared SQL: [" + name + "], parameters:" +
//                json_text_str + "], handle: [" + wilton::support::strhandle(conn) + "] ...");
//        std::string rs = conn->impl().execute_prepared_with_parameters(name, sl::json::loads(json_text_str));//sql_text_str, name);
//        auto span = wilton::support::make_string_buffer(rs);
//        *result_set_out = span.data();
//        *result_set_len_out = span.size_int();
//        wilton::support::log_debug(logger, "Execution complete, result: [" + rs + "]");
//        return nullptr;
//    } catch (const std::exception& e) {
//        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
//    }
//}

//char* wilton_db_psql_connection_execute_sql(
//        wilton_db_psql_connection* conn,
//        const char* sql_text,
//        int sql_text_len,
//        char** result_set_out,
//        int* result_set_len_out) {
//    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
//    if (nullptr == sql_text) return wilton::support::alloc_copy(TRACEMSG("Null 'sql_text' parameter specified"));
//    if (!sl::support::is_uint32_positive(sql_text_len)) return wilton::support::alloc_copy(TRACEMSG(
//        "Invalid 'sql_text_len' parameter specified: [" + sl::support::to_string(sql_text_len) + "]"));
//    if (nullptr == result_set_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_out' parameter specified"));
//    if (nullptr == result_set_len_out) return wilton::support::alloc_copy(TRACEMSG("Null 'result_set_len_out' parameter specified"));
//    try {
//        uint32_t sql_text_len_u32 = static_cast<uint32_t> (sql_text_len);
//        std::string sql_text_str{sql_text, sql_text_len_u32};
//        wilton::support::log_debug(logger, "Executing SQL: [" +
//                sql_text_str + "], handle: [" + wilton::support::strhandle(conn) + "] ...");
//        std::string rs = conn->impl().execute_sql_statement(sql_text_str);
//        auto span = wilton::support::make_string_buffer(rs);
//        *result_set_out = span.data();
//        *result_set_len_out = span.size_int();
//        wilton::support::log_debug(logger, "Execution complete, result: [" + rs + "]");
//        return nullptr;
//    } catch (const std::exception& e) {
//        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
//    }
//}

char* wilton_db_psql_connection_execute_sql(wilton_db_psql_connection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len,
        bool cache_flag,
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


char* wilton_db_psql_connection_close(
        wilton_db_psql_connection* conn) {
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

 char* wilton_db_psql_transaction_begin(
         wilton_db_psql_connection* conn) {
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

 char* wilton_db_psql_transaction_commit(
         wilton_db_psql_connection* conn) {
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

char* wilton_db_psql_transaction_rollback(
        wilton_db_psql_connection* conn) {
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

//char* wilton_db_psql_connection_deallocate_prepared(
//        wilton_db_psql_connection* conn,
//        const char* prepared_name,
//        int prepared_name_len) {
//    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
//    if (nullptr == prepared_name) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
//    if (!sl::support::is_uint32(prepared_name_len)) return wilton::support::alloc_copy(TRACEMSG(
//         "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(prepared_name_len) + "]"));
//    try {
//        std::string name{prepared_name, static_cast<size_t>(prepared_name_len)};
//        wilton::support::log_debug(logger, "Deallocate_prepared, name : [" + name + "], handle: [" + wilton::support::strhandle(conn) + "] ...");
//        conn->impl().deallocate_prepared_statement(name);
//        wilton::support::log_debug(logger, "Deallocated");
//        return nullptr;
//    } catch (const std::exception& e) {
//        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
//    }
//}

