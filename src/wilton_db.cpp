/* 
 * File:   wilton_db.cpp
 * Author: alex
 * 
 * Created on June 4, 2016, 8:22 PM
 */

#include "wilton/wilton_db.h"

#include <string>
#include <vector>

#include "staticlib/config.hpp"
#include "staticlib/io.hpp"
#include "staticlib/orm.hpp"
#include "staticlib/json.hpp"

#include "wilton/support/alloc_copy.hpp"
#include "wilton/support/buffer.hpp"

struct wilton_DBConnection {
private:
    sl::orm::connection conn;

public:
    wilton_DBConnection(sl::orm::connection&& conn) :
    conn(std::move(conn)) { }

    sl::orm::connection& impl() {
        return conn;
    }
};

struct wilton_DBTransaction {
private:
    sl::orm::transaction tran;

public:
    wilton_DBTransaction(sl::orm::transaction&& tran) :
    tran(std::move(tran)) { }

    sl::orm::transaction& impl() {
        return tran;
    }
};

char* wilton_DBConnection_open(
        wilton_DBConnection** conn_out,
        const char* conn_url,
        int conn_url_len) /* noexcept */ {
    if (nullptr == conn_out) return wilton::support::alloc_copy(TRACEMSG("Null 'conn_out' parameter specified"));
    if (nullptr == conn_url) return wilton::support::alloc_copy(TRACEMSG("Null 'conn_url' parameter specified"));
    if (!sl::support::is_uint16_positive(conn_url_len)) return wilton::support::alloc_copy(TRACEMSG(
            "Invalid 'conn_url_len' parameter specified: [" + sl::support::to_string(conn_url_len) + "]"));
    try {
        uint16_t conn_url_len_u16 = static_cast<uint16_t> (conn_url_len);
        std::string conn_url_str{conn_url, conn_url_len_u16};
        sl::orm::connection conn{conn_url_str};
        wilton_DBConnection* conn_ptr = new wilton_DBConnection{std::move(conn)};
        *conn_out = conn_ptr;
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}

char* wilton_DBConnection_query(
        wilton_DBConnection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len,
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
        auto json = params_json_len > 0 ? sl::json::load({params_json, params_json_len}) : sl::json::value();
        std::vector<sl::json::value> rs = conn->impl().query(sql_text_str, json);
        auto rs_json = sl::json::value(std::move(rs));
        auto span = wilton::support::make_json_buffer(rs_json);
        *result_set_out = span.value().data();
        *result_set_len_out = static_cast<int>(span.value().size());
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}

char* wilton_DBConnection_execute(
        wilton_DBConnection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len) {
    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
    if (nullptr == sql_text) return wilton::support::alloc_copy(TRACEMSG("Null 'sql_text' parameter specified"));
    if (!sl::support::is_uint32_positive(sql_text_len)) return wilton::support::alloc_copy(TRACEMSG(
            "Invalid 'sql_text_len' parameter specified: [" + sl::support::to_string(sql_text_len) + "]"));
    if (nullptr == params_json) return wilton::support::alloc_copy(TRACEMSG("Null 'params_json' parameter specified"));
    if (!sl::support::is_uint32(params_json_len)) return wilton::support::alloc_copy(TRACEMSG(
            "Invalid 'params_json_len' parameter specified: [" + sl::support::to_string(params_json_len) + "]"));
    try {
        uint32_t sql_text_len_u32 = static_cast<uint32_t> (sql_text_len);
        std::string sql_text_str{sql_text, sql_text_len_u32};
        auto json = params_json_len > 0 ? sl::json::load({params_json, params_json_len}) : sl::json::value();
        conn->impl().execute(sql_text_str, json);
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }    
}

char* wilton_DBConnection_close(
        wilton_DBConnection* conn) {
    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
    try {
        delete conn;
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    } 
}

char* wilton_DBTransaction_start(
        wilton_DBConnection* conn,
        wilton_DBTransaction** tran_out) {
    if (nullptr == conn) return wilton::support::alloc_copy(TRACEMSG("Null 'conn' parameter specified"));
    if (nullptr == tran_out) return wilton::support::alloc_copy(TRACEMSG("Null 'tran_out' parameter specified"));
    try {
        sl::orm::transaction tran = conn->impl().start_transaction();
        wilton_DBTransaction* tran_ptr = new wilton_DBTransaction(std::move(tran));
        *tran_out = tran_ptr;
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}

char* wilton_DBTransaction_commit(
        wilton_DBTransaction* tran) {
    if (nullptr == tran) return wilton::support::alloc_copy(TRACEMSG("Null 'tran' parameter specified"));
    try {
        tran->impl().commit();
        delete tran;
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}

char* wilton_DBTransaction_rollback(
        wilton_DBTransaction* tran) {
    if (nullptr == tran) return wilton::support::alloc_copy(TRACEMSG("Null 'tran' parameter specified"));
    try {
        delete tran;
        return nullptr;
    } catch (const std::exception& e) {
        return wilton::support::alloc_copy(TRACEMSG(e.what() + "\nException raised"));
    }
}
