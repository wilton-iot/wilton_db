/*
 * Copyright 2017, alex at staticlibs.net
 * Copyright 2018, mike at myasnikov.mike@gmail.com
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

#ifndef WILTON_DB_PSQL_H
#define WILTON_DB_PSQL_H

#include "wilton/wilton.h"

#ifdef __cplusplus
extern "C" {
#endif

struct wilton_db_psql_connection;
typedef struct wilton_db_psql_connection wilton_db_psql_connection;

char* wilton_db_psql_connection_open(wilton_db_psql_connection** conn_out,
        const char* conn_url,
        int conn_url_len,
        bool is_ping_on);

char* wilton_db_psql_connection_prepare(
        wilton_db_psql_connection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* prepare_name,
        int prepare_name_len,
        char** result_set_out,
        int* result_set_len_out);

char* wilton_db_psql_connection_get_prepare_info(
        wilton_db_psql_connection* conn,
        const char* prepare_name,
        int prepare_name_len,
        char** result_set_out,
        int* result_set_len_out);

char* wilton_db_psql_connection_execute_prepared(
        wilton_db_psql_connection* conn,
        const char* prepare_name,
        int prepare_name_len,
        char** result_set_out,
        int* result_set_len_out);

char* wilton_db_psql_connection_execute_prepared_with_parameters(
        wilton_db_psql_connection* conn,
        const char* prepare_name,
        int prepare_name_len,
        const char* params_json,
        int params_json_len,
        char** result_set_out,
        int* result_set_len_out);

char* wilton_db_psql_connection_execute_sql(
        wilton_db_psql_connection* conn,
        const char* sql_text,
        int sql_text_len,
        char** result_set_out,
        int* result_set_len_out);

char* wilton_db_psql_connection_execute_sql_with_parameters(
        wilton_db_psql_connection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len,
        char** result_set_out,
        int* result_set_len_out);

char* wilton_db_psql_connection_close(
        wilton_db_psql_connection* conn);

char* wilton_db_psql_transaction_begin(
        wilton_db_psql_connection* conn);

char* wilton_db_psql_transaction_commit(
        wilton_db_psql_connection* conn);

char* wilton_db_psql_transaction_rollback(
        wilton_db_psql_connection* conn);

char* wilton_db_psql_connection_deallocate_prepared(
        wilton_db_psql_connection* conn,
        const char* prepared_name,
        int prepared_name_len);

#ifdef __cplusplus
}
#endif

#endif /* WILTON_DB_PSQL_H */

