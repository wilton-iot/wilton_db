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

struct wilton_PGConnection;
typedef struct wilton_PGConnection wilton_PGConnection;

char* wilton_PGConnection_open(wilton_PGConnection** conn_out,
        const char* conn_url,
        int conn_url_len,
        int is_ping_on);

char* wilton_PGConnection_execute_sql(wilton_PGConnection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len,
        int cache_flag,
        char** result_set_out,
        int* result_set_len_out);

char* wilton_PGConnection_close(
        wilton_PGConnection* conn);

char* wilton_PGConnection_transaction_begin(
        wilton_PGConnection* conn);

char* wilton_PGConnection_transaction_commit(
        wilton_PGConnection* conn);

char* wilton_PGConnection_transaction_rollback(
        wilton_PGConnection* conn);


#ifdef __cplusplus
}
#endif

#endif /* WILTON_DB_PSQL_H */

