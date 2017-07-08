/* 
 * File:   wilton_db.h
 * Author: alex
 *
 * Created on June 10, 2017, 1:23 PM
 */

#ifndef WILTON_DB_H
#define	WILTON_DB_H

#include "wilton/wilton.h"

#if !defined(WILTON_DB_EXPORT) && defined(WILTON_SHARED)
#  if defined(WILTON_DB_SHARED_EXPORT) && defined(_WIN32)
#    define WILTON_DB_EXPORT __declspec(dllexport)
#  elif defined(_WIN32)
#    define WILTON_DB_EXPORT __declspec(dllimport)
#  elif defined(WILTON_DB_SHARED_EXPORT) 
#    define WILTON_DB_EXPORT __attribute__((visibility("default")))
#  else
#    define WILTON_DB_EXPORT
#  endif
#endif // !WILTON_DB_EXPORT && WILTON_SHARED

#ifdef __cplusplus
extern "C" {
#endif

struct wilton_DBConnection;
typedef struct wilton_DBConnection wilton_DBConnection;

struct wilton_DBTransaction;
typedef struct wilton_DBTransaction wilton_DBTransaction;

WILTON_DB_EXPORT char* wilton_DBConnection_open(
        wilton_DBConnection** conn_out,
        const char* conn_url,
        int conn_url_len);

WILTON_DB_EXPORT char* wilton_DBConnection_query(
        wilton_DBConnection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len,
        char** result_set_out,
        int* result_set_len_out);

WILTON_DB_EXPORT char* wilton_DBConnection_execute(
        wilton_DBConnection* conn,
        const char* sql_text,
        int sql_text_len,
        const char* params_json,
        int params_json_len);

WILTON_DB_EXPORT char* wilton_DBConnection_close(
        wilton_DBConnection* conn);

WILTON_DB_EXPORT char* wilton_DBTransaction_start(
        wilton_DBConnection* conn,
        wilton_DBTransaction** tran_out);

WILTON_DB_EXPORT char* wilton_DBTransaction_commit(
        wilton_DBTransaction* tran);

WILTON_DB_EXPORT char* wilton_DBTransaction_rollback(
        wilton_DBTransaction* tran);


#ifdef __cplusplus
}
#endif

#endif	/* WILTON_DB_H */

