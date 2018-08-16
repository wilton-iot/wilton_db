# wilton_db module

## PostgreSQL functions

All functions may throw exceptions.

| function | description |
| --- | --- |
| db_pgsql_connection_open(**json{ {string}parameters: **, {bool}ping_on:}**)                               | Connect to database. **parameters** - connection string parameters. Returns stringifyed json, containing **connectionHandle**, **ping_on** - optional, true by default. Enables ping database before each call. |
| db_pgsql_connection_close(**json{{uint_64}connectionHandle}**)                                            | Close connection to database. Requires json with connectionHandle parameter with connectionHandle value from db_pgsql_connection_open |
| db_pgsql_connection_execute_sql_with_parameters(**json{{uint_64}connectionHandle, {string}sql, json{parameters}, {bool}cache: }**) | Execute **sql** with parameters as {param_name:value,..} or {$1:value, ..}, **cache** - enables prepare/execute paradigm for sql query. True by default. |
| db_pgsql_transaction_begin(**json{connectionHandle}**)                                                   | Starts transaction, shortcut to BEGIN query |
| db_pgsql_transaction_commit(**json{connectionHandle}**)                                                  | Commits transaction, shortcut to COMMIT query |
| db_pgsql_transaction_rollback(**json{connectionHandle}**)                                                | Rollback transaction, shortcut to ROLLBACK query |

function **db_pgsql_connection_execute_sql_with_parameters** returns result as array of fields for queries, or json with commnd status:
```js
{
    "cmd_status": "INSERT 0 1"
}

```