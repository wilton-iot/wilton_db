# wilton_db module

## PostgreSQL functions

All functions may throw exceptions.

| function | description |
| --- | --- |
| db_pgsql_connection_open(**string**)                                                                     | Connect to database. **string** - connection string parameters. Returns stringifyed json, containing **connectionHandle** |
| db_pgsql_connection_close(**json{connectionHandle}**)                                                    | Close connection to database. Requires json with connectionHandle parameter with connectionHandle value from db_pgsql_connection_open |
| db_pgsql_connection_prepare(**json{connectionHandle, sql, name}**)                                       | Parse and prepare **sql** query under specified **name**. If **sql** contains named parameters like ":foo", this names stay in memory until next prepare function will be called and this names will be replaced by new, from that point you shoud use $# names for parameters. To get info about param types use **db_pgsql_connection_get_prepare_info**|
| db_pgsql_connection_get_prepare_info(**json{connectionHandle, name}**)                                   | Returns parameters info of prepared sql, selected by **name**. |
| db_pgsql_connection_deallocate_prepared(**json{connectionHandle, name}**)                                | Deallocates prepared **sql** query. |
| db_pgsql_connection_execute_prepared(**json{connectionHandle, name}**)                                   | Execute prepared query **name** without parameters |
| db_pgsql_connection_execute_prepared_with_parameters(**json{connectionHandle, name, json{parameters}}**) | Execute prepared query **name** with parameters as {param_name:value,..} or {$1:value, ..}|
| db_pgsql_connection_execute_sql(**json{connectionHandle, sql}**)                                         | Execute **sql** without parameters. |
| db_pgsql_connection_execute_sql_with_parameters(**json{connectionHandle, sql, json{parameters}}**)       | Execute **sql** with parameters as {param_name:value,..} or {$1:value, ..} |
| db_pgsql_transaction_begin(**json{connectionHandle}**)                                                   | Starts transaction, shortcut to BEGIN query |
| db_pgsql_transaction_commit(**json{connectionHandle}**)                                                  | Commits transaction, shortcut to COMMIT query |
| db_pgsql_transaction_rollback(**json{connectionHandle}**)                                                | Rollback transaction, shortcut to ROLLBACK query |
