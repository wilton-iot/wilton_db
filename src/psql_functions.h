/*
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

#ifndef PSQL_FUNCTIONS_H
#define PSQL_FUNCTIONS_H

#include <libpq-fe.h>
#include <string>
#include <vector>
#include <sstream>
#include <typeinfo>

#include <staticlib/json.hpp>

struct parameters_values {
    std::string parameter_name;
    std::string value;
    Oid type;
    int len = 0;
    int format = 0; // text format
    parameters_values(std::string name, std::string val, Oid type, int len, int format)
        : parameter_name(name), value(val), type(type), len(len), format(format) {}
};

struct column_property{
    std::string name;
    Oid type_id;
    std::string value;
    column_property(std::string in_name, Oid in_type_id, std::string in_value)
            : name(in_name), type_id(in_type_id), value(in_value) {}
};

class row
{
    // properties
    std::vector<column_property> properties;
    void add_column_property(std::string in_name, Oid in_type_id, std::string in_value);
    std::string get_value_as_string(int value_pos);
public:
    ~row();
    row(PGresult *res, int row_pos);
    std::string dump_to_json_string();
};

class psql_handler
{
    PGconn *conn;
    PGresult *res;
    std::string connection_parameters;
    std::string last_error;
    std::map<std::string, std::vector<std::string>> prepared_names;

    bool handle_result(PGconn *conn, PGresult *res, const std::string& error_message);
    void prepare_params(std::vector<Oid>& types,
            std::vector<std::string>& values,
            std::vector<int>& length,
            std::vector<int>& formats,
            std::vector<parameters_values>& vals,
            const std::vector<std::string>& names);
    std::string parse_query(const std::string &sql_query, std::vector<std::string> &last_prepared_names);
public:
    explicit psql_handler(const std::string& conn_params);
    psql_handler();

    void setup_connection_params(const std::string& conn_params);
    bool connect();
    void close();

    bool execute_hardcode_statement(PGconn *conn, const std::string& query, const std::string& error_message);

    void begin();
    void commit();
    void rollback();
    void deallocate_prepared_statement(const std::string& statement_name);

    std::string prepare(const std::string& sql_query, const std::string& tmp_statement_name);
    std::string get_prepared_info(const std::string &prepared_name);

    std::string execute_prepared(const std::string &prepared_name);
    std::string execute_sql_statement(const std::string& sql_statement);
    std::string execute_prepared_with_parameters(const std::string& prepared_name, const staticlib::json::value &parameters);
    std::string execute_sql_with_parameters(const std::string& sql_statement, const staticlib::json::value &parameters);

    std::string get_last_error();
    std::string get_execution_result(const std::string& error_msg);
};


#endif /* PSQL_FUNCTIONS_H */
