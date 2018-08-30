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

#include <string>
#include <vector>
#include <sstream>
#include <typeinfo>
#include <unordered_map>

#include <libpq-fe.h>

#include "staticlib/io.hpp"
#include "staticlib/pimpl.hpp"
#include "staticlib/json.hpp"
#include "staticlib/utils/random_string_generator.hpp"

namespace wilton{
namespace db{
namespace pgsql{

struct parameters_values {
    std::string parameter_name;
    std::string value;
    Oid type;
    int len = 0;
    int format = 0; // text format

    parameters_values(std::string name, std::string val, Oid type, int len, int format) :
    parameter_name(name),
    value(val),
    type(type),
    len(len),
    format(format) { }
};

struct column_property {
    std::string name;
    Oid type_id;
    std::string value;

    column_property(std::string in_name, Oid in_type_id, std::string in_value) :
    name(in_name),
    type_id(in_type_id),
    value(in_value) { }
};

class row {
    // properties
    std::vector<column_property> properties;
    void add_column_property(std::string in_name, Oid in_type_id, std::string in_value);
    std::string get_value_as_string(size_t value_pos);

public:
    ~row();

    row(PGresult *res, int row_pos);

    sl::json::value dump_to_json();
};

class psql_handler : public sl::pimpl::object {
protected:
    /**
     * implementation class
     */
    class impl;

//    PGconn *conn;
//    PGresult *res;
//    std::string connection_parameters;
//    std::string last_error;
//    std::map<std::string, std::vector<std::string>> prepared_names;
//    std::unordered_map<std::string, std::string> queries_cache;
//    bool ping_on;
//    sl::utils::random_string_generator names_generator;

//    std::string generate_unique_name();

//    bool handle_result(PGconn *conn, PGresult *res, const std::string& error_message);

//    void prepare_params(std::vector<Oid>& types,
//            std::vector<const char *> &values,
//            std::vector<int>& length,
//            std::vector<int>& formats,
//            std::vector<parameters_values>& vals,
//            const std::vector<std::string>& names);

//    std::string parse_query(const std::string& sql_query, std::vector<std::string>& last_prepared_names);

//    void clear_result();

//    size_t sql_cached(const std::string &sql);

//    void cache_sql(const std::string& sql, const std::string& name);

//    staticlib::json::value execute_prepared_with_parameters(const std::string& prepared_name, const staticlib::json::value &parameters);

//    staticlib::json::value execute_sql_with_parameters(const std::string& sql_statement, const staticlib::json::value &parameters);

//    void deallocate_prepared_statement(const std::string& statement_name);

//    sl::json::value prepare_cached(const std::string& sql_query, std::string& query_name);

//    sl::json::value get_command_status_as_json(PGresult *result);

public:
    /**
     * PIMPL-specific constructor
     *
     * @param pimpl impl object
     */
    PIMPL_CONSTRUCTOR(psql_handler)

    psql_handler(const std::string& conn_params, bool is_ping_on);

    ~psql_handler();

    // Need to define move constructor because we use raw pointers to incomplete structures PGconn and PGresult
//    psql_handler(psql_handler&& handler);

    void setup_connection_params(const std::string& conn_params);

    bool connect();

//    void close();

//    staticlib::json::value execute_hardcode_statement(PGconn *conn, const std::string& query, const std::string& error_message);

    void begin();

    void commit();

    void rollback();

    staticlib::json::value execute_with_parameters(const std::string& sql_statement, const staticlib::json::value& parameters, bool cache_flag);

    std::string get_last_error();

//    sl::json::value get_execution_result(const std::string& error_msg);

//    void prepare_query();
};

} // pgsql
} // db
} // wilton

#endif /* PSQL_FUNCTIONS_H */
