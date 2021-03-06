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

#ifndef PSQL_FUNCTIONS_HPP
#define PSQL_FUNCTIONS_HPP

#include <string>
#include <vector>
#include <sstream>
#include <typeinfo>
#include <unordered_map>

#include <libpq-fe.h>

#include "staticlib/pimpl.hpp"
#include <staticlib/json.hpp>
#include <staticlib/utils/random_string_generator.hpp>

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
    sl::json::value get_value_as_json(size_t value_pos);

public:
    ~row();

    row(PGresult *res, int row_pos);
    sl::json::value dump_to_json();
};

class psql_handler : public sl::pimpl::object  {
protected:
    /**
     * implementation class
     */
    class impl;

public:
    PIMPL_CONSTRUCTOR(psql_handler)

    psql_handler(const std::string& conn_params);

    void setup_connection_params(const std::string& conn_params);

    bool connect();

    void begin();

    void commit();

    void rollback();

    staticlib::json::value execute_with_parameters(const std::string& sql_statement, const staticlib::json::value& parameters, int cache_flag);

    std::string get_last_error();
};

} // pgsql
} // db
} // wilton

#endif /* PSQL_FUNCTIONS_HPP */
