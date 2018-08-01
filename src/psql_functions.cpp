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

#include "staticlib/support/exception.hpp"
#include "psql_functions.h"

// PostgreSQL types support
#include <c.h>
#include <catalog/pg_type.h>

#include <algorithm>    // std::sort
#include <stdlib.h>

namespace {

void setup_params_from_json_field(
        std::vector<parameters_values>& vals,
        const  staticlib::json::field& fi) {
    std::string value{""};
    Oid type = UNKNOWNOID;
    int len = 0;
    int format = 0; // text format

    value = fi.val().as_string();

    switch (fi.json_type()) {
    case sl::json::type::nullt:
        break;
    case sl::json::type::array:
        // TODO need to determine array type
        type = INT4ARRAYOID;
        value = fi.val().dumps();
        value.replace(0,1,"{");
        value.replace(value.size()-1,1,"}");
        break;
    case sl::json::type::object:
        type = JSONOID;
        value = fi.val().dumps();
        while (std::string::npos != value.find("\n")) {
            value.replace(value.find("\n"),1,"");
        }
        break;
    case sl::json::type::boolean:
        type = BOOLOID;
        if (fi.val().as_bool()) {
            value = "TRUE";
        } else {
            value = "FALSE";
        }
        break;
    case sl::json::type::string:
        // TODO need to check for reserved words
        type = TEXTOID;
        value = fi.val().as_string();
        break;
    case sl::json::type::integer:
        type = INT8OID;
        value = std::to_string(fi.val().as_int64());
        break;
    case sl::json::type::real:
        type = FLOAT8OID;
        value = std::to_string(fi.val().as_float());
        break;
    default:
        throw sl::support::exception("param parse error");
    }

    len = value.length();
    vals.emplace_back(fi.name(), value, type, len, format);
}

void prepare_params(
        std::vector<Oid>& types,
        std::vector<std::string>& values,
        std::vector<int>& length,
        std::vector<int>& formats,
        std::vector<parameters_values>& vals) {

    // нужно отсортировать массив.
    auto compare = [] (parameters_values& a, parameters_values& b) -> bool {
        int a_pos = 0;
        int b_pos = 0;

        a_pos = std::atoi(a.parameter_name.substr(1).c_str());
        b_pos = std::atoi(b.parameter_name.substr(1).c_str());

        return (a_pos < b_pos);
    };
    std::sort(vals.begin(), vals.end(), compare);

    for (auto& el : vals) {
        values.push_back(el.value);
        types.push_back(el.type);
        length.push_back(el.len);
        formats.push_back(el.format); // text_format
    }
}

void setup_params_from_json(
        std::vector<parameters_values>& vals,
        const staticlib::json::value& parameters) {
    switch (parameters.json_type()) {
    case sl::json::type::object:
        for (const sl::json::field& fi : parameters.as_object()) {
            setup_params_from_json_field(vals, fi);
        }
        break;
    case sl::json::type::array:
        // support only objects
        break;
    case sl::json::type::nullt:
        // support only objects
        break;
    default:
        // support only objects
        break;
    }
}

std::string get_json_result(PGresult *res){
    std::string json{"["};
    int touples_count = PQntuples(res);

    for (int i = 0; i < touples_count; ++i){
        row tmp_row(res, i);
        json += tmp_row.dump_to_json_string();
        json += ", ";
    }
    if (touples_count) {
        json.pop_back();
        json.pop_back();
    }
    json += "]";
    return json;
}

} // namespace

psql_handler::psql_handler(const std::string &conn_params)
        : conn(nullptr), res(nullptr), connection_parameters(conn_params), sql_state("")  {}

psql_handler::psql_handler()
        : conn(nullptr), res(nullptr), connection_parameters(""), sql_state("") {}

void psql_handler::setup_connection_params(const std::string &conn_params) {
    this->connection_parameters = conn_params;
}

bool psql_handler::connect() {
    /* Make a connection to the database */
    conn = PQconnectdb(connection_parameters.c_str());

    /* Check to see that the backend connection was successfully made */
    if (PQstatus(conn) != CONNECTION_OK) {
        last_error = "Connection to database failed: " + std::string(PQerrorMessage(conn));
        PQfinish(conn);
        return false;
    }
    return true;
}
void psql_handler::close(){
    if (nullptr != conn) {
        PQfinish(conn);
        conn = nullptr;
    }
}
bool psql_handler::handle_result(PGconn *conn, PGresult *res, const std::string &error_message){
    std::string msg(error_message);

    ExecStatusType const status = PQresultStatus(res);
    switch (status)
    {
    case PGRES_EMPTY_QUERY:
    case PGRES_COMMAND_OK:
        // No data but don't throw neither.
        return false;

    case PGRES_TUPLES_OK:
        return true;

    case PGRES_FATAL_ERROR:
        msg += " Fatal error.";
        if (PQstatus(conn) == CONNECTION_BAD)
        {
            msg += " Connection failed.";
            // TODO maybe add user callback call
        }
        break;

    default:
        // Some of the other status codes are not really errors but we're
        // not prepared to handle them right now and shouldn't ever receive
        // them so throw nevertheless
        break;
    }

    const char* const pqError = PQresultErrorMessage(res);
    if (pqError && *pqError)
    {
        msg += " ";
        msg += pqError;
    }

    const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    const char* const blank_sql_state = "     ";
    if (!sqlstate)
    {
        sqlstate = blank_sql_state;
    }

    throw sl::support::exception(msg);
}
bool psql_handler::execute_hardcode_statement(PGconn *conn, const std::string &query, const std::string &error_message){
    res = PQexec(conn, query.c_str());
    return handle_result(conn, res, error_message);
}

void psql_handler::begin()
{
    execute_hardcode_statement(conn, "BEGIN", "Cannot begin transaction.");
}
void psql_handler::commit()
{
    execute_hardcode_statement(conn, "COMMIT", "Cannot commit transaction.");
}
void psql_handler::rollback()
{
    execute_hardcode_statement(conn, "ROLLBACK", "Cannot rollback transaction.");
}
void psql_handler::deallocate_prepared_statement(const std::string &statement_name) {
    const std::string& query = "DEALLOCATE " + statement_name;
    execute_hardcode_statement(conn, query.c_str(), "Cannot deallocate prepared statement.");
}

std::string psql_handler::prepare(const std::string &sql_query, const std::string &tmp_statement_name){
    enum { normal, in_quotes, in_name } state = normal;

    std::string name;
    int position = 1;

    for (std::string::const_iterator it = sql_query.begin(), end = sql_query.end();
         it != end; ++it)
    {
        switch (state)
        {
        case normal:
            if (*it == '\'')
            {
                query += *it;
                state = in_quotes;
            }
            else if (*it == ':')
            {
                // Check whether this is a cast operator (e.g. 23::float)
                // and treat it as a special case, not as a named binding
                const std::string::const_iterator next_it = it + 1;
                if ((next_it != end) && (*next_it == ':'))
                {
                    query += "::";
                    ++it;
                }
                // Check whether this is an assignment(e.g. x:=y)
                // and treat it as a special case, not as a named binding
                else if ((next_it != end) && (*next_it == '='))
                {
                    query += ":=";
                    ++it;
                }
                else
                {
                    state = in_name;
                }
            }
            else // regular character, stay in the same state
            {
                query += *it;
            }
            break;
        case in_quotes:
            if (*it == '\'')
            {
                query += *it;
                state = normal;
            }
            else // regular quoted character
            {
                query += *it;
            }
            break;
        case in_name:
            if (std::isalnum(*it) || *it == '_')
            {
                name += *it;
            }
            else // end of name
            {
                names.push_back(name);
                name.clear();
                std::stringstream ss;
                ss << '$' << position++;
                query += ss.str();
                query += *it;
                state = normal;

                // Check whether the named parameter is immediatelly
                // followed by a cast operator (e.g. :name::float)
                // and handle the additional colon immediately to avoid
                // its misinterpretation later on.
                if (*it == ':')
                {
                    const std::string::const_iterator next_it = it + 1;
                    if ((next_it != end) && (*next_it == ':'))
                    {
                        query += ':';
                        ++it;
                    }
                }
            }
            break;
        }
    }

    if (state == in_name)
    {
        names.push_back(name);
        std::ostringstream ss;
        ss << '$' << position++;
        query += ss.str();
    }

    // default multi-row query execution
    res = PQprepare(conn, tmp_statement_name.c_str(), query.c_str(), static_cast<int>(names.size()), NULL);
    try {
        handle_result(conn, res, "PQprepare error");
    } catch (const sl::support::exception& e) {
        deallocate_prepared_statement(tmp_statement_name);
        throw e;
    }

    return std::string{};
}

std::string psql_handler::get_prepared_info(const std::string &prepared_name){
    res = PQdescribePrepared(conn, prepared_name.c_str());
    ExecStatusType const status = PQresultStatus(res);
    if (PGRES_COMMAND_OK == status) {
        // Собираем ответ сами и возвращаем:
        std::string info{"{"};
        info += "\"params_count\": ";
        info += std::to_string(PQnparams(res));
        for (int i = 0; i < PQnparams(res); ++i) {
            info += ", \"$";
            info += std::to_string(i+1);
            info += "\" : ";
            info += std::to_string(PQparamtype(res, i));
        }
        info += "}";
        return info;
    }
    return get_execution_result("get_prepared_info error");
}

std::string psql_handler::execute_prepared(const std::string& prepared_name){
    if (names.size()){
        auto ptr = const_cast<const char* const>(names[0].c_str());
        res = PQexecPrepared(conn, prepared_name.c_str(),
                             static_cast<int>(names.size()),
                             &ptr, NULL, NULL, 0);
    } else {
        res = PQexecPrepared(conn, prepared_name.c_str(),
                             0, nullptr, NULL, NULL, 0);
    }

    return get_execution_result("PQexecPrepared error");
}
std::string psql_handler::execute_sql_statement(const std::string &sql_statement){
    res = PQexec(conn, sql_statement.c_str());
    return get_execution_result("PQexec error");
}

std::string psql_handler::execute_prepared_with_parameters(
        const std::string &prepared_name, const staticlib::json::value &parameters){
    int params_count = 0;
    std::vector<Oid> params_types;
    std::vector<std::string> params_values;
    std::vector<int> params_length;
    std::vector<int> params_formats;
    const int text_format = 0;

    std::vector<parameters_values> vals;

    // нужно разобрать параметры, наверное как в staticlib_orm
    setup_params_from_json(vals, parameters);
    prepare_params(params_types, params_values, params_length, params_formats, vals);

    std::vector<const char*> values_ptr;
    for (auto& el: params_values) {
        values_ptr.push_back(el.c_str());
    }
    params_count = params_types.size();

    res = PQexecPrepared(conn, prepared_name.c_str(),
                       params_count, /*params_types.data(),*/
                       const_cast<const char* const*>(values_ptr.data()),
                       const_cast<const int*>(params_length.data()),
                       const_cast<const int*>(params_formats.data()),
                       text_format);

    return get_execution_result("PQexecParams error");
}

std::string psql_handler::execute_sql_with_parameters(
        const std::string &sql_statement, const staticlib::json::value& parameters) {
    int params_count = 0;
    std::vector<Oid> params_types;
    std::vector<std::string> params_values;
    std::vector<int> params_length;
    std::vector<int> params_formats;
    const int text_format = 0;

    std::vector<parameters_values> vals;

    // нужно разобрать параметры, наверное как в staticlib_orm
    setup_params_from_json(vals, parameters);
    prepare_params(params_types, params_values, params_length, params_formats, vals);

    std::vector<const char*> values_ptr;
    for (auto& el: params_values) {
        values_ptr.push_back(el.c_str());
    }
    params_count = params_types.size();

    res = PQexecParams(conn, sql_statement.c_str(),
                       params_count, params_types.data(),
                       const_cast<const char* const*>(values_ptr.data()),
                       const_cast<const int*>(params_length.data()),
                       const_cast<const int*>(params_formats.data()),
                       text_format);

    return get_execution_result("PQexecParams error");
}

std::string psql_handler::get_last_error() {
    return last_error;
}

std::string psql_handler::get_execution_result(const std::string &error_msg){
    bool result = handle_result(conn, res, error_msg);
    if (result) {
        // нужно вытащить результаты в json виде
        // Пока просто сформируем строку
        return get_json_result(res);
    }
    return std::string{};
}

//////////// ROW CLASS
row::row() {}

row::~row() {
    for (auto& el : values) {
        delete el;
    }
}

row::row(PGresult *res, int row_pos) {
    int fields_count = PQnfields(res);
    for (int i = 0; i < fields_count; ++i) {
        add_column_property(PQfname(res, i), PQftype(res,i));
    }
    for (int i = 0; i < fields_count; ++i) {
        setup_value_from_string(PQgetvalue(res, row_pos, i), i);
    }
}

void row::add_column_property(std::string in_name, Oid int_type_id){
    properties.emplace_back(in_name, int_type_id);
}

std::string row::get_value_as_string(int value_pos){
    std::string val{};
    switch(properties[value_pos].type_id){
    case INT4OID:   { val = std::to_string(get_value<int32_t>(value_pos)); break; }
    case INT8OID:   { val = std::to_string(get_value<int64_t>(value_pos)); break; }
    case JSONOID:
    case BOOLOID:
    case TEXTOID:
    case INT2ARRAYOID:
    case INT4ARRAYOID:
    case VARCHAROID: { val = get_value<std::string>(value_pos); break; }
    default:   { val = "";}
    }
    return val;
}

void row::setup_value_from_string(const std::string &str_value, int value_pos){
    Oid oid_type = properties[value_pos].type_id;
    switch(oid_type){
    case INT4OID:   {
        int32_t* val = new int32_t(std::atoi(str_value.c_str()));
        add_value<int32_t>(val);
        break;
    }
    case INT8OID:   {
        int64_t* val = new int64_t(std::atol(str_value.c_str()));
        add_value<int64_t>(val);
        break;
    }
    case TEXTOID:
    case VARCHAROID: {
        std::string* str = new std::string("\"" + str_value + "\"");
        add_value<std::string>(str);
        break;
    }
    case JSONOID: {
        std::string* str = new std::string(str_value);
        add_value<std::string>(str);
        break;
    }
    case INT2ARRAYOID:
    case INT4ARRAYOID:{
        // нуно убрать {} и добавить []
        std::string tmp(str_value);
        tmp.replace(0,1,{'['});
        tmp.replace(tmp.size()-1,1,{']'});
        std::string* str = new std::string(tmp);
        add_value<std::string>(str);
        break;
    }
    case BOOLOID: {
        std::string* str = nullptr;
        if ("t" == str_value) {
            str = new std::string{"true"};
        } else {
            str = new std::string{"false"};
        }
        add_value<std::string>(str);
        break;
    }
    default:   {}
    }
}

std::string row::dump_to_json_string(){
    std::string json{"{"};
    for (size_t i = 0; i < properties.size(); ++i) {
        json += "\"";
        json += properties[i].name;
        json += "\":";
        json += get_value_as_string(i);
        json += ", ";
    }
    if (properties.size()) {
        json.pop_back();
        json.pop_back();
    }
    json += "}";
    return json;
}
