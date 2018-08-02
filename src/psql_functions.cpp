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
#include "staticlib/support/to_string.hpp"
#include "psql_functions.h"


#include <algorithm>    // std::sort
#include <stdlib.h>

namespace {

// PostgreSQL types
#define _INT4OID  23
#define _INT8OID  20
#define _JSONOID  114
#define _BOOLOID  16
#define _TEXTOID  25
#define _INT2ARRAYOID  1005
#define _INT4ARRAYOID  1007
#define _VARCHAROID  1043
#define _FLOAT4OID  700
#define _FLOAT8OID  701
#define _UNKNOWNOID  705

void setup_params_from_json_field(
        std::vector<parameters_values>& vals,
        const  staticlib::json::field& fi) {
    std::string value{""};
    Oid type = _UNKNOWNOID;
    int len = 0;
    int format = 0; // text format

    value = fi.val().as_string();

    switch (fi.json_type()) {
    case sl::json::type::nullt:
        break;
    case sl::json::type::array:{
        // TODO need to determine array type
        type = _INT4ARRAYOID;
        value = fi.val().dumps();

        const int open_brace_pos = 0;
        const int close_brace_pos = value.size()-1;
        const int replace_symbol_amount = 1;

        value.replace(open_brace_pos, replace_symbol_amount, "{");
        value.replace(close_brace_pos, replace_symbol_amount, "}");
        break;
    }
    case sl::json::type::object: {
        type = _JSONOID;
        value = fi.val().dumps();
        while (std::string::npos != value.find("\n")) {
            value.replace(value.find("\n"),1,"");
        }
        break;
    }
    case sl::json::type::boolean:{
        type = _BOOLOID;
        if (fi.val().as_bool()) {
            value = "TRUE";
        } else {
            value = "FALSE";
        }
        break;
    }
    case sl::json::type::string:{
        // TODO need to check for reserved words
        type = _TEXTOID;
        value = fi.val().as_string();
        break;
    }
    case sl::json::type::integer:{
        type = _INT8OID;
        value = sl::support::to_string(fi.val().as_int64());
        break;
    }
    case sl::json::type::real:{
        type = _FLOAT8OID;
        value = sl::support::to_string(fi.val().as_float());
        break;
    }
    default:
        throw sl::support::exception("param parse error");
    }

    len = value.length();
    vals.emplace_back(fi.name(), value, type, len, format);
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
        : conn(nullptr), res(nullptr), connection_parameters(conn_params), prepared_statement_name("")  {}

psql_handler::psql_handler()
        : conn(nullptr), res(nullptr), connection_parameters(""), prepared_statement_name("") {}

void psql_handler::setup_connection_params(const std::string &conn_params) {
    this->connection_parameters = conn_params;
}

bool psql_handler::connect() {
    /* Make a connection to the database */
    conn = PQconnectdb(connection_parameters.c_str());

    /* Check to see that the backend connection was successfully made */
    if (CONNECTION_OK != PQstatus(conn)) {
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
        if (CONNECTION_BAD == PQstatus(conn))
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

void psql_handler::prepare_params(
        std::vector<Oid>& types,
        std::vector<std::string>& values,
        std::vector<int>& length,
        std::vector<int>& formats,
        std::vector<parameters_values>& vals,
        const std::vector<std::string>& names)
{
    // if names presents - sort by names, else sort by $# numbers
    if (names.size()) {
        for (auto& name: names) {
            for (auto& val : vals) {
                if (!name.compare(val.parameter_name)){
                    values.push_back(val.value);
                    types.push_back(val.type);
                    length.push_back(val.len);
                    formats.push_back(val.format); // text_format
                    break;
                }
            }
        }
    } else {
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
    std::string query = "DEALLOCATE " + statement_name + ";";
    execute_hardcode_statement(conn, query.c_str(), "Cannot deallocate prepared statement.");
    prepared_statement_name.clear();
}


std::string psql_handler::parse_query(const std::string& sql_query, std::vector<std::string>& names){
    enum { normal, in_quotes, in_name } state = normal;

    std::string name;
    std::string query;
    int position = 1;
    names.clear();

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

    return query;
}

std::string psql_handler::prepare(const std::string &sql_query, const std::string &tmp_statement_name){

    std::string query = parse_query(sql_query, last_prepared_names);

    if (!prepared_statement_name.empty()) {
        deallocate_prepared_statement(prepared_statement_name);
    }

    // default multi-row query execution
    res = PQprepare(conn, tmp_statement_name.c_str(), query.c_str(), static_cast<int>(last_prepared_names.size()), NULL);
    handle_result(conn, res, "PQprepare error");

    prepared_statement_name = tmp_statement_name;

    return std::string{};
}
std::string psql_handler::get_prepared_info(const std::string &prepared_name){
    res = PQdescribePrepared(conn, prepared_name.c_str());
    ExecStatusType const status = PQresultStatus(res);
    if (PGRES_COMMAND_OK == status) {
        // Собираем ответ сами и возвращаем:
        std::string info{"{"};
        info += "\"params_count\": ";
        info += sl::support::to_string(PQnparams(res));
        for (int i = 0; i < PQnparams(res); ++i) {
            info += ", \"$";
            info += sl::support::to_string(i+1);
            info += "\" : ";
            info += sl::support::to_string(PQparamtype(res, i));
        }
        info += "}";
        return info;
    }
    return get_execution_result("get_prepared_info error");
}

std::string psql_handler::execute_prepared(const std::string& prepared_name){
    res = PQexecPrepared(conn, prepared_name.c_str(), 0, nullptr, NULL, NULL, 0);
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

    setup_params_from_json(vals, parameters);
    prepare_params(params_types, params_values, params_length, params_formats, vals, last_prepared_names);

    std::vector<const char*> values_ptr;
    for (auto& el: params_values) {
        values_ptr.push_back(el.c_str());
    }
    params_count = params_types.size();

    res = PQexecPrepared(conn, prepared_name.c_str(),
                       params_count,
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

    std::vector<std::string> names;
    std::string query = parse_query(sql_statement, names);

    setup_params_from_json(vals, parameters);
    prepare_params(params_types, params_values, params_length, params_formats, vals, names);

    std::vector<const char*> values_ptr;
    for (auto& el: params_values) {
        values_ptr.push_back(el.c_str());
    }
    params_count = params_types.size();

    res = PQexecParams(conn, query.c_str(),
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
        return get_json_result(res);
    }
    return std::string{};
}

//////////// ROW CLASS
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
std::string row::get_value_as_string(int value_pos){ // converts
    std::string val{};
    switch(properties[value_pos].type_id){
    case _INT4OID:   { val = sl::support::to_string(get_value<int32_t>(value_pos)); break; }
    case _INT8OID:   { val = sl::support::to_string(get_value<int64_t>(value_pos)); break; }
    case _JSONOID:
    case _BOOLOID:
    case _TEXTOID:
    case _INT2ARRAYOID:
    case _INT4ARRAYOID:
    case _VARCHAROID: { val = get_value<std::string>(value_pos); break; }
    case _FLOAT4OID:
    case _FLOAT8OID: { val = sl::support::to_string(get_value<double>(value_pos)); break; }
    default:   { val = "";}
    }
    return val;
}

void row::setup_value_from_string(const std::string &str_value, int value_pos){
    Oid oid_type = properties[value_pos].type_id;
    switch(oid_type){
    case _INT4OID: {
        int32_t* val = new int32_t(std::atoi(str_value.c_str()));
        add_value<int32_t>(val);
        break;
    }
    case _INT8OID: {
        int64_t* val = new int64_t(std::atol(str_value.c_str()));
        add_value<int64_t>(val);
        break;
    }
    case _TEXTOID:
    case _VARCHAROID: {
        std::string* str = new std::string("\"" + str_value + "\"");
        add_value<std::string>(str);
        break;
    }
    case _JSONOID: {
        std::string* str = new std::string(str_value);
        add_value<std::string>(str);
        break;
    }
    case _INT2ARRAYOID:
    case _INT4ARRAYOID:{
        // need raplace psql array braces {} to json array braces []
        std::string tmp(str_value);
        const int open_brace_pos = 0;
        const int close_brace_pos = tmp.size()-1;
        const int replace_symbol_amount = 1;
        tmp.replace(open_brace_pos, replace_symbol_amount, "[");
        tmp.replace(close_brace_pos, replace_symbol_amount, "]");
        std::string* str = new std::string(tmp);
        add_value<std::string>(str);
        break;
    }
    case _BOOLOID: {
        std::string* str = nullptr;
        if ("t" == str_value) { // "t" - true sign for boolean type from PGresult 
            str = new std::string{"true"};
        } else {
            str = new std::string{"false"};
        }
        add_value<std::string>(str);
        break;
    }
    case _FLOAT4OID:
    case _FLOAT8OID: {
        double* val = new double(std::atol(str_value.c_str()));
        add_value<double>(val);
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
