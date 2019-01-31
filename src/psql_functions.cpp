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

#include <cstdlib>
#include <algorithm>    // std::sort
#include <stack>
#include <set>

#include "wilton/support/exception.hpp"
#include "staticlib/support/to_string.hpp"
#include "staticlib/utils/random_string_generator.hpp"
#include "staticlib/pimpl/forward_macros.hpp"
#include "staticlib/utils.hpp"

#include "psql_functions.hpp"

// PostgreSQL types
#define PSQL_NULLOID  0
#define PSQL_INT2OID  21
#define PSQL_INT4OID  23
#define PSQL_INT8OID  20
#define PSQL_JSONOID  114
#define PSQL_JSONBOID  3802
#define PSQL_BOOLOID  16
#define PSQL_TEXTOID  25
#define PSQL_VARCHAROID  1043
#define PSQL_FLOAT4OID  700
#define PSQL_FLOAT8OID  701
#define PSQL_UNKNOWNOID  705
#define PSQL_INT2ARRAYOID 1005
#define PSQL_INT4ARRAYOID 1007
#define PSQL_INT8ARRAYOID 1016
#define PSQL_TEXTARRAYOID 1009
#define PSQL_CHARARRAYOID 1014
#define PSQL_VARCHARARRAYOID 1015
#define PSQL_BOOLARARRAYOID 1000
#define PSQL_FLOAT4ARRAYOID 1021
#define PSQL_FLOAT8ARRAYOID 1022


namespace wilton{
namespace db{
namespace pgsql{

#define PSQL_EXECUTE_WITH_PARAMS \
    res = PQexecParams(conn, query.c_str(), \
        params_count, params_types.data(), \
        const_cast<const char* const*>(params_values.data()), \
        const_cast<const int*>(params_length.data()), \
        const_cast<const int*>(params_formats.data()), \
        text_format);
#define PSQL_EXECUTE_PREPARED \
    res = PQexecPrepared(conn, prepared_name.c_str(), \
        params_count, \
        const_cast<const char* const*>(params_values.data()), \
        const_cast<const int*>(params_length.data()), \
        const_cast<const int*>(params_formats.data()), \
        text_format);

namespace { // anonymous

Oid get_json_array_type(const sl::json::value& json_value) {
    auto& array = json_value.as_array();
    Oid type = PSQL_INT4ARRAYOID;
    if (array.size()) {
        switch (array.begin()->json_type()) {
        case sl::json::type::string:{
            type = PSQL_TEXTARRAYOID;
            break;
        }
        case sl::json::type::real:{
            type = PSQL_FLOAT8ARRAYOID;
            break;
        }
        case sl::json::type::integer:
        default:
            break;
        }
    }
    return type;
}

parameters_values get_json_params_values(const sl::json::value& json_value){
    std::string value{};
    Oid type = PSQL_UNKNOWNOID;
    int len = 0;
    int format = 0; // text format

    switch (json_value.json_type()) {
    case sl::json::type::nullt:
        break;
    case sl::json::type::array:{
        type = get_json_array_type(json_value);
        value = json_value.dumps();

        const size_t open_brace_pos = 0;
        const size_t close_brace_pos = value.size()-1;
        const size_t replace_symbol_amount = 1;

        value.replace(open_brace_pos, replace_symbol_amount, "{");
        value.replace(close_brace_pos, replace_symbol_amount, "}");
        break;
    }
    case sl::json::type::object: {
        type = PSQL_JSONBOID;
        value = json_value.dumps();
        // TODO not need formatting if we use only jsonb
        while (std::string::npos != value.find("\n")) {
            value.replace(value.find("\n"),1,"");
        }
        break;
    }
    case sl::json::type::boolean:{
        type = PSQL_BOOLOID;
        if (json_value.as_bool()) {
            value = "TRUE";
        } else {
            value = "FALSE";
        }
        break;
    }
    case sl::json::type::string:{
        // TODO need to check for reserved words
        type = PSQL_TEXTOID;
        value = json_value.as_string();
        break;
    }
    case sl::json::type::integer:{
        type = PSQL_INT8OID;
        value = sl::support::to_string(json_value.as_int64());
        break;
    }
    case sl::json::type::real:{
        type = PSQL_FLOAT8OID;
        value = sl::support::to_string(json_value.as_float());
        break;
    }
    default:
        throw wilton::support::exception(TRACEMSG("param parse error"));
    }
    len = static_cast<int>(value.length());
    return parameters_values("", value, type, len, format);
}

void setup_params_from_json_array(
        std::vector<parameters_values>& vals,
        const staticlib::json::value& json_value,
        const std::vector<std::string>& names) {

    std::string name{};
    if (names.size()) {
        name = names[vals.size()];
    } else {
        name = "$" + sl::support::to_string(vals.size() + 1);
    }

    parameters_values pm_value = get_json_params_values(json_value);
    pm_value.parameter_name = name;

    vals.emplace_back(pm_value);
}

void setup_params_from_json_field(
        std::vector<parameters_values>& vals,
        const  staticlib::json::field& fi) {
    parameters_values pm_value = get_json_params_values(fi.val());
    pm_value.parameter_name = fi.name();

    vals.emplace_back(pm_value);
}

void setup_params_from_json(
        std::vector<parameters_values>& vals,
        const staticlib::json::value& parameters,
        const std::vector<std::string>& names) {
    switch (parameters.json_type()) {
    case sl::json::type::object:
        for (const sl::json::field& fi : parameters.as_object()) {
            setup_params_from_json_field(vals, fi);
        }
        break;
    case sl::json::type::array:
        for (const sl::json::value& val : parameters.as_array()) {
            setup_params_from_json_array(vals, val, names);
        }
        break;
    case sl::json::type::nullt:
        // not supported
        break;
    default:
        setup_params_from_json_array(vals, parameters, names);
        break;
    }
}

sl::json::value get_result_as_json(PGresult *res){
    sl::json::value json;
    std::vector<sl::json::value> json_array;
    int touples_count = PQntuples(res);

    for (int i = 0; i < touples_count; ++i){
        row tmp_row(res, i);
        json_array.emplace_back(tmp_row.dump_to_json());
    }
    json.set_array(std::move(json_array));
    return json;
}

std::set<size_t> check_poses(const std::string& str, const std::string& val){
    std::set<size_t> null_poses;
    size_t search_pos = 0;
    while (true){
        search_pos = str.find(val, search_pos);
        // Теперь надо проверить что у него там по краям
        size_t left = search_pos-1;
        size_t right = search_pos + 4;
        char ll = str[left];
        char rl = str[right];
        if (('[' == ll && ']' == rl) ||
            ('[' == str[left] && ',' == str[right]) ||
            (',' == str[left] && ']' == str[right]) ||
            (',' == str[left] && ',' == str[right])) {
            null_poses.insert(left + 1); //because insert shifts litera
            null_poses.insert(right);
        }
        if (std::string::npos == search_pos) break;
        search_pos++;
    }
    return null_poses;
}

std::set<size_t> check_null_poses(const std::string& val){
    std::set<size_t> null_poses;
    std::set<size_t> big_null_poses;
    null_poses = check_poses(val, "null");
    big_null_poses = check_poses(val, "NULL");
    null_poses.insert(big_null_poses.begin(),big_null_poses.end());
    return null_poses;
}

void replace_all_occurences(std::string& str, const std::string& subst, const std::string& replacer) {
    size_t search_pos = 0;
    while (true){
        search_pos = str.find(subst, search_pos);
        if (std::string::npos == search_pos) break;
        str.replace(search_pos, subst.size(), replacer);
        search_pos += replacer.size();
    }
}


void change_null_register(std::string& val) {
    std::string big_null = "NULL";
    std::string small_null = "null";
    replace_all_occurences(val, big_null, small_null);
}

void prepare_bool_array(std::string& val){
    replace_all_occurences(val, "t", "true");
    replace_all_occurences(val, "f", "false");
}

void prepare_text_array(std::string& val) {
    enum class states {
        normal, in_string, manual_open
    };

    states state = states::normal;
    states prev_state = states::normal;
    const size_t open_brace_pos = 0;
    const size_t close_brace_pos = val.size()-1;
    const size_t replace_symbol_amount = 1;
    val.replace(open_brace_pos, replace_symbol_amount, "[");
    val.replace(close_brace_pos, replace_symbol_amount, "]");
    const size_t emtpy_array_size = 2;
    if (emtpy_array_size >= val.size()) {
        return;
    }

    std::set<size_t> null_poses = check_null_poses(val);
    std::stack<size_t> inserted_poses;
    const size_t start_pos = 1;
    const size_t string_length = val.size();
    for (size_t i = start_pos; i < string_length; ++i) {
        char litera = val[i];
        switch(state){
        case states::normal:{
            if (litera == '"') {
                state = states::in_string;
            } else if (litera == ',' && prev_state != states::in_string) {
                inserted_poses.push(i);
                state = states::manual_open;
            } else if (litera == ']' && prev_state != states::in_string) {
                inserted_poses.push(i);
                state = states::manual_open;
            } else if (litera != '"' && litera != ']' && litera != ',') {
                inserted_poses.push(i);
                state = states::manual_open;
            }
            prev_state = states::normal;
            break;
        }
        case states::in_string:{
            if (litera == '"') {
                state = states::normal;
            } else if (litera == '\\') {
                i++;
            }
            prev_state = states::in_string;
            break;
        }
        case states::manual_open:{
            if (litera == ',' || litera == ']') {
                state = states::normal;
                inserted_poses.push(i);
            }
            prev_state = states::in_string;
            break;
        }
        }
    }

    while (inserted_poses.size()) {
        if (!null_poses.count(inserted_poses.top())) {
            val.insert(inserted_poses.top(), "\"");
        }
        inserted_poses.pop();
    }
}

sl::json::value prepare_json_array(std::string& val) {
    prepare_text_array(val);
    const size_t emtpy_array_size = 2;
    if (emtpy_array_size >= val.size()) {
        return sl::json::loads(val);
    }
    std::vector<sl::json::value> array;
    auto buffer = std::string{};
    enum class states {
        normal, in_string
    };
    states state = states::normal;
    const size_t start_pos = 1;
    const size_t string_length = val.size();
    char prev_litera = '[';
    for (size_t i = start_pos; i < string_length; ++i) {
        char litera = val[i];
        switch(state){
        case states::normal: {
            if ('"' == litera) {
                state = states::in_string;
            } else if (('N' == litera || 'n' == litera) && '"' != prev_litera) {
                array.emplace_back(nullptr); // if we get NULL value setup it as null for json
            }
            break;
        }
        case states::in_string:{
            if ('"' != litera || ('"' == litera && '\\' == prev_litera)) {
                buffer += litera;
            } else {
                state = states::normal;
                array.emplace_back(buffer);
                buffer.clear();
            }
            break;
        }
        }
        prev_litera = litera;
    }
    return sl::json::value(std::move(array));
}


} // namespace

//////////// ROW CLASS
row::~row() {}
row::row(PGresult *res, int row_pos) {
    int fields_count = PQnfields(res);
    for (int i = 0; i < fields_count; ++i) {
        Oid type = PSQL_NULLOID;
        if (!PQgetisnull(res, row_pos, i)) {
            type = PQftype(res,i);
        }
        add_column_property(PQfname(res, i), type, PQgetvalue(res, row_pos, i));
    }
}

void row::add_column_property(std::string in_name, Oid in_type_id, std::string in_value){
    properties.emplace_back(in_name, in_type_id, in_value);
}

staticlib::json::value row::get_value_as_json(size_t value_pos) {
    std::string val{properties[value_pos].value};
    sl::json::value js_val;
    switch(properties[value_pos].type_id){
    case PSQL_CHARARRAYOID:
    case PSQL_VARCHARARRAYOID:
    case PSQL_TEXTARRAYOID: {
        return prepare_json_array(val);
        break;
    }
    case PSQL_BOOLARARRAYOID: {
        prepare_bool_array(val); // no break
    }
    case PSQL_FLOAT4ARRAYOID:
    case PSQL_FLOAT8ARRAYOID:
    case PSQL_INT2ARRAYOID:
    case PSQL_INT4ARRAYOID:
    case PSQL_INT8ARRAYOID:{
        const size_t open_brace_pos = 0;
        const size_t close_brace_pos = val.size()-1;
        const size_t replace_symbol_amount = 1;
        val.replace(open_brace_pos, replace_symbol_amount, "[");
        val.replace(close_brace_pos, replace_symbol_amount, "]");
        change_null_register(val);
        return sl::json::loads(val);
    }
    case PSQL_BOOLOID: {
        if ("t" == val) { // "t" - true sign for boolean type from PGresult
            js_val.set_bool(true);
        } else if ("f" == val) {
            js_val.set_bool(false);
        }
        break;
    }
    case PSQL_INT2OID:
    case PSQL_INT4OID:
    case PSQL_INT8OID:
    case PSQL_JSONOID:
    case PSQL_JSONBOID:
    case PSQL_FLOAT4OID:
    case PSQL_FLOAT8OID: {
        return sl::json::loads(val);
    }
    case PSQL_NULLOID:{
        break;
    }
    case PSQL_TEXTOID:
    case PSQL_VARCHAROID:
    default: {
        js_val.set_string(val);
        break;
    }
    }
    return js_val;
}

sl::json::value row::dump_to_json(){
    sl::json::value json_res;
    std::vector<sl::json::field> fields;
    for (size_t i = 0; i < properties.size(); ++i) {
        std::string field_name = properties[i].name;
        fields.emplace_back(field_name.c_str(),
                            get_value_as_json(i));
    }
    json_res.set_object(std::move(fields));
    return json_res;
}


class psql_handler::impl : public staticlib::pimpl::object::impl {
protected:
    PGconn *conn;
    PGresult *res;
    std::string connection_parameters;
    std::string last_error;
    std::map<std::string, std::vector<std::string>> prepared_names;
    std::unordered_map<std::string, std::string> queries_cache;
//    int ping_on;
    sl::utils::random_string_generator names_generator;
public:    
impl(const std::string& conn_params) :
conn(nullptr),
res(nullptr),
connection_parameters(conn_params){ }

~impl() STATICLIB_NOEXCEPT {
    clear_result();
    close();
}

void setup_connection_params(psql_handler&, const std::string& conn_params) {
    this->connection_parameters = conn_params;
}

bool connect(psql_handler&) {
    /* Make a connection to the database */
    this->conn = PQconnectdb(connection_parameters.c_str());

    /* Check to see that the backend connection was successfully made */
    if (CONNECTION_OK != PQstatus(conn)) {
        last_error = "Connection to database failed: " + std::string(PQerrorMessage(conn));
        close();
        return false;
    }
    return true;
}

void close(){
    if (nullptr != conn) {
        PQfinish(conn);
        conn = nullptr;
    }
}

bool handle_result(PGconn* conn, PGresult* res, const std::string& error_message){
    std::string msg(error_message);

    ExecStatusType const status = PQresultStatus(res);
    switch (status) {
    case PGRES_EMPTY_QUERY:
    case PGRES_COMMAND_OK:
        // No data but don't throw neither.
        return false;

    case PGRES_TUPLES_OK:
        return true;

    case PGRES_FATAL_ERROR:
        msg += " Fatal error.";
        if (CONNECTION_BAD == PQstatus(conn)) {
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
    if (pqError && *pqError) {
        msg += " Code: [";
        msg += PQresultErrorField(res, PG_DIAG_SQLSTATE);
        msg += "], ";
        msg += pqError;
    }

    const char* sqlstate = PQresultErrorField(res, PG_DIAG_SQLSTATE);
    const char* const blank_sql_state = "     ";
    if (!sqlstate) {
        sqlstate = blank_sql_state;
    }

    throw wilton::support::exception(TRACEMSG(msg));
}

void prepare_params(
        std::vector<Oid>& types,
        std::vector<const char*>& values,
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
                    if (PSQL_UNKNOWNOID != val.type) {
                        values.push_back(val.value.c_str());
                    } else {
                        values.push_back(nullptr);
                    }
                    types.push_back(val.type);
                    length.push_back(val.len);
                    formats.push_back(val.format); // text_format
                    break;
                }
            }
        }
    } else {
        auto compare = [] (const parameters_values& a, const parameters_values& b) -> bool {
            int a_pos = 0;
            int b_pos = 0;

            a_pos = std::atoi(a.parameter_name.substr(1).c_str());
            b_pos = std::atoi(b.parameter_name.substr(1).c_str());

            return (a_pos < b_pos);
        };
        std::sort(vals.begin(), vals.end(), compare);
        for (auto& el : vals) {
            if (PSQL_UNKNOWNOID != el.type) {
                values.push_back(el.value.c_str());
            } else {
                values.push_back(nullptr);
            }
            types.push_back(el.type);
            length.push_back(el.len);
            formats.push_back(el.format); // text_format
        }
    }

}

sl::json::value execute_hardcode_statement(PGconn* conn, const std::string& query, const std::string& error_message) {
    res = PQexec(conn, query.c_str());
    if (is_connection_bad()) {
        reset_database_connection();
        res = PQexec(conn, query.c_str());
    }

    return get_execution_result(error_message); // throw on error
}

void begin(psql_handler&)
{
    execute_hardcode_statement(conn, "BEGIN", "Cannot begin transaction.");
}

void commit(psql_handler&)
{
    execute_hardcode_statement(conn, "COMMIT", "Cannot commit transaction.");
}

void rollback(psql_handler&)
{
    execute_hardcode_statement(conn, "ROLLBACK", "Cannot rollback transaction.");
}

void deallocate_prepared_statement(const std::string& statement_name) {
    std::string query = "DEALLOCATE " + statement_name + ";";
    execute_hardcode_statement(conn, query.c_str(), "Cannot deallocate prepared statement.");
    prepared_names.erase(statement_name);
}

std::string parse_query(const std::string& sql_query, std::vector<std::string>& last_prepared_names){
    enum { normal, in_quotes, in_name } state = normal;
    std::map<std::string, std::string> names;
    std::string name;
    std::string query;
    int position = 1;
    last_prepared_names.clear();

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
                if (!names.count(name)) {
                    std::stringstream ss;
                    ss << '$' << position++;
                    names[name] = ss.str();
                    query += ss.str();
                    last_prepared_names.push_back(name);
                } else {
                    query += names[name];
                }
                query += *it;
                state = normal;
                name.clear();

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
        if (!names.count(name)) {
            std::stringstream ss;
            ss << '$' << position++;
            names[name] = ss.str();
            query += ss.str();
            last_prepared_names.push_back(name);
        } else {
            query += names[name];
        }
    }

    return query;
}

void clear_result(){
    if (res) {
        PQclear(res);
        res = nullptr;
    }
}

std::string generate_unique_name(){
    const int gen_length = 32;
    std::string name{};
    do {
        name = names_generator.generate(gen_length);
    } while (prepared_names.count(name));
    return name;
}

size_t sql_cached(const std::string& sql){
    return queries_cache.count(sql);
}

void cache_sql(const std::string& sql, const std::string& name){
    queries_cache[sql] = name;
}

sl::json::value prepare_and_cahce(const std::string& sql_query, std::string& query_name){
    query_name = generate_unique_name();
    std::string query = parse_query(sql_query, prepared_names[query_name]);
    res = PQprepare(conn, query_name.c_str(), query.c_str(), static_cast<int>(prepared_names[query_name].size()), NULL);
    if (is_connection_bad()) {
        auto tmp_prepared_names = prepared_names[query_name];
        reset_database_connection();
        prepared_names[query_name] = tmp_prepared_names;
        res = PQprepare(conn, query_name.c_str(), query.c_str(), static_cast<int>(prepared_names[query_name].size()), NULL);
    }
    sl::json::value result = get_execution_result("PQprepare error"); // throw on error
    cache_sql(sql_query, query_name);
    return result;
}

sl::json::value prepare_cached(const std::string& sql_query, std::string& query_name){
    if (sql_cached(sql_query)) {
        query_name = queries_cache[sql_query];
        return sl::json::value(); // null_T value
    } else {
        return prepare_and_cahce(sql_query, query_name);
    }
}

sl::json::value get_command_status_as_json(PGresult* result){
    std::vector<sl::json::field> fields;
    sl::json::value val(PQcmdStatus(result));
    fields.emplace_back("cmd_status", std::move(val));
    sl::json::value json;
    json.set_object(std::move(fields));
    return json;
}

sl::json::value prepare_and_execute_with_parameters(const std::string& sql_query, const staticlib::json::value &parameters){
    std::string prepared_name{};

    prepare_cached(sql_query, prepared_name);

    int params_count = 0;
    std::vector<Oid> params_types;
    std::vector<const char*> params_values;
    std::vector<int> params_length;
    std::vector<int> params_formats;
    const int text_format = 0;

    std::vector<parameters_values> vals;

    setup_params_from_json(vals, parameters, prepared_names[prepared_name]);
    prepare_params(params_types, params_values, params_length, params_formats, vals, prepared_names[prepared_name]);

    params_count = static_cast<int>(params_types.size());
    res = PQexecPrepared(conn, prepared_name.c_str(),
                         params_count,
                         const_cast<const char* const*>(params_values.data()),
                         const_cast<const int*>(params_length.data()),
                         const_cast<const int*>(params_formats.data()),
                         text_format);
    if (is_connection_bad()) {
        reset_database_connection();
        prepare_cached(sql_query, prepared_name);
        res = PQexecPrepared(conn, prepared_name.c_str(),
                             params_count,
                             const_cast<const char* const*>(params_values.data()),
                             const_cast<const int*>(params_length.data()),
                             const_cast<const int*>(params_formats.data()),
                             text_format);
    }

    return get_execution_result("PQexecPrepared error"); // throw on error
}

sl::json::value execute_sql_with_parameters(
        const std::string& sql_statement, const staticlib::json::value& parameters) {
    int params_count = 0;
    std::vector<Oid> params_types;
    std::vector<const char*> params_values;
    std::vector<int> params_length;
    std::vector<int> params_formats;
    const int text_format = 0;

    std::vector<parameters_values> vals;

    std::vector<std::string> names;
    std::string query = parse_query(sql_statement, names);

    setup_params_from_json(vals, parameters, names);
    prepare_params(params_types, params_values, params_length, params_formats, vals, names);

    params_count = static_cast<int>(params_types.size());

    res = PQexecParams(conn, query.c_str(),
                       params_count, params_types.data(),
                       const_cast<const char* const*>(params_values.data()),
                       const_cast<const int*>(params_length.data()),
                       const_cast<const int*>(params_formats.data()),
                       text_format);

    if (is_connection_bad()) {
        reset_database_connection();
        res = PQexecParams(conn, query.c_str(),
                           params_count, params_types.data(),
                           const_cast<const char* const*>(params_values.data()),
                           const_cast<const int*>(params_length.data()),
                           const_cast<const int*>(params_formats.data()),
                           text_format);
    }
    return get_execution_result("PQexecParams error"); // throw on error
}

bool is_connection_bad() {
    return (CONNECTION_BAD == PQstatus(conn));
}

void clear_cache(){
    queries_cache.clear();
    prepared_names.clear();
}

void reset_database_connection() {
    PQreset(conn);
    clear_cache();
}

sl::json::value execute_with_parameters(psql_handler&, const std::string& sql_statement, const staticlib::json::value& parameters, int cache_flag) {
    if (cache_flag) {
        return prepare_and_execute_with_parameters(sql_statement, parameters);
    }
    return execute_sql_with_parameters(sql_statement, parameters);
}

std::string get_last_error(psql_handler&) {
    return last_error;
}

sl::json::value get_execution_result(const std::string &error_msg){
    bool result = handle_result(conn, res, error_msg);
    sl::json::value json_result{};
    if (result) {
        json_result = get_result_as_json(res);
    } else {
        json_result = get_command_status_as_json(res);
    }
    clear_result();
    return json_result;
}

};

PIMPL_FORWARD_CONSTRUCTOR(psql_handler, (const std::string&), (), wilton::support::exception);
PIMPL_FORWARD_METHOD(psql_handler, void, setup_connection_params, (const std::string&), (), support::exception);
PIMPL_FORWARD_METHOD(psql_handler, bool, connect, (), (), support::exception);
PIMPL_FORWARD_METHOD(psql_handler, void, begin, (), (), support::exception);
PIMPL_FORWARD_METHOD(psql_handler, void, commit, (), (), support::exception);
PIMPL_FORWARD_METHOD(psql_handler, void, rollback, (), (), support::exception);
PIMPL_FORWARD_METHOD(psql_handler, sl::json::value, execute_with_parameters, (const std::string&)(const staticlib::json::value&)(int), (), support::exception);
PIMPL_FORWARD_METHOD(psql_handler, std::string, get_last_error, (), (), support::exception);

} // pgsql
} // db
} // wilton

