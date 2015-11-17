//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <assert.h>
#include <stdint.h>
#include <string>
#include <vector>
#include <boost/optional.hpp>
#include <quince/detail/column_type.h>
#include <quince/detail/compiler_specific.h>
#include <quince/exceptions.h>
#include <quince/detail/row.h>
#include <quince/detail/sql.h>
#include <sqlite3.h>
#include <quince_sqlite/database.h>
#include <quince_sqlite/detail/dialect_sql.h>
#include <quince_sqlite/detail/session.h>

using boost::optional;
using namespace quince;
using std::dynamic_pointer_cast;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;


namespace quince_sqlite {

class session_impl::statement : public abstract_result_stream_impl {
public:
    statement(sqlite3 *conn, const sql &cmd) :
        _stmt(prepare(conn, cmd.get_text(), _construction_result_code))
    {
        if (_construction_result_code == SQLITE_OK) {
            int i = 1;
            for (const cell &c: cmd.get_input().values())
                if ((_construction_result_code = bind(c, i++)) != SQLITE_OK)
                    break;
        }
    }

    ~statement() {
        finalize(_stmt);
    }

    int
    next(row *r = nullptr) {
        if (_construction_result_code != SQLITE_OK)  return _construction_result_code;

        assert(_stmt != nullptr);
        const int result_code = sqlite3_step(_stmt);
        if (result_code == SQLITE_ROW  &&  r != nullptr) {
            const int n = sqlite3_data_count(_stmt);
            for (int i = 0; i < n; i++)
                r->add_cell(extract(i), sqlite3_column_name(_stmt, i));
        }
        return result_code;
    }

private:
    static sqlite3_stmt *
    prepare(sqlite3 *conn, const string &sql_text, int &result_code) {
        sqlite3_stmt *result;
        result_code = sqlite3_prepare_v2(conn, sql_text.c_str(), -1, &result, nullptr);
        if (result_code != SQLITE_OK) {
            finalize(result);
            return nullptr;
        }
        assert(result != nullptr);
        return result;
    }

    int
    bind(const cell &c, int index) {
        assert(_stmt != nullptr);
        switch(c.type()) {
            case column_type::big_int:          return sqlite3_bind_int64(_stmt, index, c.get<int64_t>());
            case column_type::double_precision: return sqlite3_bind_double(_stmt, index, c.get<double>());
            case column_type::string:           return sqlite3_bind_text(_stmt, index, c.chars(), int(c.size()), SQLITE_TRANSIENT);
            case column_type::byte_vector:      return sqlite3_bind_blob(_stmt, index, c.data(), int(c.size()), SQLITE_TRANSIENT);
            case column_type::none:             return sqlite3_bind_null(_stmt, index);
            default:                            abort();
        }
    }

    cell
    extract(int index) const {
        assert(_stmt != nullptr);
        const int type = sqlite3_column_type(_stmt, index);
        switch (type) {
            case SQLITE_INTEGER:    return cell(static_cast<int64_t>(sqlite3_column_int64(_stmt, index)));  // sqlite3_in64 ain't int64_t to gcc.
            case SQLITE_FLOAT:      return cell(sqlite3_column_double(_stmt, index));
            case SQLITE_TEXT:       return cell(column_to_string(index));
            case SQLITE_BLOB:       return cell(column_to_byte_vector(index));
            case SQLITE_NULL:       return cell(boost::none);
            default:                throw retrieved_unrecognized_type_exception(type);
        }
    }

    static void
    finalize(sqlite3_stmt *stmt) {
        if (stmt != nullptr)  sqlite3_finalize(stmt);
    }

    string
    column_to_string(int index) const {
        return reinterpret_cast<const char *>(sqlite3_column_text(_stmt, index));
    }

    vector<uint8_t>
    column_to_byte_vector(int index)  const {
        vector<uint8_t> result;
        if (const auto base_addr = static_cast<const uint8_t *>(sqlite3_column_blob(_stmt, index)))
            result.assign(base_addr, base_addr + sqlite3_column_bytes(_stmt, index));
        return result;
    }

    int _construction_result_code;
    sqlite3_stmt * const _stmt;
};


namespace {
    sqlite3 *
    connect(const session_impl::spec &spec) {
        sqlite3 *result;
        int result_code = sqlite3_open_v2(
            spec._filename.c_str(),
            &result,
            spec._flags,
            spec._vfs_module_name ? spec._vfs_module_name->c_str() : nullptr
        );
        if (result_code != SQLITE_OK) {
            if (result != nullptr)  sqlite3_close(result);
            return nullptr;
        }
        assert(result != nullptr);
        return result;
    }
}


session_impl::session_impl(const database &database, const session_impl::spec &spec) :
    _database(database),
    _conn(connect(spec))
{
    if (! _conn)  throw failed_connection_exception();
}

session_impl::~session_impl() {
    _asynchronous_stmt.reset();
    if (_conn)  sqlite3_close(_conn);
}

bool
session_impl::unchecked_exec(const sql &cmd) {
    assert(! _asynchronous_stmt);
    return make_stmt(cmd)->next() == SQLITE_DONE;
}

unique_ptr<row>
session_impl::exec_with_one_output(const sql &cmd) {
    absorb_pending_results();
    auto result = quince::make_unique<row>(&_database);
    statement stmt(_conn, cmd);
    switch(int result_code = stmt.next(result.get())) {
        case SQLITE_DONE:   return nullptr;
        case SQLITE_ROW:    break;
        default:            throw_last_error(result_code);
    }
    if (stmt.next() != SQLITE_DONE)  throw multi_row_exception();
    return result;
}

result_stream
session_impl::exec_with_stream_output(const sql &cmd, uint32_t) {
    absorb_pending_results();
    _asynchronous_stmt = make_stmt(cmd);
    return _asynchronous_stmt;
}

void
session_impl::exec(const sql &cmd) {
    absorb_pending_results();
    int result_code = make_stmt(cmd)->next();
    if (result_code != SQLITE_DONE)  throw_last_error(result_code);
}

unique_ptr<row>
session_impl::next_output(const result_stream &rs) {
    assert(rs);
    shared_ptr<statement> stmt = dynamic_pointer_cast<statement>(rs);
    assert(stmt);
    if (stmt != _asynchronous_stmt) {
        absorb_pending_results();
        assert(_asynchronous_stmt);
        _asynchronous_stmt = stmt;
    }
    auto result = quince::make_unique<row>(&_database);
    const int result_code = stmt->next(result.get());
    if (result_code == SQLITE_ROW)  return result;
    _asynchronous_stmt.reset();
    if (result_code == SQLITE_DONE)  return nullptr;
    throw_last_error(result_code);
}

serial
session_impl::last_inserted_serial() const {
    serial result;
    result.assign(sqlite3_last_insert_rowid(_conn));
    return result;
}

void
session_impl::throw_last_error(int last_result_code) const {
    const char * const dbms_message = sqlite3_errstr(last_result_code);
    string message(dbms_message ? dbms_message : "");
    message += " (most recent SQL command was `" + _latest_sql + "')";

    switch (last_result_code & 0xff) {
        case SQLITE_BUSY:
        case SQLITE_LOCKED: throw deadlock_exception(message);
        default:            throw dbms_exception(message);
    }
}

std::unique_ptr<session_impl::statement>
session_impl::make_stmt(const sql &cmd) {
    _latest_sql = cmd.get_text();
    return quince::make_unique<statement>(_conn, cmd);
}

void
session_impl::absorb_pending_results() {
    if (_asynchronous_stmt) {
        int result_code;
        while ((result_code = _asynchronous_stmt->next()) != SQLITE_DONE)
            if (result_code != SQLITE_ROW)  throw_last_error(result_code);
        _asynchronous_stmt.reset();
    }
}

}
