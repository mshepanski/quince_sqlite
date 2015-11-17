//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <quince/detail/binomen.h>
#include <quince/detail/util.h>
#include <quince/mappers/detail/persistent_column_mapper.h>
#include <quince/exprn_mappers/detail/exprn_mapper.h>
#include <quince/query.h>
#include <quince_sqlite/database.h>
#include <quince_sqlite/detail/dialect_sql.h>

using namespace quince;
using boost::optional;
using std::string;
using std::to_string;
using std::unique_ptr;
using std::vector;


namespace quince_sqlite {

dialect_sql::dialect_sql(const database &db) :
    sql(db),
    _next_placeholder_serial(0)
{}

unique_ptr<cloneable>
dialect_sql::clone_impl() const {
    return quince::make_unique<dialect_sql>(*this);
}

void
dialect_sql::write_no_limit() {
    write("LIMIT -1 ");
}

void
dialect_sql::write_attach_database(const boost::filesystem::path &absolute_pathname, const string &db_name) {
    write("ATTACH ");
    write_quoted(absolute_pathname.string());
    write(" AS ");
    write_quoted(db_name);
}

void
dialect_sql::write_retrieve_metadata(const binomen &table) {
    write("PRAGMA ");
    if (const optional<string> &database_name = table._enclosure) {
        write_quoted(*database_name);
        write(".");
    }
    write("table_info(" + table._local + ")");
}

void
dialect_sql::write_create_index(
    const binomen &table,
    size_t per_table_index_count,
    const vector<const abstract_mapper_base *> &mappers,
    bool unique
) {
    binomen index_name = table;
    index_name._local += ":" + std::to_string(per_table_index_count);

    write("CREATE ");
    if (unique)  write("UNIQUE ");
    write("INDEX ");
    write_quoted(index_name);
    write (" ON ");
    write_quoted(table._local);

    write(" (");
    comma_separated_list_scope list_scope(*this);
    for (const abstract_mapper_base *m: mappers)
        m->for_each_persistent_column([&](const persistent_column_mapper &p) {
            list_scope.start_item();
            write_quoted(p.name());
        });
    write(")");
}

void
dialect_sql::write_create_table(
    const binomen &table,
    const abstract_mapper_base &value_mapper,
    const abstract_mapper_base &key_mapper,
    optional<column_id> generated_key,
    const vector<foreign_spec> &foreign_specs
) {
    quince::sql::write_create_table(table, value_mapper, key_mapper, generated_key, foreign_specs);
    if (! generated_key)  write(" WITHOUT ROWID");
}

void
dialect_sql::write_collective_comparison(relation r, const abstract_column_sequence &lhs, const collective_base &rhs) {
    throw unsupported_exception();
}

void
dialect_sql::write_distinct(const vector<const abstract_mapper_base *> &distincts) {
    if (! distincts.empty())  throw unsupported_exception();

    write_distinct();
}

void
dialect_sql::write_combination(combination_type type, bool all, const query_base &rhs) {
    if (all  &&  type != combination_type::union_)  throw unsupported_exception();

    if (rhs.is_combined())  throw unsupported_exception();

    sql::write_combination(type, all, rhs);
}

string
dialect_sql::next_placeholder() {
    return "?" + to_string(++_next_placeholder_serial);
}

}