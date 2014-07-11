#ifndef QUINCE_SQLITE__detail__dialect_sql_h
#define QUINCE_SQLITE__detail__dialect_sql_h

/*
    Copyright 2014 Michael Shepanski

    This file is part of the quince_sqlite library.

    Quince_sqlite is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    Quince_sqlite is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with quince_sqlite.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <quince/detail/sql.h>


namespace quince_sqlite {

class database;

class dialect_sql : public quince::sql {
public:
    explicit dialect_sql(const database &);

    virtual std::unique_ptr<quince::cloneable> clone_impl() const override;

    virtual void write_nulls_low(bool invert) override  {}

    virtual void write_no_limit() override;

    virtual void
    write_create_index(
        const quince::binomen &table,
        size_t per_table_index_count,
        const std::vector<const quince::abstract_mapper_base *> &,
        bool unique
    ) override;

    virtual void
    write_create_table(
        const quince::binomen &table,
        const quince::abstract_mapper_base &value_mapper,
        const quince::abstract_mapper_base &key_mapper,
        boost::optional<quince::column_id> generated_key,
        const std::vector<quince::foreign_spec> &
    ) override;

    virtual void write_collective_comparison(
        quince::relation,
        const quince::abstract_column_sequence &lhs,
        const quince::collective_base &rhs
    ) override;

    virtual void write_distinct(const std::vector<const quince::abstract_mapper_base*> &) override;
    using quince::sql::write_distinct;

    virtual void write_combination(quince::combination_type type, bool all, const quince::query_base &rhs);

    virtual std::string next_placeholder() override;

    void write_attach_database(const boost::filesystem::path & absolute_pathname, const std::string &db_name);

    void write_retrieve_metadata(const quince::binomen &table);

private:
    uint32_t _next_placeholder_serial;
};

}

#endif
