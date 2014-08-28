#ifndef QUINCE_SQLITE__database_h
#define QUINCE_SQLITE__database_h

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

#include <map>
#include <boost/optional.hpp>
#include <boost/filesystem/path.hpp>
#include <quince/database.h>
#include <quince/mapping_customization.h>
#include <quince_sqlite/detail/session.h>


namespace quince_sqlite {

class table_base;
class dialect_sql;

// See http://quince-lib.com/quince_sqlite.html#quince_sqlite.constructor
//
class database : public quince::database {
public:
    typedef std::map<std::string, boost::filesystem::path> filename_map;

    database(
        std::string filename,
        bool may_write = true,
        bool mutex = true,
        bool share_cache = true,
        boost::optional<std::string> vfs_module_name = boost::none,
        const boost::optional<quince::mapping_customization> &customization_for_db = boost::none,
        const filename_map &attachable_database_filenames = filename_map()
    );

    virtual ~database();


    // --- Everything from here to end of class is for quince internal use only. ---

    virtual std::unique_ptr<quince::sql>    make_sql() const override;
    virtual boost::optional<std::string>    get_default_enclosure() const override;
    void                                    make_enclosure_available(const boost::optional<std::string> &enclosure_name) const override;
    virtual quince::new_session             make_session() const override;
    virtual std::vector<std::string>        retrieve_column_titles(const quince::binomen &table) const override;
    virtual quince::serial                  insert_with_readback(std::unique_ptr<quince::sql> insert, const quince::serial_mapper &unused) const override;
    virtual std::string                     column_type_name(quince::column_type) const override;

    virtual bool supports_join(quince::conditional_junction_type) const override;
    virtual bool supports_combination(quince::combination_type, bool all) const override;
    virtual bool supports_nested_combinations() const override                              { return false; }
    virtual bool supports_index(const quince::index_spec &) const override;
    virtual bool imposes_combination_precedence() const override                            { return false; }

    std::unique_ptr<dialect_sql> make_dialect_sql() const;

private:
    std::shared_ptr<session_impl> get_session_impl() const;

    const session_impl::spec _spec;
    const std::map<std::string, boost::filesystem::path> _attachable_database_absolute_filenames;
};

}

#endif
