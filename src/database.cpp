//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <boost/filesystem/operations.hpp>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/utility/identity_type.hpp>
#include <quince/exceptions.h>
#include <quince/detail/compiler_specific.h>
#include <quince/detail/session.h>
#include <quince/detail/util.h>
#include <quince/mappers/direct_mapper.h>
#include <quince/mappers/numeric_cast_mapper.h>
#include <quince/mappers/reinterpret_cast_mapper.h>
#include <quince/mappers/serial_mapper.h>
#include <quince/query.h>
#include <quince/transaction.h>
#include <sqlite3.h>
#include <quince_sqlite/database.h>
#include <quince_sqlite/detail/dialect_sql.h>

using namespace quince;
using boost::optional;
using boost::posix_time::ptime;
using boost::filesystem::path;
using std::dynamic_pointer_cast;
using std::map;
using std::pair;
using std::shared_ptr;
using std::string;
using std::unique_ptr;
using std::vector;


QUINCE_SUPPRESS_MSVC_DOMINANCE_WARNING

namespace quince_sqlite {

namespace {
    class ptime_mapper : public abstract_mapper<ptime>, public direct_mapper<string>
    {
    public:
        explicit ptime_mapper(const optional<string> &name, const mapper_factory &creator) :
            abstract_mapper_base(name),
            abstract_mapper<ptime>(name),
            direct_mapper<string>(name, creator)
        {}

        virtual std::unique_ptr<cloneable>
        clone_impl() const override {
            return quince::make_unique<ptime_mapper>(*this);
        }

        virtual void from_row(const row &src, ptime &dest) const override {
            string text;
            direct_mapper<string>::from_row(src, text);
            dest = boost::posix_time::time_from_string(text);
        }

        virtual void to_row(const ptime &src, row &dest) const override {
            const string text(boost::posix_time::to_simple_string(src));
            direct_mapper<string>::to_row(text, dest);
        }

    protected:
        virtual void build_match_tester(const query_base &qb, predicate &result) const override {
            abstract_mapper<ptime>::build_match_tester(qb, result);
        }
    };

    struct customization_for_dbms : mapping_customization {
        customization_for_dbms() {
            customize<bool, numeric_cast_mapper<bool, direct_mapper<int64_t>>>();
            customize<int8_t, numeric_cast_mapper<int8_t, direct_mapper<int64_t>>>();
            customize<int16_t, numeric_cast_mapper<int16_t, direct_mapper<int64_t>>>();
            customize<int32_t, numeric_cast_mapper<int32_t, direct_mapper<int64_t>>>();
            customize<int64_t, direct_mapper<int64_t>>();
            customize<float, numeric_cast_mapper<float, direct_mapper<double>>>();
            customize<double, direct_mapper<double>>();
            customize<uint8_t, numeric_cast_mapper<uint8_t, direct_mapper<int64_t>>>();
            customize<uint16_t, numeric_cast_mapper<uint16_t, direct_mapper<int64_t>>>();
            customize<uint32_t, numeric_cast_mapper<uint32_t, direct_mapper<int64_t>>>();
            customize<uint64_t, reinterpret_cast_mapper<uint64_t, direct_mapper<int64_t>, uint64_t(0x8000000000000000)>>();
            customize<std::string, direct_mapper<std::string>>();
            customize<byte_vector, direct_mapper<byte_vector>>();
            customize<serial, serial_mapper>();
            customize<ptime, ptime_mapper>();
        }
    };

    map<string, path>
    to_absolute_filename_strings(const database::filename_map &paths) {
        map<string, path> result;
        for (const pair<string, path> p: paths)
            result.insert(make_pair(p.first, absolute(p.second)));
        return result;
    }
}


database::database(
    string filename,
    bool may_write,
    bool mutex,
    bool share_cache,
    optional<string> vfs_module_name,
    const boost::optional<mapping_customization> &customization_for_db,
    const filename_map &attachable_database_filenames
) :
    quince::database(
        clone_or_null(customization_for_db),
        quince::make_unique<customization_for_dbms>()
    ),
    _spec({
        filename,
        (     (may_write ? SQLITE_OPEN_READWRITE : SQLITE_OPEN_READONLY)
            | (mutex ? SQLITE_OPEN_FULLMUTEX : SQLITE_OPEN_NOMUTEX)
            | (share_cache ? SQLITE_OPEN_SHAREDCACHE : SQLITE_OPEN_PRIVATECACHE)
        ),
        vfs_module_name
    }),
    _attachable_database_absolute_filenames(to_absolute_filename_strings(attachable_database_filenames))
{}


database::~database()
{}

unique_ptr<sql>
database::make_sql() const {
    return make_dialect_sql();
}

optional<string>
database::get_default_enclosure() const {
    return boost::none;
}

void
database::make_enclosure_available(const optional<string> &enclosure_name) const {
    if (enclosure_name) {
        path absolute;
        if (const optional<const path &> found = lookup(_attachable_database_absolute_filenames, *enclosure_name))
            absolute = std::move(*found);
        else
            absolute = boost::filesystem::absolute(path(*enclosure_name));

        const unique_ptr<dialect_sql> cmd = make_dialect_sql();
        cmd->write_attach_database(absolute, *enclosure_name);
        get_session()->exec(*cmd);
    }
}

new_session
database::make_session() const {
    return quince::make_unique<session_impl>(*this, _spec);
}

vector<string>
database::retrieve_column_titles(const binomen &table) const {
    const session s = get_session();

    const unique_ptr<dialect_sql> cmd = make_dialect_sql();
    cmd->write_retrieve_metadata(table);
    const result_stream stream = s->exec_with_stream_output(*cmd, 1);

    vector<string> result;
    while (unique_ptr<row> r = s->next_output(stream)) {
        string name;
        r->get("name", name);
        string type;
        r->get("type", type);
        result.push_back("\"" + name + "\" " + type);
    }
    return result;
}

serial
database::insert_with_readback(unique_ptr<sql> insert, const serial_mapper &) const {
    const shared_ptr<session_impl> s = get_session_impl();
    s->exec(*insert);
    return (s->last_inserted_serial());
}

string
database::column_type_name(column_type type) const {
    switch (type)   {
        case column_type::big_serial:
        case column_type::big_int:          return "INTEGER";
        case column_type::double_precision: return "REAL";
        case column_type::string:           return "TEXT";
        case column_type::byte_vector:      return "BLOB";
        default:                            abort();
    }
}

bool
database::supports_join(conditional_junction_type type) const {
    switch (type) {
        case conditional_junction_type::inner:
        case conditional_junction_type::left:   return true;
        case conditional_junction_type::right:
        case conditional_junction_type::full:   return false;
        default:                                abort();
    }
}

bool
database::supports_combination(combination_type type, bool all) const {
    return !all || type == combination_type::union_;
}

bool
database::supports_index(const index_spec &spec) const {
    for (const abstract_mapper_base *m: spec._mappers)
        if (dynamic_cast<const exprn_mapper_base *>(m))
            return false;
    return true;
}


shared_ptr<session_impl>
database::get_session_impl() const {
    return dynamic_pointer_cast<session_impl>(get_session());
}

unique_ptr<dialect_sql>
database::make_dialect_sql() const {
    return quince::make_unique<dialect_sql>(*this);
}

}

QUINCE_UNSUPPRESS_MSVC_WARNING
