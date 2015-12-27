#ifndef QUINCE_SQLITE__detail__session_h
#define QUINCE_SQLITE__detail__session_h

//          Copyright Michael Shepanski 2014.
// Distributed under the Boost Software License, Version 1.0.
//    (See accompanying file ../../../LICENSE_1_0.txt or copy at
//          http://www.boost.org/LICENSE_1_0.txt)

#include <string>
#include <vector>
#include <boost/noncopyable.hpp>
#include <boost/optional.hpp>
#include <quince/detail/compiler_specific.h>
#include <quince/detail/session.h>

struct sqlite3;


namespace quince_sqlite {

class database;

class session_impl : public quince::abstract_session_impl {
public:
    struct spec {
        std::string _filename;
        int _flags;
        boost::optional<std::string> _vfs_module_name;
    };

    explicit session_impl(const database &, const session_impl::spec &);

    virtual ~session_impl();

    virtual bool                            unchecked_exec(const quince::sql &) override;
    virtual void                            exec(const quince::sql &) override;
    virtual quince::result_stream           exec_with_stream_output(const quince::sql &, uint32_t ignored) override;
    virtual std::unique_ptr<quince::row>    exec_with_one_output(const quince::sql &) override;
    virtual std::unique_ptr<quince::row>    next_output(const quince::result_stream &) override;

    quince::serial last_inserted_serial() const;

private:
    QUINCE_NORETURN void throw_last_error(int last_result_code) const;

    class statement;

    std::unique_ptr<statement> make_stmt(const quince::sql &cmd);

    const database &_database;
    sqlite3 * const _conn;
    std::string _latest_sql;
};

}

#endif
