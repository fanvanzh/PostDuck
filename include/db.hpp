#ifndef DB_HPP
#define DB_HPP
#include "duckdb.hpp"

class DB {
public:
    DB() {
        db = new duckdb::DuckDB(nullptr, nullptr); // In-memory database
    }
    ~DB() {
        delete db;
    }
    std::shared_ptr<duckdb::Connection> get_connection() {
        return std::make_shared<duckdb::Connection>(*db);
    }
private:
    duckdb::DuckDB* db = nullptr;
};

#endif // DB_HPP