#include "Database.h"

#include <iostream>
#include <sqlite3.h>

Database::~Database()
{
    close();
}

bool Database::open(const std::string& path)
{
    close();

    int rc = sqlite3_open(path.c_str(), &db_);

    if (rc != SQLITE_OK) {
        std::cerr << "打开数据库失败："
                  << (db_ ? sqlite3_errmsg(db_) : "内存不足")
                  << '\n';
        close();
        return false;
    }

    sqlite3_busy_timeout(db_, 3000);

    const char* create_sql = R"SQL(
        CREATE TABLE IF NOT EXISTS telemetry (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            received_at TEXT NOT NULL DEFAULT
                (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
            source_ts_ms INTEGER NOT NULL,
            device TEXT NOT NULL,
            group_name TEXT NOT NULL,
            temp_c REAL NOT NULL,
            humidity_pct REAL NOT NULL,
            switch_state INTEGER NOT NULL,
            device_counter INTEGER NOT NULL
        );
    )SQL";

    rc = sqlite3_exec(
        db_, create_sql, nullptr, nullptr, nullptr
    );

    if (rc != SQLITE_OK) {
        std::cerr << "创建数据表失败："
                  << sqlite3_errmsg(db_) << '\n';
        close();
        return false;
    }

    const char* insert_sql = R"SQL(
        INSERT INTO telemetry (
            source_ts_ms,
            device,
            group_name,
            temp_c,
            humidity_pct,
            switch_state,
            device_counter
        )
        VALUES (?, ?, ?, ?, ?, ?, ?);
    )SQL";

    rc = sqlite3_prepare_v2(
        db_, insert_sql, -1, &insert_stmt_, nullptr
    );

    if (rc != SQLITE_OK) {
        std::cerr << "SQL预编译失败："
                  << sqlite3_errmsg(db_) << '\n';
        close();
        return false;
    }

    std::cout << "Database模块初始化成功\n";
    return true;
}

bool Database::insert(const Telemetry& data)
{
    if (!insert_stmt_) {
        return false;
    }

    sqlite3_reset(insert_stmt_);
    sqlite3_clear_bindings(insert_stmt_);

    int rc = sqlite3_bind_int64(
        insert_stmt_, 1, data.source_ts_ms
    );

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            insert_stmt_, 2, data.device.c_str(),
            -1, SQLITE_TRANSIENT
        );

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_text(
            insert_stmt_, 3, data.group_name.c_str(),
            -1, SQLITE_TRANSIENT
        );

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_double(
            insert_stmt_, 4, data.temp_c
        );

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_double(
            insert_stmt_, 5, data.humidity_pct
        );

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_int(
            insert_stmt_, 6, data.switch_state
        );

    if (rc == SQLITE_OK)
        rc = sqlite3_bind_int(
            insert_stmt_, 7, data.device_counter
        );

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(insert_stmt_);
    }

    bool success = (rc == SQLITE_DONE);

    if (!success) {
        std::cerr << "数据库插入失败："
                  << sqlite3_errmsg(db_) << '\n';
    }

    sqlite3_reset(insert_stmt_);
    sqlite3_clear_bindings(insert_stmt_);

    return success;
}

void Database::close()
{
    if (insert_stmt_) {
        sqlite3_finalize(insert_stmt_);
        insert_stmt_ = nullptr;
    }

    if (db_) {
        sqlite3_close(db_);
        db_ = nullptr;
    }
}
