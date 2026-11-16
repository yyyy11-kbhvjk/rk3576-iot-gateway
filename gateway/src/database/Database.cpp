#include "Database.h"

#include <cstdlib>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include <sqlite3.h>

namespace {

// 执行一段 SQL，并输出错误信息
bool execute_sql(
    sqlite3* db,
    const std::string& sql,
    const char* description)
{
    char* error_message = nullptr;

    int rc = sqlite3_exec(
        db, sql.c_str(),
        nullptr, nullptr, &error_message
    );

    if (rc != SQLITE_OK) {
        std::cerr
            << description << "失败："
            << (error_message
                    ? error_message
                    : sqlite3_errmsg(db))
            << '\n';

        sqlite3_free(error_message);
        return false;
    }

    sqlite3_free(error_message);
    return true;
}

// 读取项目原有的温度告警 SQL
bool load_alarm_sql(
    std::string& sql,
    std::string& actual_path)
{
    std::vector<std::string> candidates;

    const char* env_path =
        std::getenv("RK3576_ALARM_SQL_FILE");

    if (env_path && *env_path) {
        candidates.emplace_back(env_path);
    } else {
        candidates.emplace_back(
            "gateway/sql/temperature_alarm.sql"
        );

        candidates.emplace_back(
            "sql/temperature_alarm.sql"
        );
    }

    for (const auto& path : candidates) {
        std::ifstream file(path);

        if (!file.is_open()) {
            continue;
        }

        std::ostringstream buffer;
        buffer << file.rdbuf();

        if (file.bad()) {
            std::cerr
                << "读取告警SQL失败："
                << path << '\n';
            return false;
        }

        sql = buffer.str();
        actual_path = path;

        if (sql.empty()) {
            std::cerr
                << "告警SQL文件为空："
                << path << '\n';
            return false;
        }

        return true;
    }

    std::cerr << "找不到温度告警SQL文件\n";
    return false;
}

} // namespace

Database::~Database()
{
    close();
}

bool Database::open(const std::string& path)
{
    close();

    int rc = sqlite3_open(path.c_str(), &db_);

    if (rc != SQLITE_OK) {
        std::cerr
            << "打开数据库失败："
            << (db_
                    ? sqlite3_errmsg(db_)
                    : "内存不足")
            << '\n';

        close();
        return false;
    }

    sqlite3_busy_timeout(db_, 3000);

    // 读取原来的温度告警规则
    std::string alarm_sql;
    std::string alarm_sql_path;

    if (!load_alarm_sql(
            alarm_sql, alarm_sql_path)) {
        close();
        return false;
    }

    // 保留原始 telemetry 表结构
    const char* create_telemetry_sql = R"SQL(
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

    // 原有数据库结构初始化事务
    if (!execute_sql(
            db_,
            "BEGIN IMMEDIATE TRANSACTION;",
            "开始数据库初始化事务")) {
        close();
        return false;
    }

    if (!execute_sql(
            db_,
            create_telemetry_sql,
            "创建telemetry表")) {

        execute_sql(db_, "ROLLBACK;", "回滚初始化");
        close();
        return false;
    }

    if (!execute_sql(
            db_,
            alarm_sql,
            "初始化温度告警规则")) {

        execute_sql(db_, "ROLLBACK;", "回滚初始化");
        close();
        return false;
    }

    if (!execute_sql(
            db_,
            "COMMIT;",
            "提交数据库初始化事务")) {

        execute_sql(db_, "ROLLBACK;", "回滚初始化");
        close();
        return false;
    }

    std::cout
        << "告警规则初始化成功："
        << alarm_sql_path << '\n';

    // 预编译INSERT语句
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
        db_, insert_sql, -1,
        &insert_stmt_, nullptr
    );

    if (rc != SQLITE_OK) {
        std::cerr
            << "SQL预编译失败："
            << sqlite3_errmsg(db_) << '\n';

        close();
        return false;
    }

    in_transaction_ = false;

    std::cout << "Database模块初始化成功\n";
    return true;
}

// 新增：开始批量事务
bool Database::beginTransaction()
{
    if (!db_ || !insert_stmt_ || in_transaction_) {
        std::cerr
            << "无法开始事务：数据库未就绪"
            << "或事务已经开始\n";
        return false;
    }

    if (!execute_sql(
            db_,
            "BEGIN IMMEDIATE TRANSACTION;",
            "开始批量事务")) {
        return false;
    }

    in_transaction_ = true;
    return true;
}

// 新增：提交批量事务
bool Database::commitTransaction()
{
    if (!db_ || !in_transaction_) {
        std::cerr << "没有可提交的事务\n";
        return false;
    }

    if (!execute_sql(
            db_,
            "COMMIT;",
            "提交批量事务")) {

        // SQLite在某些错误情况下可能自动回滚
        in_transaction_ =
            (sqlite3_get_autocommit(db_) == 0);

        return false;
    }

    in_transaction_ = false;
    return true;
}

// 新增：回滚批量事务
bool Database::rollbackTransaction()
{
    if (!db_ || !in_transaction_) {
        return false;
    }

    bool success = execute_sql(
        db_,
        "ROLLBACK;",
        "回滚批量事务"
    );

    in_transaction_ =
        (sqlite3_get_autocommit(db_) == 0);

    return success;
}

// 保留原有INSERT实现
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

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_text(
            insert_stmt_, 2,
            data.device.c_str(),
            -1, SQLITE_TRANSIENT
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_text(
            insert_stmt_, 3,
            data.group_name.c_str(),
            -1, SQLITE_TRANSIENT
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_double(
            insert_stmt_, 4, data.temp_c
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_double(
            insert_stmt_, 5, data.humidity_pct
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_int(
            insert_stmt_, 6, data.switch_state
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_bind_int(
            insert_stmt_, 7, data.device_counter
        );
    }

    if (rc == SQLITE_OK) {
        rc = sqlite3_step(insert_stmt_);
    }

    bool success = (rc == SQLITE_DONE);

    if (!success) {
        std::cerr
            << "数据库插入失败："
            << sqlite3_errmsg(db_) << '\n';
    }

    sqlite3_reset(insert_stmt_);
    sqlite3_clear_bindings(insert_stmt_);

    return success;
}

void Database::close()
{
    // 如果仍有未提交事务，先回滚
    if (db_ && in_transaction_) {
        rollbackTransaction();
    }

    if (insert_stmt_) {
        sqlite3_finalize(insert_stmt_);
        insert_stmt_ = nullptr;
    }

    if (db_) {
        int rc = sqlite3_close(db_);

        if (rc != SQLITE_OK) {
            std::cerr
                << "关闭数据库失败："
                << sqlite3_errmsg(db_) << '\n';
        } else {
            db_ = nullptr;
        }
    }
}
