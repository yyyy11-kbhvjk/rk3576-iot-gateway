#pragma once

#include <cstdint>
#include <string>

// 前向声明SQLite类型
struct sqlite3;
struct sqlite3_stmt;

// 一条工业设备采集记录
struct Telemetry {
    std::int64_t source_ts_ms;
    std::string device;
    std::string group_name;
    double temp_c;
    double humidity_pct;
    int switch_state;
    int device_counter;
};

// SQLite数据库管理类
class Database {
public:
    Database() = default;
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    bool open(const std::string& path);
    bool insert(const Telemetry& data);
    void close();

private:
    sqlite3* db_ = nullptr;
    sqlite3_stmt* insert_stmt_ = nullptr;
};
