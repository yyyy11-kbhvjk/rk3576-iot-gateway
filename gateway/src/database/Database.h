#pragma once

#include <cstdint>
#include <string>

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

class Database {
public:
    Database() = default;
    ~Database();

    Database(const Database&) = delete;
    Database& operator=(const Database&) = delete;

    bool open(const std::string& path);
    bool insert(const Telemetry& data);

    // 新增：批量事务接口
    bool beginTransaction();
    bool commitTransaction();
    bool rollbackTransaction();

    void close();

private:
    sqlite3* db_ = nullptr;
    sqlite3_stmt* insert_stmt_ = nullptr;

    // 记录当前是否存在尚未结束的批量事务
    bool in_transaction_ = false;
};
