#include "database/Database.h"

#include <fstream>
#include <iostream>
#include <string>

Telemetry make_telemetry(int counter, double temp)
{
    Telemetry data{};

    data.source_ts_ms = 1794800000000LL + counter;
    data.device = "batch-aw02-device";
    data.group_name = "group-1";
    data.temp_c = temp;
    data.humidity_pct = 65.0;
    data.switch_state = 1;
    data.device_counter = counter;

    return data;
}

int main()
{
    const std::string path =
        "gateway/data/telemetry_aw02_batch_api_test.db";

    // 避免覆盖已有测试数据
    {
        std::ifstream existing(path);
        if (existing.good()) {
            std::cerr
                << "测试数据库已存在，请先检查："
                << path << '\n';
            return 2;
        }
    }

    Database db;

    if (!db.open(path)) {
        return 1;
    }

    std::cout << "===== 测试1：批量提交 =====\n";

    if (!db.beginTransaction()) {
        return 1;
    }

    // 一次事务写入3条
    if (!db.insert(make_telemetry(1, 23.5)) ||
        !db.insert(make_telemetry(2, 26.5)) ||
        !db.insert(make_telemetry(3, 27.0))) {

        db.rollbackTransaction();
        return 1;
    }

    if (!db.commitTransaction()) {
        db.rollbackTransaction();
        return 1;
    }

    std::cout << "第一批3条提交成功\n";

    std::cout << "===== 测试2：第二批提交 =====\n";

    if (!db.beginTransaction()) {
        return 1;
    }

    if (!db.insert(make_telemetry(4, 25.5)) ||
        !db.insert(make_telemetry(5, 24.5))) {

        db.rollbackTransaction();
        return 1;
    }

    if (!db.commitTransaction()) {
        db.rollbackTransaction();
        return 1;
    }

    std::cout << "第二批2条提交成功\n";

    std::cout << "===== 测试3：事务回滚 =====\n";

    if (!db.beginTransaction()) {
        return 1;
    }

    if (!db.insert(make_telemetry(6, 29.0))) {
        db.rollbackTransaction();
        return 1;
    }

    if (!db.rollbackTransaction()) {
        return 1;
    }

    std::cout << "第6条数据已回滚\n";

    db.close();

    std::cout << "===== 事务接口测试执行完成 =====\n";

    return 0;
}
