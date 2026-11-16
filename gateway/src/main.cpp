#include <csignal>
#include <cstdint>
#include <iomanip>
#include <iostream>
#include <string>

#include <nlohmann/json.hpp>

#include "database/Database.h"
#include "mqtt/MqttClient.h"

using json = nlohmann::json;

volatile std::sig_atomic_t running = 1;

void handle_signal(int)
{
    running = 0;
}

int main()
{
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    Database database;

    if (!database.open(
            "gateway/data/telemetry.db")) {
        return 1;
    }

    MqttClient mqtt(
        "rk3576-gateway-v2",
        "127.0.0.1",
        1883,
        "rk3576/modbus/group-1"
    );

    unsigned long saved_count = 0;
    bool storage_error = false;

    // 收到MQTT消息后执行的Lambda
    auto message_handler =
        [&](const std::string& payload)
    {
        try {
            json data = json::parse(payload);
            const json& values = data.at("values");

            Telemetry telemetry{
                data.at("timestamp").get<std::int64_t>(),
                data.at("node").get<std::string>(),
                data.at("group").get<std::string>(),
                values.at("temp_raw").get<int>() / 10.0,
                values.at("humidity_raw").get<int>() / 10.0,
                values.at("switch_state").get<int>(),
                values.at("device_counter").get<int>()
            };

            if (!database.insert(telemetry)) {
                storage_error = true;
                return;
            }

            ++saved_count;

            std::cout << std::fixed
                      << std::setprecision(1);

            std::cout
                << "[已存储 #" << saved_count << "] "
                << "温度=" << telemetry.temp_c << "℃, "
                << "湿度=" << telemetry.humidity_pct << "%, "
                << "开关=" << telemetry.switch_state << ", "
                << "计数器=" << telemetry.device_counter
                << '\n';

        } catch (const std::exception& e) {
            std::cerr << "JSON解析失败："
                      << e.what() << '\n';
        }
    };

    if (!mqtt.start(message_handler)) {
        database.close();
        return 1;
    }

    std::cout << "RK3576模块化工业网关启动\n";

    int exit_code = 0;

    while (running && !storage_error) {
        if (!mqtt.poll()) {
            if (running) {
                exit_code = 1;
            }
            break;
        }
    }

    if (storage_error) {
        exit_code = 1;
    }

    mqtt.stop();
    database.close();

    std::cout << "网关已停止，本次保存 "
              << saved_count << " 条记录\n";

    return exit_code;
}
