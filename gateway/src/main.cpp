#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdint>
#include <cstdlib>
#include <future>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <nlohmann/json.hpp>

#include "common/ThreadSafeQueue.h"
#include "database/Database.h"
#include "mqtt/MqttClient.h"

using json = nlohmann::json;
using Clock = std::chrono::steady_clock;
using Nanoseconds = std::chrono::nanoseconds;
using Queue = ThreadSafeQueue<Telemetry>;

volatile std::sig_atomic_t running = 1;

void handle_signal(int)
{
    running = 0;
}

int main()
{
    std::signal(SIGINT, handle_signal);
    std::signal(SIGTERM, handle_signal);

    constexpr std::size_t QUEUE_CAPACITY = 128;
    constexpr std::size_t BATCH_SIZE = 20;
    constexpr auto BATCH_WAIT =
        std::chrono::milliseconds(100);

    Queue queue(QUEUE_CAPACITY);

    std::atomic<bool> database_error{false};
    std::atomic<std::uint64_t> saved_count{0};

    // MQTT主线程负责修改
    std::uint64_t queued_count = 0;
    std::uint64_t rejected_count = 0;
    std::uint64_t parse_error_count = 0;
    std::uint64_t enqueue_attempts = 0;

    Nanoseconds enqueue_wait_total{0};
    Nanoseconds enqueue_wait_max{0};

    bool enqueue_error = false;

    // DB线程负责修改，主线程在join后读取
    std::uint64_t committed_batches = 0;
    std::size_t max_batch_size = 0;

    Nanoseconds db_insert_total{0};
    Nanoseconds db_insert_max{0};
    Nanoseconds db_commit_total{0};
    Nanoseconds db_commit_max{0};

    bool has_stored_data = false;
    Clock::time_point first_transaction_begin{};
    Clock::time_point last_transaction_end{};

    const char* db_env =
        std::getenv("RK3576_GATEWAY_DB");

    std::string db_path =
        (db_env && *db_env)
            ? db_env
            : "gateway/data/telemetry.db";

    std::promise<bool> database_ready;
    auto ready_future = database_ready.get_future();

    // DB工作线程
    std::thread db_worker([&] {
        Database database;

        bool opened = database.open(db_path);
        database_ready.set_value(opened);

        if (!opened) {
            database_error.store(true);
            return;
        }

        std::cout
            << "[DB线程] 已启动，批量模式"
            << "，最大批次=" << BATCH_SIZE
            << "，收集等待="
            << BATCH_WAIT.count() << "ms\n";

        std::vector<Telemetry> batch;
        batch.reserve(BATCH_SIZE);

        Telemetry telemetry{};

        // 阻塞等待第一条数据
        while (queue.pop(telemetry)) {
            batch.clear();
            batch.push_back(std::move(telemetry));

            // 固定截止时间，不因新消息而重置
            auto deadline = Clock::now() + BATCH_WAIT;

            while (batch.size() < BATCH_SIZE) {
                auto result =
                    queue.pop_until(telemetry, deadline);

                if (result != Queue::PopResult::Ok) {
                    break;
                }

                batch.push_back(std::move(telemetry));
            }

            auto transaction_begin = Clock::now();

            // 开始批量事务
            bool success = database.beginTransaction();

            Nanoseconds current_insert_total{0};
            Nanoseconds current_insert_max{0};
            Nanoseconds current_commit_time{0};

            if (success) {
                for (const auto& item : batch) {
                    auto begin = Clock::now();

                    bool inserted = database.insert(item);

                    auto end = Clock::now();

                    if (!inserted) {
                        success = false;
                        break;
                    }

                    auto elapsed =
                        std::chrono::duration_cast<Nanoseconds>(
                            end - begin
                        );

                    current_insert_total += elapsed;

                    current_insert_max =
                        std::max(current_insert_max, elapsed);
                }
            }

            // 只有全部INSERT成功才允许提交
            if (success) {
                auto commit_begin = Clock::now();

                success = database.commitTransaction();

                auto commit_end = Clock::now();

                current_commit_time =
                    std::chrono::duration_cast<Nanoseconds>(
                        commit_end - commit_begin
                    );
            }

            if (!success) {
                // 回滚未完成的事务
                database.rollbackTransaction();

                std::cerr
                    << "[DB线程] 批量事务失败，"
                    << "停止接收并报告错误\n";

                database_error.store(true);
                queue.close();
                break;
            }

            auto transaction_end = Clock::now();

            // COMMIT成功后才能统计为已存储
            auto total_saved =
                saved_count.fetch_add(batch.size())
                + batch.size();

            ++committed_batches;

            max_batch_size =
                std::max(max_batch_size, batch.size());

            db_insert_total += current_insert_total;

            db_insert_max =
                std::max(db_insert_max, current_insert_max);

            db_commit_total += current_commit_time;

            db_commit_max =
                std::max(db_commit_max, current_commit_time);

            if (!has_stored_data) {
                first_transaction_begin = transaction_begin;
                has_stored_data = true;
            }

            last_transaction_end = transaction_end;

            // 批次日志
            if (committed_batches == 1 ||
                total_saved % 100 == 0) {

                std::cout
                    << "[DB批量提交] 本批="
                    << batch.size()
                    << "，累计已存储="
                    << total_saved << '\n';
            }
        }

        database.close();

        std::cout << "[DB线程] 已退出\n";
    });

    // 等待数据库初始化
    if (!ready_future.get()) {
        queue.close();
        db_worker.join();

        std::cerr << "数据库初始化失败\n";
        return 1;
    }

    std::cout
        << "数据库初始化完成："
        << db_path << '\n';

    MqttClient mqtt(
        "rk3576-gateway-v2",
        "127.0.0.1",
        1883,
        "rk3576/modbus/group-1"
    );

    // MQTT回调：解析JSON，放入有界队列
    auto message_handler =
        [&](const std::string& payload)
    {
        if (database_error.load() || enqueue_error) {
            ++rejected_count;

            std::cerr
                << "[MQTT线程] 存储链路异常，拒收消息\n";
            return;
        }

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

            auto begin = Clock::now();

            auto result = queue.try_push_for(
                std::move(telemetry),
                std::chrono::milliseconds(10)
            );

            auto end = Clock::now();

            auto elapsed =
                std::chrono::duration_cast<Nanoseconds>(
                    end - begin
                );

            ++enqueue_attempts;
            enqueue_wait_total += elapsed;

            enqueue_wait_max =
                std::max(enqueue_wait_max, elapsed);

            if (result == Queue::PushResult::Ok) {
                ++queued_count;
                return;
            }

            ++rejected_count;
            enqueue_error = true;

            if (result == Queue::PushResult::Timeout) {
                std::cerr
                    << "[MQTT线程] 队列已满，入队超时\n";
            } else {
                std::cerr
                    << "[MQTT线程] 队列已关闭\n";
            }

        } catch (const std::exception& e) {
            ++parse_error_count;

            std::cerr
                << "[MQTT线程] JSON解析或处理失败："
                << e.what() << '\n';
        }
    };

    if (!mqtt.start(message_handler)) {
        std::cerr << "MQTT启动失败\n";

        queue.close();
        db_worker.join();

        return 1;
    }

    std::cout
        << "RK3576工业网关启动：批量事务模式\n"
        << "队列容量：" << QUEUE_CAPACITY << '\n'
        << "最大批次：" << BATCH_SIZE << '\n'
        << "批次收集等待："
        << BATCH_WAIT.count() << "ms\n";

    int exit_code = 0;

    // MQTT主线程
    while (running &&
           !database_error.load() &&
           !enqueue_error) {

        if (!mqtt.poll()) {
            if (running) {
                exit_code = 1;
            }
            break;
        }
    }

    // 正常停机：先停止接收，再排空队列
    mqtt.stop();
    queue.close();

    db_worker.join();

    if (database_error.load() || enqueue_error) {
        exit_code = 1;
    }

    const auto stored = saved_count.load();

    auto to_ms = [](Nanoseconds duration) {
        return std::chrono::duration<double, std::milli>(
            duration
        ).count();
    };

    double avg_enqueue_ms =
        enqueue_attempts == 0
            ? 0.0
            : to_ms(enqueue_wait_total) / enqueue_attempts;

    double avg_insert_ms =
        stored == 0
            ? 0.0
            : to_ms(db_insert_total) / stored;

    double avg_commit_ms =
        committed_batches == 0
            ? 0.0
            : to_ms(db_commit_total) / committed_batches;

    double avg_batch_size =
        committed_batches == 0
            ? 0.0
            : static_cast<double>(stored)
              / committed_batches;

    double write_window_s =
        has_stored_data
            ? std::chrono::duration<double>(
                last_transaction_end
                - first_transaction_begin
              ).count()
            : 0.0;

    double write_rate =
        write_window_s > 0.0
            ? stored / write_window_s
            : 0.0;

    std::cout << std::fixed << std::setprecision(3);

    std::cout
        << "\n========== 网关运行统计 ==========\n"
        << "成功入队：" << queued_count << '\n'
        << "成功存储：" << stored << '\n'
        << "入队拒绝：" << rejected_count << '\n'
        << "解析失败：" << parse_error_count << '\n'
        << "队列容量：" << queue.capacity() << '\n'
        << "队列峰值：" << queue.peak_size() << '\n'
        << "队列剩余：" << queue.size() << '\n'
        << "平均入队耗时："
        << avg_enqueue_ms << " ms\n"
        << "最大入队耗时："
        << to_ms(enqueue_wait_max) << " ms\n"
        << "提交批次数：" << committed_batches << '\n'
        << "平均批次大小：" << avg_batch_size << '\n'
        << "最大实际批次：" << max_batch_size << '\n'
        << "平均单条INSERT耗时："
        << avg_insert_ms << " ms\n"
        << "最大单条INSERT耗时："
        << to_ms(db_insert_max) << " ms\n"
        << "平均每批COMMIT耗时："
        << avg_commit_ms << " ms\n"
        << "最大每批COMMIT耗时："
        << to_ms(db_commit_max) << " ms\n"
        << "首批开始至末批完成："
        << write_window_s << " s\n"
        << "窗口平均存储速率："
        << write_rate << " 条/秒\n"
        << "退出码：" << exit_code << '\n'
        << "==================================\n";

    return exit_code;
}
