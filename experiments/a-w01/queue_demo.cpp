#include "ThreadSafeQueue.h"

#include <chrono>
#include <iostream>
#include <mutex>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    // 创建容量为1的线程安全队列
    ThreadSafeQueue<int> queue(1);

    std::mutex output_mutex;

    // 消费者工作线程
    std::thread consumer([&] {
        int data = 0;

        while (queue.pop(data)) {
            {
                std::lock_guard<std::mutex> lock(
                    output_mutex
                );

                std::cout
                    << "[消费者] 处理数据: "
                    << data << '\n';
            }

            // 模拟 SQLite 写入耗时
            std::this_thread::sleep_for(500ms);
        }

        std::lock_guard<std::mutex> lock(
            output_mutex
        );

        std::cout << "消费者线程正常退出\n";
    });

    // 主线程作为生产者
    for (int i = 1; i <= 10; ++i) {

        auto start =
            std::chrono::steady_clock::now();

        bool success = queue.push(i);

        auto end =
            std::chrono::steady_clock::now();

        if (!success) {
            std::cerr << "队列已关闭\n";
            break;
        }

        auto wait_ms =
            std::chrono::duration_cast<
                std::chrono::milliseconds
            >(end - start).count();

        {
            std::lock_guard<std::mutex> lock(
                output_mutex
            );

            std::cout
                << "[生产者] 数据 " << i
                << "，入队等待 "
                << wait_ms << " ms\n";
        }

        std::this_thread::sleep_for(30ms);
    }

    // 不再允许新任务入队
    queue.close();

    // 等待消费者处理完剩余任务
    consumer.join();

    std::cout << "主线程正常退出\n";
    std::cout << "全部数据处理完成\n";

    return 0;
}
