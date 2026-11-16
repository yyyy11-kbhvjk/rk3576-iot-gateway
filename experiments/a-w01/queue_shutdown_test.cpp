#include "ThreadSafeQueue.h"

#include <atomic>
#include <chrono>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    std::cout << "===== 测试1：消费者等待时关闭队列 =====\n";

    {
        ThreadSafeQueue<int> queue(1);

        std::atomic<bool> pop_result{true};

        std::thread consumer([&] {
            int value = 0;

            std::cout << "[消费者] 尝试从空队列取数据...\n";

            bool result = queue.pop(value);

            pop_result.store(result);

            std::cout
                << "[消费者] pop 返回："
                << std::boolalpha << result << '\n';
        });

        // 给消费者线程时间进入等待
        std::this_thread::sleep_for(200ms);

        std::cout << "[主线程] 关闭队列\n";
        queue.close();

        consumer.join();

        if (pop_result.load() != false) {
            std::cerr << "测试1失败\n";
            return 1;
        }

        std::cout << "测试1通过\n\n";
    }

    std::cout << "===== 测试2：生产者等待时关闭队列 =====\n";

    {
        ThreadSafeQueue<int> queue(1);

        // 先将队列填满
        queue.push(100);

        std::atomic<bool> push_result{true};

        std::thread producer([&] {
            std::cout
                << "[生产者] 尝试向满队列放入200...\n";

            bool result = queue.push(200);

            push_result.store(result);

            std::cout
                << "[生产者] push 返回："
                << std::boolalpha << result << '\n';
        });

        // 给生产者线程时间进入等待
        std::this_thread::sleep_for(200ms);

        std::cout << "[主线程] 关闭队列\n";
        queue.close();

        producer.join();

        if (push_result.load() != false) {
            std::cerr << "测试2失败：关闭后仍允许入队\n";
            return 1;
        }

        // 关闭后仍可以取出原有数据
        int value = 0;

        bool first_pop = queue.pop(value);

        if (!first_pop || value != 100) {
            std::cerr << "测试2失败：剩余数据异常\n";
            return 1;
        }

        std::cout << "[主线程] 剩余数据：" << value << '\n';

        // 队列已关闭且数据已经取完
        bool second_pop = queue.pop(value);

        if (second_pop) {
            std::cerr << "测试2失败：空队列仍返回成功\n";
            return 1;
        }

        std::cout << "测试2通过\n\n";
    }

    std::cout << "===== 所有关闭测试通过 =====\n";

    return 0;
}
