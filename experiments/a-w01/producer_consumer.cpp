
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <thread>

using namespace std::chrono_literals;

int main()
{
    std::queue<int> tasks;

    std::mutex mtx;
    std::condition_variable not_empty;
    std::condition_variable not_full;

    constexpr std::size_t capacity = 3;
    bool finished = false;

    // 消费者：模拟数据库写入线程
    std::thread consumer([&] {
        while (true) {
            int data;

            {
                std::unique_lock<std::mutex> lock(mtx);

                not_empty.wait(lock, [&] {
                    return !tasks.empty() || finished;
                });

                if (tasks.empty() && finished) {
                    break;
                }

                data = tasks.front();
                tasks.pop();
            }

            // 通知生产者：队列有空位
            not_full.notify_one();

            std::cout << "[消费者] 处理数据: "
                      << data << '\n';

            std::this_thread::sleep_for(150ms);
        }
    });

    // 主线程：模拟 MQTT 消息接收
    for (int i = 1; i <= 10; ++i) {
        {
            std::unique_lock<std::mutex> lock(mtx);

            not_full.wait(lock, [&] {
                return tasks.size() < capacity;
            });

            tasks.push(i);

            std::cout << "[生产者] 收到数据: "
                      << i << '\n';
        }

        not_empty.notify_one();

        std::this_thread::sleep_for(30ms);
    }

    // 通知消费者：不再有新任务
    {
        std::lock_guard<std::mutex> lock(mtx);
        finished = true;
    }

    not_empty.notify_one();

    // 等待消费者处理完剩余数据
    consumer.join();

    std::cout << "全部数据处理完成\n";
    return 0;
}
