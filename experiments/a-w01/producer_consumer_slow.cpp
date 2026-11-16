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

    // 队列最多存放一条等待处理的数据
    constexpr std::size_t capacity = 1;

    bool finished = false;

    // 消费者线程：模拟数据库写入
    std::thread consumer([&] {
        while (true) {
            int data;

            {
                std::unique_lock<std::mutex> lock(mtx);

                // 队列为空时等待生产者
                not_empty.wait(lock, [&] {
                    return !tasks.empty() || finished;
                });

                // 生产者结束且队列已空，退出
                if (tasks.empty() && finished) {
                    break;
                }

                data = tasks.front();
                tasks.pop();
            }

            // 通知生产者：队列现在有空位了
            not_full.notify_one();

            std::cout
                << "[消费者] 处理数据: "
                << data << '\n';

            // 模拟数据库写入需要 500ms
            std::this_thread::sleep_for(500ms);
        }
    });

    // 主线程作为生产者：模拟 MQTT 接收
    for (int i = 1; i <= 10; ++i) {

        {
            std::unique_lock<std::mutex> lock(mtx);

            // 记录开始等待空位的时间
            auto wait_start =
                std::chrono::steady_clock::now();

            // 队列满时等待，等待期间释放互斥锁
            not_full.wait(lock, [&] {
                return tasks.size() < capacity;
            });

            // 记录等待结束时间
            auto wait_end =
                std::chrono::steady_clock::now();

            // 计算等待耗时（毫秒）
            auto wait_ms =
                std::chrono::duration_cast<
                    std::chrono::milliseconds
                >(wait_end - wait_start).count();

            // 放入新数据
            tasks.push(i);

            std::cout
                << "[生产者] 数据 " << i
                << "，等待空位 "
                << wait_ms << " ms\n";
        }

        // 通知消费者：队列有数据了
        not_empty.notify_one();

        // 模拟 MQTT 每 30ms 收到一条消息
        std::this_thread::sleep_for(30ms);
    }

    // 生产者不再生成新数据
    {
        std::lock_guard<std::mutex> lock(mtx);
        finished = true;
    }

    // 唤醒消费者，让其处理剩余数据
    not_empty.notify_one();

    // 等待消费者线程执行结束
    consumer.join();

    std::cout << "全部数据处理完成\n";

    return 0;
}
