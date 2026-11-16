#include "ThreadSafeQueue.h"

#include <chrono>
#include <iostream>
#include <thread>

using namespace std::chrono_literals;

using Queue = ThreadSafeQueue<int>;

int main()
{
    std::cout << "===== 测试1：容量检查 =====\n";

    Queue queue(1);

    if (queue.capacity() != 1 || queue.size() != 0) {
        std::cerr << "容量检查失败\n";
        return 1;
    }

    queue.push(100);

    std::cout << "队列容量：" << queue.capacity() << '\n';
    std::cout << "当前数量：" << queue.size() << '\n';

    std::cout << "\n===== 测试2：入队超时 =====\n";

    auto start = std::chrono::steady_clock::now();

    auto result = queue.try_push_for(200, 200ms);

    auto end = std::chrono::steady_clock::now();

    auto elapsed =
        std::chrono::duration_cast<
            std::chrono::milliseconds
        >(end - start).count();

    std::cout << "入队等待：" << elapsed << " ms\n";

    if (result != Queue::PushResult::Timeout) {
        std::cerr << "超时测试失败\n";
        return 1;
    }

    std::cout << "超时测试通过\n";

    std::cout << "\n===== 测试3：释放空位后入队 =====\n";

    int value = 0;

    if (!queue.pop(value) || value != 100) {
        std::cerr << "取出旧数据失败\n";
        return 1;
    }

    result = queue.try_push_for(300, 200ms);

    if (result != Queue::PushResult::Ok) {
        std::cerr << "重新入队失败\n";
        return 1;
    }

    std::cout << "重新入队成功，当前数量："
              << queue.size() << '\n';

    std::cout << "\n===== 测试4：队列关闭 =====\n";

    queue.close();

    result = queue.try_push_for(400, 200ms);

    if (result != Queue::PushResult::Closed) {
        std::cerr << "关闭检测失败\n";
        return 1;
    }

    std::cout << "关闭后拒绝入队：通过\n";

    if (!queue.pop(value) || value != 300) {
        std::cerr << "剩余数据读取失败\n";
        return 1;
    }

    if (queue.pop(value)) {
        std::cerr << "关闭后空队列处理失败\n";
        return 1;
    }

    std::cout << "关闭后排空数据：通过\n";

    std::cout << "\n===== 测试5：等待时空位释放 =====\n";

    Queue another_queue(1);
    another_queue.push(1);

    std::thread consumer([&] {
        std::this_thread::sleep_for(100ms);

        int data = 0;
        another_queue.pop(data);

        std::cout << "消费者取出：" << data << '\n';
    });

    result = another_queue.try_push_for(2, 500ms);

    consumer.join();

    if (result != Queue::PushResult::Ok) {
        std::cerr << "等待后入队失败\n";
        return 1;
    }

    another_queue.close();

    if (!another_queue.pop(value) || value != 2) {
        std::cerr << "新数据验证失败\n";
        return 1;
    }

    std::cout << "等待空位后成功入队：通过\n";

    std::cout << "\n===== 所有测试通过 =====\n";

    return 0;
}
