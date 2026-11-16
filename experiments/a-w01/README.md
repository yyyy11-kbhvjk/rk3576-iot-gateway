# A-W01 Linux C++ 多线程与线程安全队列

## 实验环境

- 开发板：KICKPI K7 / RK3576
- 语言：C++17
- 编译器：GCC 13.3.0
- 系统：Ubuntu Linux

## 实验内容

1. producer_consumer.cpp
   基础生产者—消费者模型。

2. producer_consumer_slow.cpp
   队列背压与生产者等待时间测量。

3. ThreadSafeQueue.h
   通用线程安全有界队列模板。

4. queue_demo.cpp
   通用队列的生产者—消费者测试。

5. queue_shutdown_test.cpp
   关闭队列、唤醒线程、排空数据测试。

6. queue_timeout_test.cpp
   超时入队、容量管理、等待唤醒测试。

## 已验证结果

- 10 条数据全部被消费者处理。
- 容量为 1 的队列能够限制生产者速度。
- 慢速实验中典型入队等待约 470ms。
- 正常关闭后，消费者可以处理剩余数据。
- 关闭队列能够唤醒等待中的线程。
- 三个回归测试均成功，退出码为 0。

## 编译与运行

```bash
mkdir -p build

for name in queue_demo queue_shutdown_test queue_timeout_test
do
    g++ -std=c++17 -Wall -Wextra -pthread \
        "${name}.cpp" -o "build/${name}" || exit 1
done

./build/queue_demo
./build/queue_shutdown_test
./build/queue_timeout_test
```

## 已掌握知识

- std::thread
- std::mutex
- std::unique_lock
- std::lock_guard
- std::condition_variable
- std::queue
- std::chrono
- C++ 模板类
- 生产者—消费者模型
- 有界队列与背压
- 线程生命周期管理

## 下一阶段

A-W02：将 ThreadSafeQueue<T> 接入真实 MQTT 网关，
实现 MQTT 接收与 SQLite 数据库存储线程分离。

注意：内存队列不提供断电数据持久化保证。
后续需要通过 SQLite Outbox 等机制处理可靠存储问题。
