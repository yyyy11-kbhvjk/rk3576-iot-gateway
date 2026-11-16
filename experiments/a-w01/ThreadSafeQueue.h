#pragma once

#include <chrono>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <utility>

template <typename T>
class ThreadSafeQueue {
public:
    enum class PushResult {
        Ok,
        Timeout,
        Closed
    };

    explicit ThreadSafeQueue(std::size_t capacity)
        : capacity_(capacity)
    {
        if (capacity == 0) {
            throw std::invalid_argument(
                "Queue capacity must be greater than zero"
            );
        }
    }

    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

    // 阻塞入队：队列满时等待
    bool push(T value)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        not_full_.wait(lock, [this] {
            return closed_ || queue_.size() < capacity_;
        });

        if (closed_) {
            return false;
        }

        queue_.push(std::move(value));

        lock.unlock();
        not_empty_.notify_one();

        return true;
    }

    // 限时入队：队列满时最多等待 timeout
    PushResult try_push_for(
        T value,
        std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        bool ready = not_full_.wait_for(
            lock,
            timeout,
            [this] {
                return closed_ ||
                       queue_.size() < capacity_;
            }
        );

        if (closed_) {
            return PushResult::Closed;
        }

        if (!ready) {
            return PushResult::Timeout;
        }

        queue_.push(std::move(value));

        lock.unlock();
        not_empty_.notify_one();

        return PushResult::Ok;
    }

    // 消费者取数据
    bool pop(T& result)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        not_empty_.wait(lock, [this] {
            return closed_ || !queue_.empty();
        });

        if (queue_.empty()) {
            return false;
        }

        result = std::move(queue_.front());
        queue_.pop();

        lock.unlock();
        not_full_.notify_one();

        return true;
    }

    // 当前排队数量：线程安全的瞬时快照
    std::size_t size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    // 队列最大容量
    std::size_t capacity() const
    {
        return capacity_;
    }

    // 关闭队列并唤醒所有等待线程
    void close()
    {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            closed_ = true;
        }

        not_empty_.notify_all();
        not_full_.notify_all();
    }

private:
    std::queue<T> queue_;

    const std::size_t capacity_;

    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;

    bool closed_ = false;
};
