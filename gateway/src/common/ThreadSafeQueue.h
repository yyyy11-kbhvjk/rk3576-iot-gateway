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
    using Clock = std::chrono::steady_clock;

    enum class PushResult {
        Ok,
        Timeout,
        Closed
    };

    enum class PopResult {
        Ok,
        Timeout,
        Closed
    };

    explicit ThreadSafeQueue(std::size_t capacity)
        : capacity_(capacity)
    {
        if (capacity_ == 0) {
            throw std::invalid_argument(
                "Queue capacity must be positive"
            );
        }
    }

    ThreadSafeQueue(const ThreadSafeQueue&) = delete;
    ThreadSafeQueue& operator=(const ThreadSafeQueue&) = delete;

    // 普通阻塞入队
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
        update_peak_locked();

        lock.unlock();
        not_empty_.notify_one();

        return true;
    }

    // 限时入队
    PushResult try_push_for(
        T value,
        std::chrono::milliseconds timeout)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        bool ready = not_full_.wait_for(
            lock, timeout, [this] {
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
        update_peak_locked();

        lock.unlock();
        not_empty_.notify_one();

        return PushResult::Ok;
    }

    // 普通阻塞出队
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

    // 新增：等待到指定截止时间
    PopResult pop_until(
        T& result,
        Clock::time_point deadline)
    {
        std::unique_lock<std::mutex> lock(mutex_);

        not_empty_.wait_until(
            lock, deadline, [this] {
                return closed_ || !queue_.empty();
            }
        );

        // 有数据时优先取数据，即使队列已经关闭
        if (!queue_.empty()) {
            result = std::move(queue_.front());
            queue_.pop();

            lock.unlock();
            not_full_.notify_one();

            return PopResult::Ok;
        }

        if (closed_) {
            return PopResult::Closed;
        }

        return PopResult::Timeout;
    }

    std::size_t size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }

    std::size_t peak_size() const
    {
        std::lock_guard<std::mutex> lock(mutex_);
        return peak_size_;
    }

    std::size_t capacity() const
    {
        return capacity_;
    }

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
    // 调用时必须已经持有 mutex_
    void update_peak_locked()
    {
        if (queue_.size() > peak_size_) {
            peak_size_ = queue_.size();
        }
    }

    std::queue<T> queue_;
    const std::size_t capacity_;

    mutable std::mutex mutex_;
    std::condition_variable not_empty_;
    std::condition_variable not_full_;

    std::size_t peak_size_ = 0;
    bool closed_ = false;
};
