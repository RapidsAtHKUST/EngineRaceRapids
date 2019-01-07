#pragma once

#include <mutex>
#include <condition_variable>
#include <deque>
#include <chrono>

using namespace std;
using namespace std::chrono;

template<typename T>
class blocking_queue {
private:
    std::mutex d_mutex;
    std::condition_variable d_condition;
    std::deque<T> d_queue;
public:
    void push(T const &value) {
        {
            std::unique_lock<std::mutex> lock(this->d_mutex);
            d_queue.push_front(value);
        }
        this->d_condition.notify_one();
    }

    T pop(double &total_io_sleep_time_) {
        auto range_clock_beg = high_resolution_clock::now();
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(lock, [=] { return !this->d_queue.empty(); });
        T rc(std::move(this->d_queue.back()));
        this->d_queue.pop_back();

        auto range_clock_end = high_resolution_clock::now();
        double sleep_time =
                duration_cast<nanoseconds>(range_clock_end - range_clock_beg).count() /
                static_cast<double>(1000000000);
        total_io_sleep_time_ += sleep_time;
        return rc;
    }

    T pop() {
        std::unique_lock<std::mutex> lock(this->d_mutex);
        this->d_condition.wait(lock, [=] { return !this->d_queue.empty(); });
        T rc(std::move(this->d_queue.back()));
        this->d_queue.pop_back();

        return rc;
    }

    size_t size(){
        std::unique_lock<std::mutex> lock(this->d_mutex);
        return d_queue.size();
    }
};