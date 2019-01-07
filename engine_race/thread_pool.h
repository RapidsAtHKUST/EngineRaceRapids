#pragma once

#include <vector>
#include <queue>
#include <memory>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <future>
#include <functional>
#include <stdexcept>
#include <chrono>
//#include "util.h"
#include "log.h"

using namespace std;
using namespace std::chrono;

#define POOL_STAT

// https://github.com/progschj/ThreadPool
class ThreadPool {
public:
    explicit ThreadPool(size_t threads);

    template<class F, class... Args>
    auto enqueue(F &&f, Args &&... args)
    -> std::future<typename std::result_of<F(Args...)>::type>;

    ~ThreadPool();

private:
    // need to keep track of threads so we can join them
    std::vector<std::thread> workers;
    // the task queue
    std::queue<std::function<void()>> tasks;

    // synchronization
    std::mutex queue_mutex;
    std::condition_variable condition;
#ifdef POOL_STAT
    double iter_time;
    double wait_time;
    double task_time;
#endif
    bool stop;
};

// the constructor just launches some amount of workers
ThreadPool::ThreadPool(size_t threads) :
#ifdef POOL_STAT
        iter_time(0), wait_time(0), task_time(0),
#endif
        stop(false) {
    workers.reserve(threads);
//    std::vector<int32_t> affinity_arr_ = {44, 45, 46, 47, 60, 61, 62, 63};
    for (size_t i = 0; i < threads; ++i)
        workers.emplace_back(
//                [this, i, &affinity_arr_] {
//                    setThreadSelfAffinity(affinity_arr_[i]);
                [this] {
                    for (;;) {
                        std::function<void()> task;
                        {
#ifdef POOL_STAT
                            auto clock_beg = high_resolution_clock::now();
#endif
                            std::unique_lock<std::mutex> lock(this->queue_mutex);
                            this->condition.wait(lock, [this] { return this->stop || !this->tasks.empty(); });

#ifdef POOL_STAT
                            auto clock_end = high_resolution_clock::now();
                            wait_time += duration_cast<nanoseconds>(clock_end - clock_beg).count() /
                                         static_cast<double>(1000000000);

                            clock_beg = high_resolution_clock::now();
#endif
                            if (this->stop && this->tasks.empty()) { return; }

                            task = std::move(this->tasks.front());
                            this->tasks.pop();
#ifdef POOL_STAT

                            clock_end = high_resolution_clock::now();
                            iter_time += duration_cast<nanoseconds>(clock_end - clock_beg).count() /
                                         static_cast<double>(1000000000);
#endif
                        }
                        auto clock_beg = high_resolution_clock::now();
                        task();
                        auto clock_end = high_resolution_clock::now();
#ifdef POOL_STAT
                        task_time += duration_cast<nanoseconds>(clock_end - clock_beg).count() /
                                     static_cast<double>(1000000000);
#endif
                    }
                }
        );
}

// add new work item to the pool
template<class F, class... Args>
auto ThreadPool::enqueue(F &&f, Args &&... args) -> std::future<typename std::result_of<F(Args...)>::type> {
    using return_type = typename std::result_of<F(Args...)>::type;

    auto task = std::make_shared<std::packaged_task<return_type()>>(
            std::bind(std::forward<F>(f), std::forward<Args>(args)...)
    );

    std::future<return_type> res = task->get_future();
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        // don't allow enqueueing after stopping the pool
        if (stop) { throw std::runtime_error("enqueue on stopped ThreadPool"); }

        tasks.emplace([task]() { (*task)(); });
    }
    condition.notify_one();

    return res;
}

// the destructor joins all threads
ThreadPool::~ThreadPool() {
#ifdef POOL_STAT
    log_info("Pool QueueOp Time: %.6lf s, CondWait Time: %.6lf s, Task Time: %.6lf s", iter_time, wait_time, task_time);
#endif
    {
        std::unique_lock<std::mutex> lock(queue_mutex);
        stop = true;
    }
    condition.notify_all();

    for (auto &worker: workers) { worker.join(); }
}