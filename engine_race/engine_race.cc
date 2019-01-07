#include <utility>

// Copyright [2018] Alibaba Cloud All rights reserved
#include "engine_race.h"

#include <malloc.h>
#include <byteswap.h>

#include <atomic>
#include <iostream>
#include <chrono>
#include <thread>
#include <algorithm>

#include "log.h"
#include "util.h"
#include "file_util.h"

#define STAT
//#define DSTAT_TESTING
#define FLUSH_IN_WRITER_DESTRUCTOR

#define IO_AFFINITY_EXP
#define WAIT_STAT_RANGE_WORKER

namespace polar_race {
    using namespace std;

    atomic_int write_num_threads(-1);
    atomic_int read_num_threads_count(-1);
    atomic_int range_num_threads_count(-1);

    const char *key_file_name = "polar.keys";
    const char *value_file_name = "polar.values";
    const char *meta_file_name = "/polar.meta";

    const char *key_buf_file_name = "/polar.keybuffers";
    const char *value_buf_file_name = "/polar.valbuffers";

    constexpr size_t tmp_buffer_value_file_size = static_cast<size_t>(VALUE_SIZE) * TMP_VALUE_BUFFER_SIZE * BUCKET_NUM;
    constexpr size_t tmp_buffer_key_file_size = sizeof(uint64_t) * TMP_KEY_BUFFER_SIZE * BUCKET_NUM;

    using namespace std::chrono;
    std::chrono::time_point<std::chrono::high_resolution_clock> clock_start;
    std::chrono::time_point<std::chrono::high_resolution_clock> clock_end;

    bool operator<(KeyEntry l, KeyEntry r) {
        return l.key_ < r.key_;
    }

    uint32_t branchfree_search(KeyEntry *a, uint32_t n, KeyEntry x) {
        using I = uint32_t;
        const KeyEntry *base = a;
        while (n > 1) {
            I half = n / 2;
            __builtin_prefetch(base + half / 2, 0, 0);
            __builtin_prefetch(base + half + half / 2, 0, 0);
            base = (base[half] < x) ? base + half : base;
            n -= half;
        }
        return (*base < x) + base - a;
    }

    inline uint32_t get_par_bucket_id(uint64_t key) {
        return static_cast<uint32_t >((key >> (64 - BUCKET_DIGITS)) & 0xffffffu);
    }

    inline pair<uint32_t, uint64_t> get_key_fid_foff(uint32_t bucket_id, uint32_t bucket_off) {
        constexpr uint32_t BUCKET_NUM_PER_FILE = (BUCKET_NUM / KEY_FILE_NUM);
        uint32_t fid = bucket_id / BUCKET_NUM_PER_FILE;
        uint64_t foff = MAX_KEY_BUCKET_SIZE * (bucket_id % BUCKET_NUM_PER_FILE) + bucket_off;
        return make_pair(fid, foff * sizeof(uint64_t));
    }

    inline pair<uint32_t, uint64_t> get_value_fid_foff(uint32_t bucket_id, uint32_t bucket_off) {
        // Buckets 0,1,2,3... grouped together.
        constexpr uint32_t BUCKET_NUM_PER_FILE = (BUCKET_NUM / VAL_FILE_NUM);
        uint32_t fid = bucket_id / BUCKET_NUM_PER_FILE;
        uint64_t foff = MAX_VAL_BUCKET_SIZE * (bucket_id % BUCKET_NUM_PER_FILE) + bucket_off;
        return make_pair(fid, foff * VALUE_SIZE);
    }

    inline uint32_t get_notify_big_round(uint32_t current_local_offset) {
        return current_local_offset / SHRINK_SYNC_FACTOR;
    }

    RetCode Engine::Open(const std::string &name, Engine **eptr) {
        clock_start = high_resolution_clock::now();
        log_info("sizeof %d, %d, %d", sizeof(off_t), sizeof(off64_t), sizeof(KeyEntry));

        auto ret = EngineRace::Open(name, eptr);
        return ret;
    }

    Engine::~Engine() = default;

/*
 * Complete the functions below to implement you own engine
 */
    EngineRace::EngineRace(const std::string &dir) :
            mmap_meta_cnt_(nullptr), key_file_dp_(nullptr), key_buffer_file_dp_(-1),
            mmap_key_aligned_buffer_(nullptr), mmap_key_aligned_buffer_view_(nullptr),
            value_file_dp_(nullptr), value_buffer_file_dp_(-1),
            mmap_value_aligned_buffer_(nullptr), mmap_value_aligned_buffer_view_(nullptr),
            bucket_mtx_(nullptr), write_barrier_(WRITE_BARRIER_NUM),
            aligned_read_buffer_(nullptr), read_barrier_(NUM_THREADS),
            is_range_init_(false), range_barrier_ptr_(nullptr), polar_keys_(NUM_THREADS),
            total_time_(0), total_blocking_queue_time_(0), total_io_sleep_time_(0), wait_get_time_(0),
            val_buffer_max_size_(0),
            single_range_io_worker_(nullptr),
            bucket_consumed_num_(nullptr), total_range_num_threads_(0) {
        printTS(__FUNCTION__, __LINE__, clock_start);

        const string meta_file_path = dir + meta_file_name;
        const string key_file_path = dir + "/" + key_file_name;
        const string key_buffers_file_path = dir + key_buf_file_name;
        const string value_file_path = dir + "/" + value_file_name;
        const string value_buffers_file_path = dir + value_buf_file_name;

        key_file_dp_ = new int[KEY_FILE_NUM];
        mmap_key_aligned_buffer_view_ = new uint64_t *[BUCKET_NUM];

        value_file_dp_ = new int[BUCKET_NUM];
        mmap_value_aligned_buffer_view_ = new char *[BUCKET_NUM];

        if (!file_exists(meta_file_path.c_str())) {
            // Meta.
            meta_cnt_file_dp_ = open(meta_file_path.c_str(), O_RDWR | O_CREAT, FILE_PRIVILEGE);
            ftruncate(meta_cnt_file_dp_, sizeof(uint32_t) * BUCKET_NUM);
            mmap_meta_cnt_ = (uint32_t *) mmap(nullptr, sizeof(uint32_t) * (BUCKET_NUM),
                                               PROT_READ | PROT_WRITE, MAP_SHARED, meta_cnt_file_dp_, 0);
            memset(mmap_meta_cnt_, 0, sizeof(uint32_t) * (BUCKET_NUM));

            // Write Mutex Array.
            bucket_mtx_ = new mutex[BUCKET_NUM];
            printTS(__FUNCTION__, __LINE__, clock_start);

            // Value.
            for (int i = 0; i < VAL_FILE_NUM; ++i) {
                string temp_value = value_file_path + to_string(i);

                value_file_dp_[i] = open(temp_value.c_str(), O_RDWR | O_CREAT | O_DIRECT, FILE_PRIVILEGE);
                if (value_file_dp_[i] < 0) {
                    log_info("fd err of %d: %d, err info: %s", i, value_file_dp_[i], strerror(errno));
                    exit(-1);
                }
            }
            printTS(__FUNCTION__, __LINE__, clock_start);

            // Value Buffers. (To be sliced into BUCKET_NUM slices)
            value_buffer_file_dp_ = open(value_buffers_file_path.c_str(), O_RDWR | O_CREAT, FILE_PRIVILEGE);
            if (value_buffer_file_dp_ < 0) {
                log_info("valbuf err info: %s", strerror(errno));
                exit(-1);
            }
            ftruncate(value_buffer_file_dp_, tmp_buffer_value_file_size);
            mmap_value_aligned_buffer_ = (char *) mmap(nullptr, tmp_buffer_value_file_size, \
                        PROT_READ | PROT_WRITE, MAP_SHARED, value_buffer_file_dp_, 0);
            for (int i = 0; i < BUCKET_NUM; i++) {
                mmap_value_aligned_buffer_view_[i] =
                        mmap_value_aligned_buffer_ + VALUE_SIZE * TMP_VALUE_BUFFER_SIZE * i;
            }

            // Key.
            for (int i = 0; i < KEY_FILE_NUM; ++i) {
                string temp_key = key_file_path + to_string(i);
                key_file_dp_[i] = open(temp_key.c_str(), O_RDWR | O_CREAT | O_DIRECT, FILE_PRIVILEGE);
                if (key_file_dp_[i] < 0) {
                    log_info("open err: %s", strerror(errno));
                }
            }

            // Key Buffers.  (To be sliced into BUCKET_NUM slices)
            key_buffer_file_dp_ = open(key_buffers_file_path.c_str(), O_RDWR | O_CREAT, FILE_PRIVILEGE);
            ftruncate(key_buffer_file_dp_, tmp_buffer_key_file_size);
            mmap_key_aligned_buffer_ = (uint64_t *) mmap(nullptr, tmp_buffer_key_file_size, \
                        PROT_READ | PROT_WRITE, MAP_SHARED, key_buffer_file_dp_, 0);
            for (int i = 0; i < BUCKET_NUM; i++) {
                mmap_key_aligned_buffer_view_[i] = mmap_key_aligned_buffer_ + TMP_KEY_BUFFER_SIZE * i;
            }
        } else {
            // Meta.
            meta_cnt_file_dp_ = open(meta_file_path.c_str(), O_RDONLY, FILE_PRIVILEGE);
            mmap_meta_cnt_ = (uint32_t *) mmap(nullptr, sizeof(uint32_t) * (BUCKET_NUM),
                                               PROT_READ, MAP_PRIVATE | MAP_POPULATE, meta_cnt_file_dp_, 0);
            // Value.
            for (int i = 0; i < VAL_FILE_NUM; ++i) {
                string temp_value = value_file_path + to_string(i);

                value_file_dp_[i] = open(temp_value.c_str(), O_RDONLY | O_DIRECT, FILE_PRIVILEGE);
            }
            printTS(__FUNCTION__, __LINE__, clock_start);

            // Value Buffers.
            value_buffer_file_dp_ = -1;
            mmap_value_aligned_buffer_ = nullptr;

            // Key.
            for (int i = 0; i < KEY_FILE_NUM; ++i) {
                string temp_key = key_file_path + to_string(i);
                key_file_dp_[i] = open(temp_key.c_str(), O_RDONLY | O_DIRECT, FILE_PRIVILEGE);
            }

            // Key Buffers.
            key_buffer_file_dp_ = -1;
            mmap_key_aligned_buffer_ = nullptr;

            // Thread.
            aligned_read_buffer_ = new char *[NUM_THREADS];
            for (int i = 0; i < NUM_THREADS; ++i) {
                aligned_read_buffer_[i] = (char *) memalign(FILESYSTEM_BLOCK_SIZE, VALUE_SIZE);
            }

            // Flush tmp Files.
            printTS(__FUNCTION__, __LINE__, clock_start);
            FlushTmpFiles(dir);
            printTS(__FUNCTION__, __LINE__, clock_start);

            // Build Index.
            BuildIndex();
            printTS(__FUNCTION__, __LINE__, clock_start);
        }
        clock_end = high_resolution_clock::now();
        printTS(__FUNCTION__, __LINE__, clock_start);
    }

// 1. Open engine
    RetCode EngineRace::Open(const std::string &name, Engine **eptr) {
        printTS(__FUNCTION__, __LINE__, clock_start);
#ifdef DSTAT_TESTING
        DstatThreading();
        IOStatThreading();
#endif
        if (!file_exists(name.c_str())) {
            int ret = mkdir(name.c_str(), 0755);
            if (ret != 0) {
                log_info("Fail to create the target directory %s.", name.c_str());
                exit(-1);
            }
            log_info("Create the target directory %s.", name.c_str());
        }
        *eptr = new EngineRace(name);
        printTS(__FUNCTION__, __LINE__, clock_start);
        return kSucc;
    }

    EngineRace::~EngineRace() {
        printTS(__FUNCTION__, __LINE__, clock_start);
        // Range: Thread.
        if (is_range_init_) {
            for (auto &kv_pair : polar_keys_) {
                if (kv_pair != nullptr)
                    delete[] kv_pair->data();
                delete kv_pair;
            }
            // Join Range Submitter.
            for (int next_future_idx = 0; next_future_idx < BUCKET_NUM * 2; next_future_idx++) {
                free_buffers_->push(nullptr);
            }
            single_range_io_worker_->join();
            // Join Range IO Workers.
            for (uint32_t io_id = 0; io_id < RANGE_QUEUE_DEPTH; io_id++) {
                log_info("notify: %d", io_id);
                range_worker_task_tls_[io_id]->enqueue(UserIOCB(nullptr, FD_FINISHED, 0, 0));
                log_info("join: %d, %d", io_id, io_threads_[io_id].joinable() ? 1 : 0);
                io_threads_[io_id].join();
                log_info("join ok: %d, %d", io_id, io_threads_[io_id].joinable() ? 1 : 0);
            }

            delete range_barrier_ptr_;
            if (total_time_ != 0) {
                log_info("Total Range Time: %.9lf s, wait: %.9lf s,  io thread sleep: %.9lf s, bq-pop: %.9lf s",
                         total_time_, wait_get_time_, total_io_sleep_time_, total_blocking_queue_time_);
            }
        }

        // Thread.
        for (uint32_t i = 0; i < NUM_THREADS; ++i) {
            if (aligned_read_buffer_ != nullptr) {
                free(aligned_read_buffer_[i]);
            }
        }
        delete[] aligned_read_buffer_;

        // Flush If Writer Reach Here.
#ifdef FLUSH_IN_WRITER_DESTRUCTOR
        if (index_.empty()) {
            printTS(__FUNCTION__, __LINE__, clock_start);
            ParallelFlushTmp(key_file_dp_, value_file_dp_);
            printTS(__FUNCTION__, __LINE__, clock_start);
        }
#endif

        // Key Buffers.
        if (mmap_key_aligned_buffer_ != nullptr) {
            int ret = munmap(mmap_key_aligned_buffer_, sizeof(uint64_t) * (size_t) TMP_KEY_BUFFER_SIZE * BUCKET_NUM);
            if (ret < 0) {
                log_info("Key Buffer Munmap Err: %s", strerror(errno));
            }
        }
        delete[] mmap_key_aligned_buffer_view_;
        if (key_buffer_file_dp_ != -1) {
            close(key_buffer_file_dp_);
        }

        // Key.
        for (uint32_t i = 0; i < KEY_FILE_NUM; ++i) {
            close(key_file_dp_[i]);
        }
        delete[] key_file_dp_;

        // Value Buffers.
        if (mmap_value_aligned_buffer_ != nullptr) {
            munmap(mmap_value_aligned_buffer_, (size_t) VALUE_SIZE * TMP_VALUE_BUFFER_SIZE * BUCKET_NUM);
        }
        delete[] mmap_value_aligned_buffer_view_;
        if (value_buffer_file_dp_ != -1) {
            close(value_buffer_file_dp_);
        }
        printTS(__FUNCTION__, __LINE__, clock_start);

        // Value.
        for (uint32_t i = 0; i < VAL_FILE_NUM; ++i) {
            close(value_file_dp_[i]);
        }
        delete[] value_file_dp_;

        // Free indices.
#ifdef ENABLE_INDEX_FREE
        if (!index_.empty()) {
            printTS(__FUNCTION__, __LINE__, clock_start);
            for (uint32_t bucket_id = 0; bucket_id < BUCKET_NUM; bucket_id++) {
                free(index_[bucket_id]);
            }
            printTS(__FUNCTION__, __LINE__, clock_start);
        }
#endif

        // Meta.
        if (mmap_meta_cnt_ != nullptr) {
            munmap(mmap_meta_cnt_, sizeof(uint32_t) * (BUCKET_NUM));
        }
        close(meta_cnt_file_dp_);

#ifdef EANBLE_VALUE_BUFFER_FREE
        printTS(__FUNCTION__, __LINE__, clock_start);
        for (char *ptr: value_shared_buffers_) {
            free(ptr);
        }
        printTS(__FUNCTION__, __LINE__, clock_start);
#endif

        clock_end = high_resolution_clock::now();
        printTS(__FUNCTION__, __LINE__, clock_start);
    }

// 3. Write a key-value pair into engine
    RetCode EngineRace::Write(const PolarString &key, const PolarString &value) {
        static thread_local uint32_t tid = (uint32_t) (++write_num_threads) % NUM_THREADS;
        static thread_local uint32_t local_block_offset = 0;
        uint64_t key_int_big_endian = bswap_64(TO_UINT64(key.data()));
        uint32_t bucket_id = get_par_bucket_id(key_int_big_endian);

#ifdef ENABLE_WRITE_BARRIER
        if (local_block_offset % 100000 == 0 && local_block_offset < 1000000 && tid < WRITE_BARRIER_NUM) {
            write_barrier_.Wait();
        }
#endif
        {
            unique_lock<mutex> lock(bucket_mtx_[bucket_id]);
            // Write value to the value file, with a tmp file as value_buffer.
            uint32_t val_buffer_offset = (mmap_meta_cnt_[bucket_id] % TMP_VALUE_BUFFER_SIZE) * VALUE_SIZE;
            char *value_buffer = mmap_value_aligned_buffer_view_[bucket_id];
            memcpy(value_buffer + val_buffer_offset, value.data(), VALUE_SIZE);

            // Write value to the value file.
            if ((mmap_meta_cnt_[bucket_id] + 1) % TMP_VALUE_BUFFER_SIZE == 0) {
                uint32_t in_bucket_id = mmap_meta_cnt_[bucket_id] - (TMP_VALUE_BUFFER_SIZE - 1);
                uint32_t fid;
                uint64_t foff;
                tie(fid, foff) = get_value_fid_foff(bucket_id, in_bucket_id);
                pwrite(value_file_dp_[fid], value_buffer, VALUE_SIZE * TMP_VALUE_BUFFER_SIZE, foff);
            }

            // Write key to the key file.
            uint32_t key_buffer_offset = (mmap_meta_cnt_[bucket_id] % TMP_KEY_BUFFER_SIZE);
            uint64_t *key_buffer = mmap_key_aligned_buffer_view_[bucket_id];
            key_buffer[key_buffer_offset] = key_int_big_endian;
            if (((mmap_meta_cnt_[bucket_id] + 1) % TMP_KEY_BUFFER_SIZE) == 0) {
                uint32_t in_bucket_id = (mmap_meta_cnt_[bucket_id] - (TMP_KEY_BUFFER_SIZE - 1));

                uint32_t fid;
                uint64_t foff;
                tie(fid, foff) = get_key_fid_foff(bucket_id, in_bucket_id);
                pwrite(key_file_dp_[fid], key_buffer, sizeof(uint64_t) * TMP_KEY_BUFFER_SIZE, foff);
            }

            // Update the meta data.
            mmap_meta_cnt_[bucket_id]++;
        }
        local_block_offset++;
#ifdef STAT
        if (local_block_offset == 1000000) {
            auto last_write_clk = high_resolution_clock::now();
            log_info("Write Stat of tid %d, elapsed time: %.3lf s, ts: %.3lf s",
                     tid, duration_cast<milliseconds>(last_write_clk - clock_start).count() / 1000.0,
                     duration_cast<milliseconds>(last_write_clk.time_since_epoch()).count() / 1000.0);
        }
#endif
        return kSucc;
    }

    void EngineRace::NotifyRandomReader(uint32_t local_block_offset, int64_t tid) {
        uint32_t current_round = local_block_offset - 1;
        if ((current_round % SHRINK_SYNC_FACTOR) == SHRINK_SYNC_FACTOR - 1) {
            uint32_t notify_big_round_idx = get_notify_big_round(current_round);
            if (tid % 2 == 0) {
                notify_queues_[(notify_big_round_idx) % 2 + 2]->enqueue(1);   // Notify This Round
            } else {
                notify_queues_[(notify_big_round_idx + 1) % 2]->enqueue(1);   // Notify Next Round
            }
        }

#ifdef STAT
        if (local_block_offset == 1000000) {
            auto last_write_clk = high_resolution_clock::now();
            log_info("Read Stat of tid %d, elapsed time: %.3lf s, ts: %.3lf s",
                     tid, duration_cast<milliseconds>(last_write_clk - clock_start).count() / 1000.0,
                     duration_cast<milliseconds>(last_write_clk.time_since_epoch()).count() / 1000.0);
        }
#endif
    }

// 4. Read value of a key
    RetCode EngineRace::Read(const PolarString &key, std::string *value) {
        static thread_local int64_t tid = (++read_num_threads_count) % NUM_THREADS;
        static thread_local char *value_buffer = aligned_read_buffer_[tid];
        static thread_local bool is_first_not_found = true;
        static thread_local uint32_t local_block_offset = 0;

        if (local_block_offset == 0) {
            if (tid == 0) {
                notify_queues_.resize(4);
                for (auto i = 0; i < 4; i++) {
                    // Even-0,1  Odd-2,3
                    notify_queues_[i] = new moodycamel::BlockingConcurrentQueue<int32_t>(NUM_THREADS);
                }
                for (uint32_t i = 0; i < NUM_THREADS / 2; i++) {
                    notify_queues_[0]->enqueue(1);
                }
            }
            read_barrier_.Wait();
        }
        uint64_t big_endian_key_uint = bswap_64(TO_UINT64(key.data()));

        KeyEntry tmp{};
        tmp.key_ = big_endian_key_uint;
        auto bucket_id = get_par_bucket_id(big_endian_key_uint);

        auto it = index_[bucket_id] + branchfree_search(index_[bucket_id], mmap_meta_cnt_[bucket_id], tmp);
        local_block_offset++;

        int32_t tmp_val;
        uint32_t current_round = local_block_offset - 1;
        if ((current_round % SHRINK_SYNC_FACTOR) == 0) {
            uint32_t notify_big_round_idx = get_notify_big_round(current_round);
            if (tid % 2 == 0) {
                notify_queues_[notify_big_round_idx % 2]->wait_dequeue(tmp_val);
            } else {
                notify_queues_[notify_big_round_idx % 2 + 2]->wait_dequeue(tmp_val);
            }
        }
        if (it == index_[bucket_id] + mmap_meta_cnt_[bucket_id] || it->key_ != big_endian_key_uint) {
            if (is_first_not_found) {
                log_info("not found in tid: %d\n", tid);
                is_first_not_found = false;
            }
            NotifyRandomReader(local_block_offset, tid);
            return kNotFound;
        }

        uint32_t fid;
        uint64_t foff;
        std::tie(fid, foff) = get_value_fid_foff(bucket_id, it->value_offset_);

        // lock
        pread(value_file_dp_[fid], value_buffer, VALUE_SIZE, foff);
        NotifyRandomReader(local_block_offset, tid);

        value->assign(value_buffer, VALUE_SIZE);
        return kSucc;
    }

    void EngineRace::ReadBucketToBuffer(uint32_t bucket_id, char *value_buffer) {
        auto range_clock_beg = high_resolution_clock::now();

        if (value_buffer == nullptr) {
            return;
        }

        // Get fid, and off.
        uint32_t fid;
        uint64_t foff;
        std::tie(fid, foff) = get_value_fid_foff(bucket_id, 0);

        uint32_t value_num = mmap_meta_cnt_[bucket_id];
        uint32_t remain_value_num = value_num % VAL_AGG_NUM;
        uint32_t total_block_num = (remain_value_num == 0 ? (value_num / VAL_AGG_NUM) :
                                    (value_num / VAL_AGG_NUM + 1));
        uint32_t completed_block_num = 0;
        uint32_t last_block_size = (remain_value_num == 0 ? (VALUE_SIZE * VAL_AGG_NUM) :
                                    (remain_value_num * VALUE_SIZE));
        uint32_t submitted_block_num = 0;
        // Submit to Maintain Queue Depth.
        while (completed_block_num < total_block_num) {
            for (uint32_t io_id = 0; io_id < RANGE_QUEUE_DEPTH; io_id++) {
                // Peek Completions If Possible.
                if (range_worker_status_tls_[io_id] == WORKER_COMPLETED) {
                    completed_block_num++;
                    range_worker_status_tls_[io_id] = WORKER_IDLE;
                }

                // Submit If Possible.
                if (submitted_block_num < total_block_num && range_worker_status_tls_[io_id] == WORKER_IDLE) {
                    size_t offset = submitted_block_num * (size_t) VAL_AGG_NUM * VALUE_SIZE;
                    uint32_t size = (submitted_block_num == (total_block_num - 1) ?
                                     last_block_size : (VAL_AGG_NUM * VALUE_SIZE));
                    range_worker_status_tls_[io_id] = WORKER_SUBMITTED;
                    range_worker_task_tls_[io_id]->enqueue(
                            UserIOCB(value_buffer + offset, value_file_dp_[fid], size, offset + foff));
                    submitted_block_num++;
                }
            }
        }

        auto range_clock_end = high_resolution_clock::now();
        double elapsed_time = duration_cast<nanoseconds>(range_clock_end - range_clock_beg).count() /
                              static_cast<double>(1000000000);
        total_time_ += elapsed_time;
#ifdef STAT
        if (bucket_id < MAX_TOTAL_BUFFER_NUM + 8 || bucket_id % 64 == 63) {
            double bucket_size = static_cast<double>(mmap_meta_cnt_[bucket_id] * VALUE_SIZE) / (1024. * 1024.);
            log_info(
                    "In Bucket %d, Free Buf: %d, Read time %.9lf s, Acc time: %.9lf s, "
                    "Bucket size: %.6lf MB, Speed: %.6lf MB/s",
                    bucket_id, free_buffers_->size(), elapsed_time,
                    total_time_, bucket_size, bucket_size / elapsed_time);
        }
#endif
        if (bucket_id == BUCKET_NUM - 1) {
            printTS(__FUNCTION__, __LINE__, clock_start);
        }
    }

    void EngineRace::InitPollingContext() {
        io_threads_ = vector<thread>(RANGE_QUEUE_DEPTH);
        range_worker_task_tls_.resize(RANGE_QUEUE_DEPTH);
        range_worker_status_tls_ = new atomic_int[RANGE_QUEUE_DEPTH];
        for (uint32_t io_id = 0; io_id < RANGE_QUEUE_DEPTH; io_id++) {
            range_worker_task_tls_[io_id] = new moodycamel::BlockingConcurrentQueue<UserIOCB>();
            range_worker_status_tls_[io_id] = WORKER_IDLE;
            io_threads_[io_id] = thread([this, io_id]() {
                UserIOCB user_iocb;
#ifdef IO_AFFINITY_EXP
                setThreadSelfAffinity(io_id);
#endif
                double wait_time = 0;
                for (;;) {
                    // Statistics here.
#ifdef WAIT_STAT_RANGE_WORKER
                    auto clock_beg = high_resolution_clock::now();
#endif
                    range_worker_task_tls_[io_id]->wait_dequeue(user_iocb);

#ifdef WAIT_STAT_RANGE_WORKER
                    auto clock_end = high_resolution_clock::now();
                    wait_time += static_cast<double>(duration_cast<nanoseconds>(clock_end - clock_beg).count()) /
                                 1000000000.;
#endif
                    if (user_iocb.fd_ == FD_FINISHED) {
#ifdef WAIT_STAT_RANGE_WORKER
                        log_info("yes! notified, %d, total wait time: %.6lf s", io_id, wait_time);
#else
                        log_info("yes! notified, %d", io_id);
#endif
                        break;
                    } else {
                        pread(user_iocb.fd_, user_iocb.buffer_, user_iocb.size_, user_iocb.offset_);
                        range_worker_status_tls_[io_id] = WORKER_COMPLETED;
                    }
                }
            });
        }
    }

    void EngineRace::InitRangeReader() {
        for (int i = 0; i < BUCKET_NUM; i++) {
            val_buffer_max_size_ = max<uint64_t>(val_buffer_max_size_, mmap_meta_cnt_[i]);
        }
        val_buffer_max_size_ *= VALUE_SIZE;
        log_info("Max Buffer Size: %zu B", val_buffer_max_size_);
        value_shared_buffers_ = vector<char *>(MAX_TOTAL_BUFFER_NUM);
        for (uint32_t i = 0; i < MAX_TOTAL_BUFFER_NUM; i++) {
            value_shared_buffers_[i] = (char *) memalign(FILESYSTEM_BLOCK_SIZE, val_buffer_max_size_);
        }
        // Value Files.
        free_buffers_ = new blocking_queue<char *>();
        bucket_consumed_num_ = new atomic_int[BUCKET_NUM * 2];
        futures_.resize(BUCKET_NUM * 2);
        cached_front_buffers_.resize(KEEP_REUSE_BUFFER_NUM);
    }

    void EngineRace::InitForRange(int64_t tid) {
        static thread_local bool is_first = true;
        if (!is_range_init_) {
            unique_lock<mutex> lock(range_mtx_);
            if (tid == 0) {
                auto range_clock_beg = high_resolution_clock::now();
                InitRangeReader();
                InitPollingContext();
                auto range_clock_end = high_resolution_clock::now();
                double elapsed_time = duration_cast<nanoseconds>(range_clock_end - range_clock_beg).count() /
                                      static_cast<double>(1000000000);
                log_info("Elapsed time in first sync, %.9lf s", elapsed_time);

                // Init Barrier, Notify All.
                total_range_num_threads_ = range_num_threads_count + 1;
                if (total_range_num_threads_ > 1 && total_range_num_threads_ != NUM_THREADS) {
                    // Do not Sleep, But With Calibration.
                    log_info("Update Number of Threads Correctly");
                    total_range_num_threads_ = NUM_THREADS;
                }
                // Init IO Threads Polling.

                range_barrier_ptr_ = new Barrier(static_cast<size_t>(total_range_num_threads_));
                log_info("Total number of range threads: %zu", total_range_num_threads_);
                is_range_init_ = true;
                range_init_cond_.notify_all();
            } else {
                if (!is_range_init_) {
                    range_init_cond_.wait(lock, [this]() { return is_range_init_; });
                }
            }
        }

        if (is_first && tid < MAX_TOTAL_BUFFER_NUM) {
            // Really populate the physical memory.
            log_info("Tid: %d, Load Physical Mem %d", tid, tid);
            for (uint32_t off = 0; off < val_buffer_max_size_; off += FILESYSTEM_BLOCK_SIZE) {
                value_shared_buffers_[tid][off] = -1;
            }
            is_first = false;
            log_info("Tid: %d, Load Physical Mem Finish %d", tid, tid);
        }
        range_barrier_ptr_->Wait();

        if (tid == 0) {
            // Submit All IO Jobs.
            printTS(__FUNCTION__, __LINE__, clock_start);
            promises_.resize(BUCKET_NUM * 2);
            for (int i = 0; i < BUCKET_NUM * 2; i++) {
                futures_[i] = promises_[i].get_future();
            }
            if (single_range_io_worker_ != nullptr) {
                single_range_io_worker_->join();
            }
            single_range_io_worker_ = new thread([this]() {
                // Odd Round.
                log_info("In Range IO");
                for (uint32_t next_bucket_idx = 0; next_bucket_idx < BUCKET_NUM; next_bucket_idx++) {
                    // 1st: Pop Buffer.
                    auto range_clock_beg = high_resolution_clock::now();
                    char *buffer = free_buffers_->pop(total_io_sleep_time_);
                    auto range_clock_end = high_resolution_clock::now();
                    double elapsed_time =
                            duration_cast<nanoseconds>(range_clock_end - range_clock_beg).count() /
                            static_cast<double>(1000000000);
                    total_blocking_queue_time_ += elapsed_time;

                    // 2nd: Read
                    ReadBucketToBuffer(next_bucket_idx, buffer);
                    promises_[next_bucket_idx].set_value(buffer);
                }
                log_info("In Range IO, Finish Odd Round");

                // Even Round.
                for (uint32_t next_bucket_idx = 0; next_bucket_idx < BUCKET_NUM; next_bucket_idx++) {
                    uint32_t future_id = next_bucket_idx + BUCKET_NUM;
                    char *buffer;
                    if (next_bucket_idx >= KEEP_REUSE_BUFFER_NUM) {
                        // 1st: Pop Buffer.
                        auto range_clock_beg = high_resolution_clock::now();
                        buffer = free_buffers_->pop(total_io_sleep_time_);
                        auto range_clock_end = high_resolution_clock::now();
                        double elapsed_time =
                                duration_cast<nanoseconds>(range_clock_end - range_clock_beg).count() /
                                static_cast<double>(1000000000);
                        total_blocking_queue_time_ += elapsed_time;

                        // 2nd: Read
                        ReadBucketToBuffer(next_bucket_idx, buffer);
                    } else {
                        buffer = cached_front_buffers_[next_bucket_idx];
                    }
                    promises_[future_id].set_value(buffer);
                }
                log_info("In Range IO, Finish Even Round");
            });
            printTS(__FUNCTION__, __LINE__, clock_start);

            for (uint32_t i = 0; i < MAX_TOTAL_BUFFER_NUM; i++) {
                free_buffers_->push(value_shared_buffers_[i]);
            }
            for (uint32_t i = 0; i < BUCKET_NUM * 2; i++) {
                bucket_consumed_num_[i].store(0);
            }
            printTS(__FUNCTION__, __LINE__, clock_start);
        }

        range_barrier_ptr_->Wait();
    }

// 5. Applies the given Vistor::Visit function to the result
// of every key-value pair in the key range [first, last),
// in order
// lower=="" is treated as a key before all keys in the database.
// upper=="" is treated as a key after all keys in the database.
// Therefore the following call will traverse the entire database:
//   Range("", "", visitor)
    RetCode EngineRace::Range(const PolarString &lower, const PolarString &upper,
                              Visitor &visitor) {
        static thread_local int64_t tid = (++range_num_threads_count) % NUM_THREADS;
        static thread_local uint32_t bucket_future_id_beg = 0;

        static thread_local PolarString *polar_key_ptr_;
        static thread_local PolarString polar_val_ptr_;

        // Thread Local Key/Value Init.
        if (bucket_future_id_beg == 0) {
            char *key_chars = new char[sizeof(uint64_t)];
            polar_keys_[tid] = new PolarString(key_chars, sizeof(uint64_t));
            polar_key_ptr_ = polar_keys_[tid];
        }
        if (bucket_future_id_beg % (BUCKET_NUM * 2) == 0) {
            InitForRange(tid);
        }

        if (tid == 0) {
            printTS(__FUNCTION__, __LINE__, clock_start);
        }
        // 2-level Loop.
        uint32_t lower_key_par_id = 0;
        uint32_t upper_key_par_id = BUCKET_NUM - 1;
        for (uint32_t bucket_id = lower_key_par_id; bucket_id < upper_key_par_id + 1; bucket_id++) {
            range_barrier_ptr_->Wait();

            uint32_t future_id = bucket_id + bucket_future_id_beg;
            char *shared_buffer;
            uint32_t relative_id = future_id % (2 * BUCKET_NUM);
            if (relative_id >= BUCKET_NUM && relative_id < BUCKET_NUM + KEEP_REUSE_BUFFER_NUM) {
                shared_buffer = cached_front_buffers_[relative_id - BUCKET_NUM];
            } else {
                if (tid == 0) {
                    auto wait_start_clock = high_resolution_clock::now();
                    shared_buffer = futures_[future_id].get();
                    auto wait_end_clock = high_resolution_clock::now();
                    double elapsed_time = duration_cast<nanoseconds>(wait_end_clock - wait_start_clock).count() /
                                          static_cast<double>(1000000000);
#ifdef STAT
                    if (bucket_id < MAX_TOTAL_BUFFER_NUM) {
                        log_info("Elapsed Wait For Bucket %d: %.6lf s", bucket_id, elapsed_time);
                    }
#endif
                    wait_get_time_ += elapsed_time;
                } else {
                    shared_buffer = futures_[future_id].get();
                }
            }

            uint32_t in_par_id_beg = 0;
            uint32_t in_par_id_end = mmap_meta_cnt_[bucket_id];
            uint64_t prev_key = 0;
            for (uint32_t in_par_id = in_par_id_beg; in_par_id < in_par_id_end; in_par_id++) {
                // Skip the equalities.
                uint64_t big_endian_key = index_[bucket_id][in_par_id].key_;
                if (in_par_id != in_par_id_beg) {
                    if (big_endian_key == prev_key) {
                        continue;
                    }
                }
                prev_key = big_endian_key;

                // Key (to little endian first).
                (*(uint64_t *) polar_key_ptr_->data()) = bswap_64(big_endian_key);

                // Value.
                uint64_t val_id = index_[bucket_id][in_par_id].value_offset_;
                polar_val_ptr_ = PolarString(shared_buffer + val_id * VALUE_SIZE, VALUE_SIZE);

                // Visit Key/Value.
                visitor.Visit(*polar_key_ptr_, polar_val_ptr_);
            }

            // End of inner loop, Submit IO Jobs.
            int32_t my_order = ++bucket_consumed_num_[future_id];
            if (my_order == total_range_num_threads_) {
                if ((future_id % (2 * BUCKET_NUM)) < KEEP_REUSE_BUFFER_NUM) {
                    cached_front_buffers_[future_id] = shared_buffer;
                } else {
                    free_buffers_->push(shared_buffer);
                }
            }
        }

        bucket_future_id_beg += BUCKET_NUM;
        if (tid == 0) { log_info("one round ok..."); }
        range_barrier_ptr_->Wait();
        return kSucc;
    }

    void EngineRace::ParallelFlushTmp(int *key_fds, int *val_fds) {
        vector<thread> workers(NUM_FLUSH_TMP_THREADS);
        for (uint32_t tid = 0; tid < NUM_FLUSH_TMP_THREADS; ++tid) {
            workers[tid] = thread([tid, this, val_fds, key_fds]() {
                // Flush Values.
                for (uint32_t bucket_id = tid; bucket_id < BUCKET_NUM; bucket_id += NUM_FLUSH_TMP_THREADS) {
                    mmap_value_aligned_buffer_view_[bucket_id] =
                            mmap_value_aligned_buffer_ + VALUE_SIZE * TMP_VALUE_BUFFER_SIZE * bucket_id;
                    if (mmap_meta_cnt_[bucket_id] % TMP_VALUE_BUFFER_SIZE != 0) {
                        uint32_t fid;
                        uint64_t foff;
                        uint32_t in_bucked_off = mmap_meta_cnt_[bucket_id] / TMP_VALUE_BUFFER_SIZE *
                                                 TMP_VALUE_BUFFER_SIZE;
                        tie(fid, foff) = get_value_fid_foff(bucket_id, in_bucked_off);

                        size_t write_length = (mmap_meta_cnt_[bucket_id] % TMP_VALUE_BUFFER_SIZE) * VALUE_SIZE;
                        pwrite(val_fds[fid], mmap_value_aligned_buffer_view_[bucket_id], write_length, foff);
                    }
                }
                // Flush Keys.
                for (uint32_t bucket_id = tid; bucket_id < BUCKET_NUM; bucket_id += NUM_FLUSH_TMP_THREADS) {
                    mmap_key_aligned_buffer_view_[bucket_id] =
                            mmap_key_aligned_buffer_ + TMP_KEY_BUFFER_SIZE * bucket_id;
                    if ((mmap_meta_cnt_[bucket_id] % TMP_KEY_BUFFER_SIZE) != 0) {
                        uint32_t fid;
                        uint64_t foff;
                        tie(fid, foff) = get_key_fid_foff(
                                bucket_id, mmap_meta_cnt_[bucket_id] / TMP_KEY_BUFFER_SIZE * TMP_KEY_BUFFER_SIZE);
                        size_t write_length = (TMP_KEY_BUFFER_SIZE) * sizeof(uint64_t);

                        pwrite(key_fds[fid], mmap_key_aligned_buffer_view_[bucket_id], write_length, foff);
                    }
                }
            });
        }
        for (uint32_t tid = 0; tid < NUM_FLUSH_TMP_THREADS; ++tid) {
            workers[tid].join();
        }
        ftruncate(value_buffer_file_dp_, 0);
        ftruncate(key_buffer_file_dp_, 0);
    }

    void EngineRace::FlushTmpFiles(string dir) {
        const string tmp_value_file_path = dir + value_buf_file_name;
        const string tmp_key_file_path = dir + key_buf_file_name;

        if (file_size(tmp_value_file_path.c_str()) > 0) {
            printTS(__FUNCTION__, __LINE__, clock_start);

            value_buffer_file_dp_ = open(tmp_value_file_path.c_str(), O_RDWR, FILE_PRIVILEGE);
            mmap_value_aligned_buffer_ = (char *) mmap(nullptr, tmp_buffer_value_file_size, PROT_READ,
                                                       MAP_PRIVATE, value_buffer_file_dp_, 0);

            key_buffer_file_dp_ = open(tmp_key_file_path.c_str(), O_RDWR, FILE_PRIVILEGE);
            mmap_key_aligned_buffer_ = (uint64_t *) mmap(nullptr, tmp_buffer_key_file_size, PROT_READ,
                                                         MAP_PRIVATE, key_buffer_file_dp_, 0);
            printTS(__FUNCTION__, __LINE__, clock_start);
            vector<int> val_fds(VAL_FILE_NUM);
            vector<int> key_fds(KEY_FILE_NUM);
            for (int i = 0; i < VAL_FILE_NUM; i++) {
                string value_file_path = dir + "/" + value_file_name + to_string(i);
                val_fds[i] = open(value_file_path.c_str(), O_WRONLY | O_DIRECT, FILE_PRIVILEGE);
            }
            for (int i = 0; i < KEY_FILE_NUM; i++) {
                string key_file_path = dir + "/" + key_file_name + to_string(i);
                key_fds[i] = open(key_file_path.c_str(), O_WRONLY | O_DIRECT, FILE_PRIVILEGE);
            }
            printTS(__FUNCTION__, __LINE__, clock_start);
            ParallelFlushTmp(&key_fds.front(), &val_fds.front());
            printTS(__FUNCTION__, __LINE__, clock_start);

            for (int i = 0; i < VAL_FILE_NUM; i++) {
                close(val_fds[i]);
            }
            for (int i = 0; i < KEY_FILE_NUM; i++) {
                close(key_fds[i]);
            }
            printTS(__FUNCTION__, __LINE__, clock_start);
        }

        clock_end = high_resolution_clock::now();
        log_info("After Flush Files, time: %.3lf s",
                 duration_cast<milliseconds>(clock_end - clock_start).count() / 1000.0);
    }

    void EngineRace::BuildIndex() {
        // Key Cnt, Index Allocation.
        index_ = vector<KeyEntry *>(BUCKET_NUM, nullptr);
        for (int key_par_id = 0; key_par_id < BUCKET_NUM; key_par_id++) {
            index_[key_par_id] = static_cast<KeyEntry *>(malloc(mmap_meta_cnt_[key_par_id] * sizeof(KeyEntry)));
        }

        // Read each key file.
        auto **local_buffers_g = new uint64_t *[NUM_READ_KEY_THREADS];
        for (uint32_t tid = 0; tid < NUM_READ_KEY_THREADS; ++tid) {
            local_buffers_g[tid] = (uint64_t *) memalign(FILESYSTEM_BLOCK_SIZE,
                                                         sizeof(uint64_t) * KEY_READ_BLOCK_COUNT);
        }
        vector<thread> workers(NUM_READ_KEY_THREADS);
        for (uint32_t tid = 0; tid < NUM_READ_KEY_THREADS; ++tid) {
            workers[tid] = thread([tid, local_buffers_g, this]() {
                uint64_t *local_buffer = local_buffers_g[tid];
                uint32_t avg = BUCKET_NUM / NUM_READ_KEY_THREADS;
                for (uint32_t bucket_id = tid * avg; bucket_id < (tid + 1) * avg; bucket_id++) {
                    uint32_t entry_count = mmap_meta_cnt_[bucket_id];
                    if (entry_count > 0) {
                        uint32_t passes = entry_count / KEY_READ_BLOCK_COUNT;
                        uint32_t remain_entries_count = entry_count - passes * KEY_READ_BLOCK_COUNT;
                        uint32_t file_offset = 0;

                        auto fid_foff = get_key_fid_foff(bucket_id, 0);
                        uint32_t key_fid = fid_foff.first;
                        size_t read_offset = fid_foff.second;
                        for (uint32_t j = 0; j < passes; ++j) {
                            auto ret = pread(key_file_dp_[key_fid], local_buffer,
                                             KEY_READ_BLOCK_COUNT * sizeof(uint64_t), read_offset);
                            if (ret != KEY_READ_BLOCK_COUNT * sizeof(uint64_t)) {
                                log_info("ret: %d, err: %s", ret, strerror(errno));
                            }
                            for (uint32_t k = 0; k < KEY_READ_BLOCK_COUNT; k++) {
                                index_[bucket_id][file_offset].key_ = local_buffer[k];
                                index_[bucket_id][file_offset].value_offset_ = file_offset;
                                file_offset++;
                            }
                            read_offset += KEY_READ_BLOCK_COUNT * sizeof(uint64_t);
                        }

                        if (remain_entries_count != 0) {
                            size_t num_bytes = (remain_entries_count * sizeof(uint64_t) + FILESYSTEM_BLOCK_SIZE - 1) /
                                               FILESYSTEM_BLOCK_SIZE * FILESYSTEM_BLOCK_SIZE;
                            auto ret = pread(key_file_dp_[key_fid], local_buffer, num_bytes, read_offset);
                            if (ret < static_cast<ssize_t>(remain_entries_count * sizeof(uint64_t))) {
                                log_info("ret: %d, err: %s, fid:%zu off: %zu", ret, strerror(errno), key_fid,
                                         read_offset);
                            }
                            for (uint32_t k = 0; k < remain_entries_count; k++) {
                                index_[bucket_id][file_offset].key_ = local_buffer[k];
                                index_[bucket_id][file_offset].value_offset_ = file_offset;
                                file_offset++;
                            }
                        }
                        sort(index_[bucket_id], index_[bucket_id] + entry_count, [](KeyEntry l, KeyEntry r) {
                            if (l.key_ == r.key_) {
                                return l.value_offset_ > r.value_offset_;
                            } else {
                                return l.key_ < r.key_;
                            }
                        });
                    }
                }
            });
        }
        for (uint32_t i = 0; i < NUM_READ_KEY_THREADS; ++i) {
            workers[i].join();
            free(local_buffers_g[i]);
        }
        delete[]local_buffers_g;
    }
}  // namespace polar_race
