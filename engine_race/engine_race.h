// Copyright [2018] Alibaba Cloud All rights reserved
#ifndef ENGINE_RACE_ENGINE_RACE_H_
#define ENGINE_RACE_ENGINE_RACE_H_

#include <string>
#include <mutex>
#include <vector>
#include <atomic>
#include <list>

#include "include/engine.h"
#include "barrier.h"
#include "thread_pool.h"
#include "blocking_queue.h"
#include "blockingconcurrentqueue.h"

#define TO_UINT64(buffer) (*(uint64_t*)(buffer))

#define FILE_PRIVILEGE (0644)
#define FILESYSTEM_BLOCK_SIZE (4096)
#define NUM_THREADS (64)

// Buffers.
#define TMP_KEY_BUFFER_SIZE (512)
#define TMP_VALUE_BUFFER_SIZE (4)
// Key/Value Files.
#define VALUE_SIZE (4096)

// Buckets.
#define BUCKET_DIGITS (10)      // k-v-buckets must be the same for the range query
#define BUCKET_NUM (1 << BUCKET_DIGITS)

// Max Bucket Size * BUCKET_NUM.
#define MAX_TOTAL_SIZE (68 * 1024 * 1024)

#define KEY_FILE_DIGITS (5)     // must make sure same bucket in the same file
#define KEY_FILE_NUM (1 << KEY_FILE_DIGITS)
#define MAX_KEY_BUCKET_SIZE (MAX_TOTAL_SIZE / BUCKET_NUM / FILESYSTEM_BLOCK_SIZE * FILESYSTEM_BLOCK_SIZE)

#define VAL_FILE_DIGITS (5)
#define VAL_FILE_NUM (1 << VAL_FILE_DIGITS)  // must make sure same bucket in the same file
#define MAX_VAL_BUCKET_SIZE (MAX_TOTAL_SIZE / BUCKET_NUM / FILESYSTEM_BLOCK_SIZE * FILESYSTEM_BLOCK_SIZE)

// Write.
#define WRITE_BARRIER_NUM (16)
// Read.
#define NUM_READ_KEY_THREADS (NUM_THREADS)
#define NUM_FLUSH_TMP_THREADS (32u)
#define KEY_READ_BLOCK_COUNT (8192u)
// Range.
#define RECYCLE_BUFFER_NUM (2u)
#define KEEP_REUSE_BUFFER_NUM (3u)
#define MAX_TOTAL_BUFFER_NUM (RECYCLE_BUFFER_NUM + KEEP_REUSE_BUFFER_NUM)

#define SHRINK_SYNC_FACTOR (2)      // should be divided

// Range Thread Pool.
#define RANGE_QUEUE_DEPTH (8u)
#define VAL_AGG_NUM (32)

#define WORKER_IDLE (0)
#define WORKER_SUBMITTED (1)
#define WORKER_COMPLETED (2)
#define FD_FINISHED (-2)

namespace polar_race {
    using namespace std;

    struct KeyEntry {
        uint64_t key_;
        uint16_t value_offset_;
    }__attribute__((packed));

    struct UserIOCB {
        char *buffer_;
        int fd_;
        uint32_t size_;
        uint64_t offset_;

        UserIOCB() : buffer_(nullptr), fd_(-1), size_(0), offset_(0) {}

        UserIOCB(char *buf, int fd, uint32_t size, uint64_t offset) :
                buffer_(buf), fd_(fd), size_(size), offset_(offset) {
        }
    };

    bool operator<(KeyEntry l, KeyEntry r);

    class EngineRace : public Engine {
    public:
        int meta_cnt_file_dp_;
        uint32_t *mmap_meta_cnt_;

        int *key_file_dp_;
        int key_buffer_file_dp_;
        uint64_t *mmap_key_aligned_buffer_;
        uint64_t **mmap_key_aligned_buffer_view_;

        int *value_file_dp_;
        int value_buffer_file_dp_;
        char *mmap_value_aligned_buffer_;
        char **mmap_value_aligned_buffer_view_;

        // Write.
        mutex *bucket_mtx_;
        Barrier write_barrier_;

        // Read.
        vector<moodycamel::BlockingConcurrentQueue<int32_t> *> notify_queues_;
        char **aligned_read_buffer_;
        Barrier read_barrier_;

        vector<KeyEntry *> index_;

        // Range.
        volatile bool is_range_init_;
        Barrier *range_barrier_ptr_;
        vector<PolarString *> polar_keys_;

        mutex range_mtx_;
        condition_variable range_init_cond_;
        vector<char *> value_shared_buffers_;

        vector<promise<char *>> promises_;
        vector<shared_future<char *>> futures_;
        double total_time_;
        double total_blocking_queue_time_;
        double total_io_sleep_time_;

        double wait_get_time_;
        uint64_t val_buffer_max_size_;
        thread *single_range_io_worker_;

        // Range Sequential IO.
        blocking_queue<char *> *free_buffers_;
        vector<char *> cached_front_buffers_;
        atomic_int *bucket_consumed_num_;
        int32_t total_range_num_threads_;

        // Detail.
        vector<moodycamel::BlockingConcurrentQueue<UserIOCB> *> range_worker_task_tls_;
        atomic_int *range_worker_status_tls_;
        vector<thread> io_threads_;

    public:
        static RetCode Open(const std::string &name, Engine **eptr);

        explicit EngineRace(const std::string &dir);

        ~EngineRace() override;

        RetCode Write(const PolarString &key,
                      const PolarString &value) override;

        RetCode Read(const PolarString &key,
                     std::string *value) override;

        /*
         * NOTICE: Implement 'Range' in quarter-final,
         *         you can skip it in preliminary.
         */
        RetCode Range(const PolarString &lower,
                      const PolarString &upper,
                      Visitor &visitor) override;

    private:
        void NotifyRandomReader(uint32_t local_block_offset, int64_t tid);

    private:
        void InitRangeReader();

        void InitPollingContext();

        void InitForRange(int64_t tid);

        void ReadBucketToBuffer(uint32_t bucket_id, char *value_buffer);

    private:
        void ParallelFlushTmp(int *key_fds, int *val_fds);

        void FlushTmpFiles(string dir);

        void BuildIndex();
    };

}  // namespace polar_race

#endif  // ENGINE_RACE_ENGINE_RACE_H_
