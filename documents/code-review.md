# Rapids团队 - Code Review 文档

## 1. 算法设计思路

整体设计中除了内存映射文件(meta-file, key/val buffer files), 
其他文件操作都通过DirectIO。
本章节分别从 
(1) 文件设计, (2) 容错设计, (3) 并行index构建, 
(4) 写入阶段, (5) 随机读取阶段, (6) Range顺序读取阶段
来讲我们的设计思路。

### 1.1 K-V-DB文件设计

文件整体设计分为三部分: 
(1) K-V Log Files, (2) Meta Count File, (3) Key/Value Buffer Files。

#### 1.1.1 K-V Log Files

* key-value的对应:
逻辑上, key-value被写到一个的相同bucket, 对应到相同的in-bucket offset, 
通过write-ahead追加到对应的log文件。 
我们把`8-byte-key`通过big-endian转化出`uint64_t`类型的整数`key`。

对应从`key`到`bucket_id`的计算如下代码所示:

```cpp
    inline uint32_t get_par_bucket_id(uint64_t key) {
        return static_cast<uint32_t >((key >> (64 - BUCKET_DIGITS)) & 0xffffffu);
    }
```

* 逻辑上的bucket到实际中的文件, 通过下面的函数算出, 
在设计中, 
我们让相邻的value buckets被group到同一个value log file, 
来为range查询顺序读服务。

```cpp
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
```

* 最终设计中, 我们采用了32个value文件和32个key文件。
这是因为多线程写入同一文件时候可以有一定的同步作用(有写入锁的存在), 
来缓解最后剩下tail threads打不满IO的情况。 
此外，文件过多容易触发Linux操作系统的bug，从DirectIO进入BufferIO, 即使已经标志flag设置了`O_DIRECT`.

#### 1.1.2 Meta Count File

* meta count文件用来记录每个bucket中现在write-ahead进行到第几个in-bucket位置了, 
该文件通过内存映射的方式, 来通过操作对应数组`mmap_meta_cnt_`， 
记录每个bucket写入write-ahead-entry个数。

```cpp
// Meta.
meta_cnt_file_dp_ = open(meta_file_path.c_str(), O_RDWR | O_CREAT, FILE_PRIVILEGE);
ftruncate(meta_cnt_file_dp_, sizeof(uint32_t) * BUCKET_NUM);
mmap_meta_cnt_ = (uint32_t *) mmap(nullptr, sizeof(uint32_t) * (BUCKET_NUM),
                                               PROT_READ | PROT_WRITE, MAP_SHARED, meta_cnt_file_dp_, 0);
memset(mmap_meta_cnt_, 0, sizeof(uint32_t) * (BUCKET_NUM));
```

#### 1.1.3 Key/Value Buffer Files

buffer files用来在写入时候进行对每个bucket-entries的buffer
(通过内存映射文件得到aligned buffer, 来具备`kill-9`容错功能).
一个bucket对应相应的key-buffer和value-buffer; 
所有的key-buffers从一个key-buffer文件内存映射出来;
同理所有的val-buffers从一个val-buffer文件映射出来。

我们给出一个Value Buffer文件的示例, Key Buffer文件相关的设计与之类似。

```cpp
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
```

* 在设计中, 我们使用了`16KB` value buffer 和 `4KB` key buffer, 
分别整除`VALUE_SIZE`和`sizeof(uint64_t)`。
我们选择较小的buffer是为了让IO尽可能快地均衡地被打出去(不要有很少的线程最后还在打IO以致于打不满), 
value buffer不选择更小是为了防止sys-cpu过高影响性能。

### 1.2 容错(K/V Buffer Files Flush)的设计

* 大体思路: 我们通过`ParallelFlushTmp`并行flush key, value buffers; 
该函数在写入阶段的析构函数调用(如果进行到对应代码), 
否则在读取阶段构建index前会调用。

* 优化: 我们通过`ftruncate`对应文件长度为0表示所有buckets对应的需要flush的buffers已经Flush出去了, 
避免重复的Flush. 相应逻辑在`FlushTmpFiles`函数中。

### 1.3 并行Index构建的设计

* 思路: 对每个Bucket构建SortedArray作为Index。

* 回顾: 文件设计中统一bucket的key-value对应起来了,
那么在构建中key的in-bucket offset和value的in-bucket offset是一样的。

每个worker处理对应的buckets, 逻辑上的buckets可以通过之前讲的K-V Log文件设计对应过去。
在整个数组被填充好了之后可以根据下面这个comparator函数对象进行排序. 

```cpp
[](KeyEntry l, KeyEntry r) {
                            if (l.key_ == r.key_) {
                                return l.value_offset_ > r.value_offset_;
                            } else {
                                return l.key_ < r.key_;
                            }
                        }
```

详细代码如下 (1. 读取填充in-bucket-offset, 2. 排序): 

```cpp
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
```

* 这个阶段主要时间开销在于读key-logs文件(sort开销可以忽略不计), 总开销大概 `0.2 seconds`。

### 1.4 写入阶段设计

通过锁一个bucket使得key-value在bucket中一一对应，
并且使得bucket的meta-count被正确地更改; 
写入之前先写bucket对应buffer, buffer满了之后进行阻塞的`pwrite`系统调用。

大体逻辑如下代码所示: 

```cpp
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
```

### 1.5 随机读取阶段设计

随机读取基本逻辑就是查询index, 如果是key-not-found就返回; 
否则读文件。

* 查询index代码如下，其中主要用了带prefetch的二分查找:

```cpp
 uint64_t big_endian_key_uint = bswap_64(TO_UINT64(key.data()));

        KeyEntry tmp{};
        tmp.key_ = big_endian_key_uint;
        auto bucket_id = get_par_bucket_id(big_endian_key_uint);

        auto it = index_[bucket_id] + branchfree_search(index_[bucket_id], mmap_meta_cnt_[bucket_id], tmp);
```

* 剩余的key-not-found判断和读value逻辑:

```cpp
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
```

### 1.6 Range顺序读阶段(两次)设计

* 主体逻辑: 单个IO协调线程一直发任务让IO线程打IO, 其他线程消费内存,
每进行一个bucket进行一次barrier来防止visit内存占用太多资源。

* 实现细节1 (IO协调线程通知memory visit 线程 value buffer结果ready 的同步):
通过使用promise和future进行 (`promises_`, `futures_`). 
每个bucket会对应一个promise, 来表示一个未来的获取到的返回结果(也就是读取完的buffer), 
这个promise对应了一个`shared_future`, 使得所有visitors可以等待该返回结果。

* 实现细节2 (通知IO协调线程free buffers已经有了):
通过一个blocking queue `free_buffers_`来记录free buffers, 
visitor线程push buffer进入 `free_buffers_`, 
IO线程从中pop buffer。

对应的IO协调thread逻辑如下 (`ReadBucketToBuffer`来打满IO):

```cpp
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
```

具体的submit读单个bucket任务的逻辑如下:

```cpp
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
```

对应的IO线程逻辑如下:

```cpp
    void EngineRace::InitPoolingContext() {
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
                    // statistics here.
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
```

对应的内存visitor线程的逻辑如下: 
其中每个bucket开始有个barrier过程, 在每次结束的时候会更新`free_buffers_`。

更新buffers的逻辑就是最后一个线程将使用完的buffer放入blocking queue。

```cpp
 // End of inner loop, Submit IO Jobs.
            int32_t my_order = ++bucket_consumed_num_[future_id];
            if (my_order == total_range_num_threads_) {
                if ((future_id % (2 * BUCKET_NUM)) < KEEP_REUSE_BUFFER_NUM) {
                    cached_front_buffers_[future_id] = shared_buffer;
                } else {
                    free_buffers_->push(shared_buffer);
                }
            }
```

```cpp
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
                    auto wait_start_clock = high_resolutIOn_clock::now();
                    shared_buffer = futures_[future_id].get();
                    auto wait_end_clock = high_resolutIOn_clock::now();
                    double elapsed_time = duratIOn_cast<nanoseconds>(wait_end_clock - wait_start_clock).count() /
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
```

## 2. 创新点

### 2.1 随机读阶段稳定性的提高

思路: 我们设计了同步策略来保证足够的queue-depth (25-30之间)的同时，
又使得不同线程可以尽量同时退出, 尽量避免少queue-depth打IO情况的出现. 

* 实现细节: 我们引入了4个blocking queues `notify_queues_`来作为
偶数和奇数线程的 当前和下一轮读取的同步通信工具 (`tid%2==0`与`tid%2==1`线程互相通知)。

* 实现细节1 (初始化逻辑): 初始化时候放入偶数线程的blocking queue来让他们启动起来。

```cpp
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
```    
    
* 实现细节2 (等待逻辑): 每一round的开始的时候会有一个等待。

```cpp
    uint32_t current_round = local_block_offset - 1;
        if ((current_round % SHRINK_SYNC_FACTOR) == 0) {
            uint32_t notify_big_round_idx = get_notify_big_round(current_round);
            if (tid % 2 == 0) {
                notify_queues_[notify_big_round_idx % 2]->wait_dequeue(tmp_val);
            } else {
                notify_queues_[notify_big_round_idx % 2 + 2]->wait_dequeue(tmp_val);
            }
        }
```

* 实现细节3 (通知逻辑): 偶数线程通知奇数线程当前round, 奇数线程通知偶数线程下一round。

```cpp
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
    }
```

### 2.2 针对Range阶段两次全量的优化

* 优化1 (增大overlap区域): 每次处理两轮的任务, 来最小化没有overlapped的IO和内存访问的时间。
详细可见IO线程的逻辑(Odd Round, Even Round)。

* 优化2 (利用Cache一些buffers减少IO数量): cache前几块buffer来进行第二次range的优化, 减少IO数量。
详细可见内存visitor线程收尾阶段。

```cpp
           // End of inner loop, Submit IO Jobs.
            int32_t my_order = ++bucket_consumed_num_[future_id];
            if (my_order == total_range_num_threads_) {
                if ((future_id % (2 * BUCKET_NUM)) < KEEP_REUSE_BUFFER_NUM) {
                    cached_front_buffers_[future_id] = shared_buffer;
                } else {
                    free_buffers_->push(shared_buffer);
                }
            }

```

为了支持优化2，我们设计了`free_buffers_`的逻辑来精确地控制IO buffers和cache的使用。

## 3. 最终线上效果

* 历史最佳成绩: `413.69 seconds`

* 进程elapsed time

```
写入进程的历史最佳状态: 114.1 seconds左右
读取进程的历史最佳状态: 105.9 seconds左右 (包括0.2 seconds index构建)
Range进程的历史最佳状态: 192.1 seconds左右 (包括0.2 seconds index构建)
```

* 进程启动间隔

```
写入启动的间隔: 0.1 seconds 左右
写入到读取的间隔: 0.35 seconds左右
读取到Range的间隔: 0.45 seconds左右
```
