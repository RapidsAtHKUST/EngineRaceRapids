#pragma once

#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <cstring>

#include <thread>
#include <chrono>

#include "log.h"

#define PRINT_LOG

using namespace std;
using namespace std::chrono;

inline void setThreadSelfAffinity(int core_id) {
//    long num_cores = sysconf(_SC_NPROCESSORS_ONLN);
//    assert(core_id >= 0 && core_id < num_cores);
//    if (core_id == 0) {
//        printf("affinity relevant logical cores: %ld\n", num_cores);
//    }
//    core_id = core_id % num_cores;
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(core_id, &cpuset);

    pthread_t current_thread = pthread_self();
    int ret = pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);
    if (ret != 0) {
        log_error("Set affinity error: %d, %s", ret, strerror(errno));
    }
}

inline std::string exec(const char *cmd) {
    std::array<char, 128> buffer;
    std::string result;
    std::shared_ptr<FILE> pipe(popen(cmd, "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() failed!");
    while (!feof(pipe.get())) {
        if (fgets(buffer.data(), 128, pipe.get()) != nullptr)
            result += buffer.data();
    }
    return result;
}

inline std::string dstat() {
    std::array<char, 512> buffer;
    std::string result;
    std::shared_ptr<FILE> pipe(popen("dstat -tcdrlmgy --fs 1 105", "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() failed!");
    int times = 0;
    int global_times = 0;
    while (!feof(pipe.get())) {
        if (fgets(buffer.data(), 512, pipe.get()) != nullptr) {
            if (global_times < 100) {
                result += buffer.data();
                times++;
                global_times++;
                if (times >= 5) {
                    log_debug(
                            "\n----system---- --total-cpu-usage-- -dsk/total- --io/total- ---load-avg---"
                            " ------memory-usage----- ---paging-- ---system-- --filesystem-"
                            "\n     time     |usr sys idl wai hiq siq| read  writ| read  writ| 1m   5m  15m | "
                            "used  buff  cach  free|  in   out | int   csw |files  inodes"
                            "\n%s", result.c_str());
                    times = 0;
                    result.clear();
                }
            } else {
                log_debug(
                        "\n----system---- --total-cpu-usage-- -dsk/total- --io/total- ---load-avg---"
                        " ------memory-usage----- ---paging-- ---system-- --filesystem-"
                        "\n     time     |usr sys idl wai hiq siq| read  writ| read  writ| 1m   5m  15m | "
                        "used  buff  cach  free|  in   out | int   csw |files  inodes"
                        "\n%s", buffer.data());
            }
        }
    }
    return result;
}

inline void DstatThreading() {
    thread my_coroutine = thread([]() {
        dstat();
    });
    my_coroutine.detach();
}

#define DEVICE_STR ("Device")
#define SPACE_DEVICE_STR (" Device")
#define LINUX_STR ("Linux")
#define SPACE_LINUX_STR (" Linux")

inline std::string iostat() {
    std::array<char, 512> buffer;
    std::string result;
    std::shared_ptr<FILE> pipe(popen("iostat -d -x -m /dev/nvme0n1 /dev/sda 1 105", "r"), pclose);
    if (!pipe) throw std::runtime_error("popen() failed!");
    while (!feof(pipe.get())) {
        if (fgets(buffer.data(), 512, pipe.get()) != nullptr) {
            if (memcmp(LINUX_STR, buffer.data(), strlen(LINUX_STR)) == 0 ||
                memcmp(SPACE_LINUX_STR, buffer.data(), strlen(SPACE_LINUX_STR)) == 0) {
                continue;
            }
            if (memcmp(DEVICE_STR, buffer.data(), strlen(DEVICE_STR)) == 0 ||
                memcmp(SPACE_DEVICE_STR, buffer.data(), strlen(SPACE_DEVICE_STR)) == 0) {
                log_debug("\n%s", result.c_str());
                result.clear();
            }
            result += buffer.data();
            if (result.back() != '\n') {
                result += '\n';
            }
        }
    }
    return result;
}

inline void IOStatThreading() {
    thread my_coroutine = thread([]() {
        iostat();
    });
    my_coroutine.detach();
}

inline int parseLine(char *line) {
    // This assumes that a digit will be found and the line ends in " Kb".
    int i = strlen(line);
    const char *p = line;
    while (*p < '0' || *p > '9') p++;
    line[i - 3] = '\0';
    i = atoi(p);
    return i;
}

inline int getValue() { //Note: this value is in KB!
    FILE *file = fopen("/proc/self/status", "r");
    int result = -1;
    char line[128];

    while (fgets(line, 128, file) != NULL) {
        if (strncmp(line, "VmRSS:", 6) == 0) {
            result = parseLine(line);
            break;
        }
    }
    fclose(file);
    return result;
}

inline void PrintMemFree() {
    log_info("Consumption: %d KB, Free (MB): \n%s", getValue(), exec("free -m").c_str());
}


inline void printTS(const char *func, int line, time_point<high_resolution_clock> &clock_beg) {
#ifdef PRINT_LOG
    /* Get current time */
    time_t t = time(nullptr);
    struct tm *lt = localtime(&t);
    char buf[16];
    buf[strftime(buf, sizeof(buf), "%H:%M:%S", lt)] = '\0';
    time_point<high_resolution_clock> clock_now = high_resolution_clock::now();
    fprintf(stderr, "%s (Func: %s, Line: %d), (TS: %.6lf s)-(Elapsed: %.6lf s), (Mem: %.6lf MB) \n", buf, func,
            line, duration_cast<nanoseconds>(clock_now.time_since_epoch()).count() / 1000000000.0,
            duration_cast<nanoseconds>(clock_now - clock_beg).count() / 1000000000.0,
            static_cast<double>(getValue()) / 1024.);
    fprintf(stderr, "\n");
#endif
}
