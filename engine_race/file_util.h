#pragma once

#include <cstring>
#include <cstdlib>
#include <cstdint>

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

inline bool file_exists(const char *file_name) {
    struct stat buffer;
    return (stat(file_name, &buffer) == 0);
}

inline size_t file_size(const char *file_name) {
    struct stat st;
    stat(file_name, &st);
    size_t size = st.st_size;
    return size;
}
