cmake_minimum_required(VERSION 3.10)
project(engine)

set(CMAKE_CXX_STANDARD 11)

include_directories(engine_example)
include_directories(engine_race)
include_directories(include)

add_compile_options(-O2 -g -fno-omit-frame-pointer -W -Wextra -Wall -Wsign-compare
-Wno-unused-parameter -Woverloaded-virtual
-Wnon-virtual-dtor -Wno-missing-field-initializers)

add_library(engine_race
        engine_example/data_store.cc
        engine_example/data_store.h
        engine_example/door_plate.cc
        engine_example/door_plate.h
        engine_example/engine_example.cc
        engine_example/engine_example.h
        engine_example/util.cc
        engine_example/util.h
        )

add_library(test_engine
        engine_race/engine_race.cc
        engine_race/engine_race.h
        include/engine.h
        include/polar_string.h
        test/test.cc)

add_subdirectory(playground)