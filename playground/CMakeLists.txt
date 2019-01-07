add_executable(skip_list_study skip_list_study.cpp)

add_executable(polar_string_study polar_string_study.cpp log.cpp stat.cpp)

find_package(OpenMP)
if (OPENMP_FOUND)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${OpenMP_C_FLAGS}")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${OpenMP_CXX_FLAGS}")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} ${OpenMP_EXE_LINKER_FLAGS}")
endif ()

add_executable(test_engine_nonoff test_engine.cpp ../engine_race/engine_race.cc ../engine_race/log.cc
        )
#link_directories(${CMAKE_SOURCE_DIR}/lib)
message(${CMAKE_SOURCE_DIR})


