cmake_minimum_required(VERSION 3.20)

add_library(pulse
    dag.cpp
    runtime.cpp
    workload.cpp
    policy_fifo.cpp
    policy_work_stealing.cpp
    policy_critical_path.cpp
    policy_locality_aware.cpp
)

target_include_directories(pulse PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/../include
)

target_link_libraries(pulse PUBLIC Threads::Threads)

target_compile_features(pulse PUBLIC cxx_std_20)
