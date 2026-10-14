cmake_minimum_required(VERSION 3.20)
project(Pulse LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Options
option(BUILD_TESTS "Build tests" ON)
option(BUILD_BENCHMARKS "Build benchmarks" ON)
set(PULSE_SANITIZE "" CACHE STRING "Enable a sanitizer: address, thread, or undefined")

if(PULSE_SANITIZE)
    message(STATUS "Pulse: enabling ${PULSE_SANITIZE} sanitizer")
    if(PULSE_SANITIZE STREQUAL "thread")
        add_compile_options(-fsanitize=thread -g -O1)
        add_link_options(-fsanitize=thread)
    elseif(PULSE_SANITIZE STREQUAL "address")
        add_compile_options(-fsanitize=address,undefined -fno-omit-frame-pointer -g -O1)
        add_link_options(-fsanitize=address,undefined)
    elseif(PULSE_SANITIZE STREQUAL "undefined")
        add_compile_options(-fsanitize=undefined -fno-omit-frame-pointer -g -O1)
        add_link_options(-fsanitize=undefined)
    else()
        message(FATAL_ERROR "Unknown PULSE_SANITIZE='${PULSE_SANITIZE}'")
    endif()
endif()

# Find packages (will be available in typical environments)
find_package(Threads REQUIRED)

add_subdirectory(src)

if(BUILD_TESTS)
    enable_testing()
    add_subdirectory(tests)
endif()

if(BUILD_BENCHMARKS)
    add_subdirectory(benchmarks)
endif()
