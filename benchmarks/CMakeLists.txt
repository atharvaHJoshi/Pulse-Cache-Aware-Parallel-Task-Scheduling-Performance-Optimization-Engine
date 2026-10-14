cmake_minimum_required(VERSION 3.20)

# Google Benchmark is optional; fall back to the built-in driver if absent.
find_package(benchmark QUIET CONFIG)

add_executable(pulse_bench
    pulse_bench.cpp
)

target_link_libraries(pulse_bench PRIVATE pulse)

if(benchmark_FOUND)
    target_link_libraries(pulse_bench PRIVATE benchmark::benchmark)
    target_compile_definitions(pulse_bench PRIVATE PULSE_HAVE_GOOGLE_BENCHMARK=1)
endif()
