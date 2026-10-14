cmake_minimum_required(VERSION 3.20)

find_package(GTest REQUIRED)

add_executable(pulse_tests
    test_basic.cpp
)

target_link_libraries(pulse_tests PRIVATE
    pulse
    GTest::gtest
    GTest::gtest_main
)

include(GoogleTest)
gtest_discover_tests(pulse_tests)
