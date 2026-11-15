cmake_minimum_required(VERSION 3.20)
project(gateway_failover CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)
if(NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Debug)
endif()

option(GW_SANITIZE "Build with address/undefined sanitizers" ON)

add_library(gw
  src/clordid.cpp
  src/order_book.cpp
  src/wal.cpp
  src/mock_broker.cpp
  src/reconciler.cpp
  src/connection.cpp
  src/safety.cpp
  src/gateway.cpp
  src/kill_switch.cpp
)
target_include_directories(gw PUBLIC include)
target_compile_options(gw PUBLIC -Wall -Wextra -Wpedantic)
if(GW_SANITIZE)
  target_compile_options(gw PUBLIC -fsanitize=address,undefined -fno-omit-frame-pointer)
  target_link_options(gw PUBLIC -fsanitize=address,undefined)
endif()

add_executable(gw_tests tests/main.cpp tests/test_unit.cpp tests/test_scenarios.cpp)
target_link_libraries(gw_tests PRIVATE gw)
target_compile_options(gw_tests PRIVATE -Wno-missing-field-initializers)
target_compile_definitions(gw_tests PRIVATE GW_TEST_DIR="${CMAKE_CURRENT_BINARY_DIR}")

add_executable(killswitch tools/killswitch_main.cpp)
target_link_libraries(killswitch PRIVATE gw)

enable_testing()
add_test(NAME gw_tests COMMAND gw_tests)
