cmake_minimum_required(VERSION 3.16)
project(qnx_pulseguard LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

add_compile_options(-O2 -Wall -Wextra -Werror)

find_package(Threads REQUIRED)

add_library(pulseguard_core STATIC
    src/resmgr_pulseguard.cpp
)

target_include_directories(pulseguard_core PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}/src
)

target_link_libraries(pulseguard_core PUBLIC Threads::Threads)

add_executable(pulseguard_daemon src/pulseguard_daemon.cpp)
target_link_libraries(pulseguard_daemon PRIVATE pulseguard_core)

add_executable(fault_injector src/fault_injector.cpp)
target_link_libraries(fault_injector PRIVATE pulseguard_core)

enable_testing()
add_executable(test_pulseguard tests/test_pulseguard.cpp)
target_link_libraries(test_pulseguard PRIVATE pulseguard_core)
add_test(NAME PulseGuardUnitAndE2E COMMAND test_pulseguard)
