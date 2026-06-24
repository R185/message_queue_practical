cmake_minimum_required(VERSION 3.20)

project(message_queue_practical LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(MESSAGE_QUEUE_OPTIMIZATION O2 CACHE STRING "Compiler optimization level: O0, O1, O2, O3")
set_property(CACHE MESSAGE_QUEUE_OPTIMIZATION PROPERTY STRINGS O0 O1 O2 O3)
option(MESSAGE_QUEUE_MARCH_NATIVE "Use native architecture optimizations" OFF)

option(MESSAGE_QUEUE_ENABLE_TSAN "Build tests with ThreadSanitizer" OFF)

if(MSVC)
  if(MESSAGE_QUEUE_OPTIMIZATION STREQUAL "O0")
    add_compile_options(/Od)
  elseif(MESSAGE_QUEUE_OPTIMIZATION STREQUAL "O1")
    add_compile_options(/O1)
  elseif(MESSAGE_QUEUE_OPTIMIZATION STREQUAL "O2")
    add_compile_options(/O2)
  elseif(MESSAGE_QUEUE_OPTIMIZATION STREQUAL "O3")
    add_compile_options(/Ox)
  else()
    message(FATAL_ERROR "Unknown MESSAGE_QUEUE_OPTIMIZATION: ${MESSAGE_QUEUE_OPTIMIZATION}")
  endif()
else()
  add_compile_options(-${MESSAGE_QUEUE_OPTIMIZATION})
endif()

if(MESSAGE_QUEUE_MARCH_NATIVE)
  if(MSVC)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "^[Aa][Rr][Mm]")
      message(WARNING "MESSAGE_QUEUE_MARCH_NATIVE is ignored for MSVC ARM targets")
    else()
      add_compile_options(/arch:AVX2)
    endif()
  elseif(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
    add_compile_options(-march=native)
  endif()
endif()

add_library(message_queue INTERFACE)
target_include_directories(message_queue INTERFACE
  ${CMAKE_CURRENT_SOURCE_DIR}/lib
)
target_compile_features(message_queue INTERFACE cxx_std_20)

enable_testing()
if(EXISTS "${CMAKE_CURRENT_SOURCE_DIR}/tests/CMakeLists.txt")
  add_subdirectory(tests)
endif()
add_subdirectory(bin)
