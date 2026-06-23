include(FetchContent)

FetchContent_Declare(
  googlebenchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_GTEST_TESTS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googlebenchmark)

add_executable(
  message_queue_benchmark
  benchmark.cpp
)

target_link_libraries(
  message_queue_benchmark
  PRIVATE
  message_queue
  benchmark::benchmark
  benchmark::benchmark_main
)
