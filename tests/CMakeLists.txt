include(FetchContent)

FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

add_executable(
  tests
  exception_tests.cpp
  
  michael_scott_tests.cpp
  
  deque_bounded_correctness_test.cpp
  deque_bounded_concurrency_test.cpp
  deque_bounded_deadlock_test.cpp
  
  circular_buffer_correctness_test.cpp
  circular_buffer_concurrency_test.cpp
  circular_buffer_deadlock_test.cpp
  circular_buffer_timing_test.cpp

  min_heap_tests.cpp
)

target_link_libraries(
  tests
  PRIVATE
  message_queue
  GTest::gtest_main
)

target_include_directories(tests PUBLIC ${PROJECT_SOURCE_DIR})
include(GoogleTest)
gtest_discover_tests(tests)

include(coverage.cmake)