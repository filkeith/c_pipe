cmake_minimum_required(VERSION 4.2)
project(c_pipe C)

set(CMAKE_C_STANDARD 17)

# enable Ctest
enable_testing()

# Lib and header files.
add_library(channel src/chan.c)
target_include_directories(channel PUBLIC include)

# Unity for tests.
add_library(unity vendor/Unity/src/unity.c)
target_include_directories(unity PUBLIC vendor/Unity/src)

# Main app.
add_executable(c_pipe src/main.c src/pipe.c)
target_link_libraries(c_pipe PRIVATE channel)

# Tests.
add_executable(chan_tests tests/test_chan.c)
target_link_libraries(chan_tests PRIVATE channel unity pthread)
add_executable(pipe_tests tests/test_pipe.c src/pipe.c)
target_link_libraries(pipe_tests PRIVATE channel unity pthread)

# Timeouts for tests not to hang.
add_test(NAME chan_tests COMMAND chan_tests)
set_tests_properties(chan_tests PROPERTIES TIMEOUT 10)
add_test(NAME pipe_tests COMMAND pipe_tests)
set_tests_properties(pipe_tests PROPERTIES TIMEOUT 10)