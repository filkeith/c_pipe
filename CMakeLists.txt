cmake_minimum_required(VERSION 4.2)
project(c_pipe C)

set(CMAKE_C_STANDARD 17)

# enable Ctest
enable_testing()

# Properly link as lib.
if(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
        set(AS_TARGET_PLATFORM "Darwin-arm64")
    else()
        set(AS_TARGET_PLATFORM "Darwin-x86_64")
    endif()
else()
    set(AS_TARGET_PLATFORM "Linux-x86_64")
endif()

# Aerospike client library.
add_library(aerospike STATIC IMPORTED)
set_target_properties(aerospike PROPERTIES
        IMPORTED_LOCATION ${CMAKE_SOURCE_DIR}/vendor/aerospike-client-c/target/${AS_TARGET_PLATFORM}/lib/libaerospike.a
        INTERFACE_INCLUDE_DIRECTORIES "${CMAKE_SOURCE_DIR}/vendor/aerospike-client-c/src/include;${CMAKE_SOURCE_DIR}/vendor/aerospike-client-c/modules/common/src/include"
)

# Lib and header files.
add_library(channel src/chan.c)
target_include_directories(channel PUBLIC include)

# Unity for tests.
add_library(unity vendor/Unity/src/unity.c)
target_include_directories(unity PUBLIC vendor/Unity/src)

# Main app.
add_executable(c_pipe src/main.c src/pipe.c
        src/as_reader.c
        include/c_pipe/as_reader.h)
target_link_libraries(c_pipe PRIVATE channel aerospike pthread)

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