cmake_minimum_required(VERSION 3.20)
project(c_pipe C)

# ----------------------------------------------------------------------------
# Standard, defaults, build type.
# ----------------------------------------------------------------------------
set(CMAKE_C_STANDARD 17)
set(CMAKE_C_STANDARD_REQUIRED ON)
# NOTE: do NOT disable extensions — Aerospike client uses GNU `asm` keyword.

# Default to Release for a backup tool — performance matters.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
    set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
    set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS
            Debug Release RelWithDebInfo MinSizeRel)
endif()

enable_testing()

# ----------------------------------------------------------------------------
# Optional sanitizers for tests.
# ----------------------------------------------------------------------------
option(C_PIPE_TSAN "Build tests with ThreadSanitizer" OFF)
option(C_PIPE_ASAN "Build tests with AddressSanitizer + UBSan" OFF)

# ----------------------------------------------------------------------------
# External dependencies.
# ----------------------------------------------------------------------------
set(THREADS_PREFER_PTHREAD_FLAG ON)
find_package(Threads REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(ZLIB    REQUIRED)

# ----------------------------------------------------------------------------
# Project-wide warnings — applied only to OUR targets via INTERFACE library.
# Kept conservative: no -Wconversion/-Wpedantic — they fight with AS headers
# and produce noise on perfectly fine size_t arithmetic.
# ----------------------------------------------------------------------------
add_library(c_pipe_warnings INTERFACE)
target_compile_options(c_pipe_warnings INTERFACE
        -Wall
        -Wextra
        -Wshadow
        -Wstrict-prototypes
        -Wmissing-prototypes
        -Wcast-align
)

# ----------------------------------------------------------------------------
# Aerospike static archive — platform detection.
# ----------------------------------------------------------------------------
if(APPLE)
    if(CMAKE_SYSTEM_PROCESSOR MATCHES "arm64")
        set(AS_TARGET_PLATFORM "Darwin-arm64")
    else()
        set(AS_TARGET_PLATFORM "Darwin-x86_64")
    endif()
elseif(UNIX)
    set(AS_TARGET_PLATFORM "Linux-x86_64")
else()
    message(FATAL_ERROR "Unsupported platform for Aerospike client linkage")
endif()

set(AS_LIB_PATH
        ${CMAKE_SOURCE_DIR}/vendor/aerospike-client-c/target/${AS_TARGET_PLATFORM}/lib/libaerospike.a)

if(NOT EXISTS ${AS_LIB_PATH})
    message(WARNING
            "Aerospike static lib not found at ${AS_LIB_PATH}. "
            "Build it first: cd vendor/aerospike-client-c && make")
endif()

# ----------------------------------------------------------------------------
# Aerospike imported target.
# ----------------------------------------------------------------------------
add_library(aerospike STATIC IMPORTED)
set_target_properties(aerospike PROPERTIES
        IMPORTED_LOCATION ${AS_LIB_PATH}
)
target_include_directories(aerospike SYSTEM INTERFACE
        ${CMAKE_SOURCE_DIR}/vendor/aerospike-client-c/src/include
        ${CMAKE_SOURCE_DIR}/vendor/aerospike-client-c/modules/common/src/include
)

# AS client transitive deps.
if(APPLE)
    target_link_libraries(aerospike INTERFACE
            OpenSSL::SSL
            OpenSSL::Crypto
            ZLIB::ZLIB
            "-framework Security"
            "-framework CoreFoundation"
    )
else()
    target_link_libraries(aerospike INTERFACE
            OpenSSL::SSL
            OpenSSL::Crypto
            ZLIB::ZLIB
            rt dl m
    )
endif()

# ----------------------------------------------------------------------------
# Unity — test framework.
# ----------------------------------------------------------------------------
add_library(unity STATIC vendor/Unity/src/unity.c)
target_include_directories(unity SYSTEM PUBLIC vendor/Unity/src)

# ----------------------------------------------------------------------------
# channel — bounded thread-safe queue.
# ----------------------------------------------------------------------------
add_library(channel STATIC src/chan.c)
target_include_directories(channel PUBLIC include)
target_link_libraries(channel
        PUBLIC  Threads::Threads
        PRIVATE c_pipe_warnings
)

# ----------------------------------------------------------------------------
# pipe_lib — N-to-M pipeline over channels.
# ----------------------------------------------------------------------------
add_library(pipe_lib STATIC src/pipe.c)
target_include_directories(pipe_lib PUBLIC include)
target_link_libraries(pipe_lib
        PUBLIC  channel
        PRIVATE c_pipe_warnings
)

# ----------------------------------------------------------------------------
# as_pipe — Aerospike Reader/Writer adapters.
# ----------------------------------------------------------------------------
add_library(as_pipe STATIC
        src/as_reader.c
        src/as_writer.c
)
target_include_directories(as_pipe PUBLIC include)
target_link_libraries(as_pipe
        PUBLIC  pipe_lib aerospike
        PRIVATE c_pipe_warnings
)

# ----------------------------------------------------------------------------
# Main executable.
# ----------------------------------------------------------------------------
add_executable(c_pipe src/main.c)
target_link_libraries(c_pipe
        PRIVATE as_pipe c_pipe_warnings
)

# ----------------------------------------------------------------------------
# Tests.
# ----------------------------------------------------------------------------
add_executable(chan_tests tests/test_chan.c)
target_link_libraries(chan_tests PRIVATE channel unity c_pipe_warnings)

add_executable(pipe_tests tests/test_pipe.c)
target_link_libraries(pipe_tests PRIVATE pipe_lib unity c_pipe_warnings)

if(C_PIPE_TSAN)
    foreach(t chan_tests pipe_tests)
        target_compile_options(${t} PRIVATE -fsanitize=thread -g -O1)
        target_link_options(${t}    PRIVATE -fsanitize=thread)
    endforeach()
endif()

if(C_PIPE_ASAN)
    foreach(t chan_tests pipe_tests)
        target_compile_options(${t} PRIVATE -fsanitize=address,undefined -g -O1
                -fno-omit-frame-pointer)
        target_link_options(${t}    PRIVATE -fsanitize=address,undefined)
    endforeach()
endif()

add_test(NAME chan_tests COMMAND chan_tests)
set_tests_properties(chan_tests PROPERTIES TIMEOUT 10)

add_test(NAME pipe_tests COMMAND pipe_tests)
set_tests_properties(pipe_tests PROPERTIES TIMEOUT 10)