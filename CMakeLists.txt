cmake_minimum_required(VERSION 3.16)

project(DataStore)

# GoogleTest requires at least C++11
set(CMAKE_CXX_STANDARD 11)

set(CMAKE_INSTALL_PREFIX gtest)

include(FetchContent)
FetchContent_Declare(
  googletest
  URL https://github.com/google/googletest/archive/e2239ee6043f73722e7aa812a459f54a28552929.zip
)
# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

enable_testing()

include_directories(include)
add_executable(TestDataStore tests/TestDataStore.cpp)
target_link_libraries(TestDataStore
    sqlite3
    gtest_main
    )

include(GoogleTest)
gtest_discover_tests(TestDataStore)

install(TARGETS TestDataStore
        DESTINATION ${CMAKE_CURRENT_BINARY_DIR}/../bin)