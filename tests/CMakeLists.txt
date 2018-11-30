add_executable(BQCppTests tests.cpp)
target_link_libraries(BQCppTests ${GTEST_LIBRARIES} ${GTEST_LIB_MAIN} bqcpp pthread)
add_test(BQCppTests BQCppTests)
