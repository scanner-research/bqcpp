# Copyright 2016 Carnegie Mellon University
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

cmake_minimum_required(VERSION 3.2.0 FATAL_ERROR)

project(BQCpp)

option(BUILD_STATIC "" OFF)

enable_testing()

include(ExternalProject)

######################
###     Setup     ####
######################
# Verify C++11 support
include(CheckCXXCompilerFlag)
CHECK_CXX_COMPILER_FLAG("-std=c++11" COMPILER_SUPPORTS_CXX11)
if(COMPILER_SUPPORTS_CXX11)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")
else()
  message(STATUS "The compiler ${CMAKE_CXX_COMPILER} has no C++11 support.")
endif()


### Helper macros ####
MACRO(INSTALL_HEADERS_WITH_DIRECTORY HEADER_LIST)

FOREACH(HEADER ${${HEADER_LIST}})
STRING(REGEX MATCH "(.*)[/\\]" DIR ${HEADER})
INSTALL(FILES ${HEADER} DESTINATION include/${DIR})
ENDFOREACH(HEADER)

ENDMACRO(INSTALL_HEADERS_WITH_DIRECTORY)


set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake/Modules/")

set(GLOBAL_OUTPUT_PATH ${PROJECT_BINARY_DIR}/bin)
set(THIRDPARTY_SOURCE_DIR "${CMAKE_SOURCE_DIR}/thirdparty")

if(CMAKE_BUILD_TYPE MATCHES Debug)
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
endif()


###### DEPENDENCIES #######

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC")
if(NOT APPLE AND UNIX)
  set(CMAKE_SHARED_LINKER_FLAGS
    "${CMAKE_SHARED_LINKER_FLAGS} -pthread -ldl -lrt")
endif()

###### PROJECT LIBRARIES #######
find_package(GFlags REQUIRED)
find_package(Glog REQUIRED)
find_package(CURL REQUIRED)

set(GTEST_INCLUDE_DIRS
  "${THIRDPARTY_SOURCE_DIR}/build/bin/googletest/include")
set(GTEST_LIBRARIES
  "${THIRDPARTY_SOURCE_DIR}/build/bin/googletest/lib/libgtest.a")
set(GTEST_LIB_MAIN
  "${THIRDPARTY_SOURCE_DIR}/build/bin/googletest/lib/libgtest_main.a")

set(BQCPP_LIBRARIES
  "${THIRDPARTY_SOURCE_DIR}/build/bin/pistache/lib/libpistache.a"
  "${GLOG_LIBRARIES}"
  "${CURL_LIBRARIES}"
  "${BQCPP_LIBRARIES}")

include_directories(
  "."
  "${THIRDPARTY_SOURCE_DIR}/build/bin/pistache/include"
  "${GLOG_INCLUDE_DIRS}"
  "${GTEST_INCLUDE_DIRS}")

message(STATUS "${THIRDPARTY_SOURCE_DIR}/build/bin/pistache/include")
add_subdirectory(bqcpp)
add_subdirectory(tests)

set(SOURCE_FILES
  bqcpp/bqcpp.cpp)

add_library(bqcpp SHARED ${SOURCE_FILES})
target_link_libraries(bqcpp PUBLIC ${BQCPP_LIBRARIES})

set(PUBLIC_HEADER_FILES bqcpp/bqcpp.h)

install(TARGETS bqcpp
  EXPORT BQCppTarget
  ARCHIVE DESTINATION lib
  LIBRARY DESTINATION lib)

configure_file(cmake/BQCppConfig.cmake.in
  "${PROJECT_BINARY_DIR}/cmake/BQCppConfig.cmake" @ONLY)

install(FILES "${PROJECT_BINARY_DIR}/cmake/BQCppConfig.cmake"
  DESTINATION share/bqcpp)

install_headers_with_directory(PUBLIC_HEADER_FILES)

install(EXPORT BQCppTarget
  DESTINATION share/bqcpp)
