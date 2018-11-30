#include <cassert>
#include <curl/curl.h>
#include <gtest/gtest.h>
#include <iostream>
#include <glog/logging.h>

#include "bqcpp/bqcpp.h"

class BQCppTests : public ::testing::Test {
protected:
  void SetUp() override { curl_global_init(CURL_GLOBAL_DEFAULT); }

  void TearDown() { curl_global_cleanup(); }
};

#include <stdio.h>
TEST_F(BQCppTests, Foobar) {
  curl_global_init(CURL_GLOBAL_DEFAULT);
  std::vector<std::vector<std::string>> results;
  std::string error;
  bool worked = bqcpp::run_query(
      "visualdb-1046",
      "select id, path from tvnews.query_video limit 5", results, error,
      true);
  LOG(INFO) << worked << " " << error;
}
