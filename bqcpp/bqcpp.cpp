#include <curl/curl.h>
#include <glog/logging.h>
#include <iostream>

#include <array>
#include <cstdio>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

#include "json.hpp"

namespace bqcpp {

using nlohmann::json;

// https://stackoverflow.com/questions/478898/how-to-execute-a-command-and-get-output-of-command-within-c-using-posix
std::string exec(const char *cmd) {
  std::array<char, 256> buffer;
  std::string result;
  std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd, "r"), pclose);
  if (!pipe) {
    throw std::runtime_error("popen() failed!");
  }
  while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
    result += buffer.data();
  }
  return result;
}

static size_t curl_response_callback(void *contents, size_t size, size_t nmemb,
                                     void *buf) {
  std::string *str = static_cast<std::string *>(buf);
  size_t realsize = size * nmemb;
  str->append((char *)contents, realsize);
  return realsize;
}

bool run_query(std::string project, std::string query, std::vector<std::vector<std::string>>& results, std::string& error, bool verbose) {
  // Initialize session
  CURL *curl = curl_easy_init();
  if (!curl) {
    error = "init curl failed";
    return false;
  }
  std::string url =
      "https://www.googleapis.com/bigquery/v2/projects/" + project + "/queries";
  curl_easy_setopt(curl, CURLOPT_URL, url.c_str());

  // Headers
  struct curl_slist *headers = nullptr;
  headers = curl_slist_append(headers, "Accept: application/json");
  headers = curl_slist_append(headers, "Content-Type: application/json");
  headers = curl_slist_append(headers, "charsets: utf-8");
  curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);

  // Authentication
  std::string token =
      exec("gcloud auth application-default print-access-token");
  token.pop_back();
  LOG(INFO) << "Token: " << token;
  curl_easy_setopt(curl, CURLOPT_HTTPAUTH, CURLAUTH_BEARER);
  curl_easy_setopt(curl, CURLOPT_XOAUTH2_BEARER, token.c_str());

  // Payload
  json data;
  data["kind"] = "bigquery#queryRequest";
  data["useLegacySql"] = false;
  data["query"] = query;
  std::string payload = data.dump();
  curl_easy_setopt(curl, CURLOPT_POSTFIELDS, payload.c_str());

  // Read response
  std::string response;
  curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, curl_response_callback);
  curl_easy_setopt(curl, CURLOPT_WRITEDATA, &response);

  if (verbose) {
    curl_easy_setopt(curl, CURLOPT_VERBOSE, 1);
  }

  // Execute query
  CURLcode res = curl_easy_perform(curl);
  if (res != CURLE_OK) {
    error = "curl_easy_perform failed: " + std::string(curl_easy_strerror(res));
    return false;
  }

  json response_json = json::parse(response);
  if (response_json.count("error") == 1) {
    error = response_json["error"]["message"];
    return false;
  }

  for (json& row : response_json["rows"]) {
    std::vector<std::string> res;
    for (json& v : row["f"]) {
      res.push_back(v["v"]);
    }
    results.push_back(res);
  }

  curl_slist_free_all(headers);
  curl_easy_cleanup(curl);

  return true;
}

} // namespace bqcpp
