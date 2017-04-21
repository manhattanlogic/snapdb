// #include "json/src/json.hpp"
#include <dlfcn.h>

#include <algorithm>
#include <iostream>
#include <fstream>
#include <string>
#include <boost/algorithm/string.hpp>
#include <stdio.h>
#include <string.h>
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <vector>
#include <unordered_map>
#include <unordered_set>
#include <map>
#include <ctime>
#include <boost/regex.hpp>
#include <locale>
#include <string>
#include <cctype>

#include <stdlib.h>

#include "cpp-httplib/httplib.h"

#include "snapdb.hpp"

char* getCmdOption(char ** begin, char ** end, const std::string & option)
{
    char ** itr = std::find(begin, end, option);
    if (itr != end && ++itr != end)
    {
        return *itr;
    }
    return 0;
}

bool cmdOptionExists(char** begin, char** end, const std::string& option)
{
    return std::find(begin, end, option) != end;
}



std::string replace_all(
			const std::string & str ,   // where to work
			const std::string & find ,  // substitute 'find'
			const std::string & replace //      by 'replace'
			) {
  using namespace std;
  string result;
  size_t find_len = find.size();
  size_t pos,from=0;
  while ( string::npos != ( pos=str.find(find,from) ) ) {
    result.append( str, from, pos-from );
    result.append( replace );
    from = pos + find_len;
  }
  result.append( str, from , string::npos );
  return result;
}


// const std::string _top_categories[] = 
std::vector<std::string> top_categories = {"BABY","BEDDING & BATH","BOOKS & MEDIA","CLOTHING & SHOES",
					   "CRAFTS & SEWING", "ELECTRONICS", "EMERGENCY PREPAREDNESS",
					   "FARMERS MARKET", "FOOD & GIFTS", "HEALTH & BEAUTY",
					   "FOR THE HOME", "FURNITURE", "JEWELRY & WATCHES",
					   "LUGGAGE & BAGS", "MAIN STREET REVOLUTION", "OFFICE SUPPLIES",
					   "PET SUPPLIES", "SPORTS & TOYS", "WORLDSTOCK FAIR TRADE"};

std::unordered_map<std::string, std::string> top_categories_index;

void index_top_categories() {
  for (int i = 0; i < top_categories.size(); i++) {
    std::string replaced = replace_all(top_categories[i], "&", "&AMP;");
    top_categories_index[top_categories[i]] = top_categories[i];
    if (top_categories[i] != replaced) {
      top_categories_index[replaced] = top_categories[i];
    }
  }
}

std::string get_true_top_category(std::string crumb) {
  if (top_categories_index.size() == 0) {
    index_top_categories();
    std::cerr << "created index: " << top_categories_index.size() << "\n";
  }
  auto upped = crumb;
  std::transform(upped.begin(), upped.end(), upped.begin(), ::toupper);
  auto it = top_categories_index.find(upped);
  if (it == top_categories_index.end()) {
    return "";
  } else {
    return it->second;
  }
}

#define THREADS 16

static const char* kTypeNames[] = 
  { "Null", "False", "True", "Object", "Array", "String", "Number" };

// using json = nlohmann::json;

std::string gat_domain(std::string url) {
  char domain[1024] = {};
  sscanf(url.c_str(), "https://%[^/^:]", domain);
  if (domain[0] == 0) {
    sscanf(url.c_str(), "http://%[^/^:]", domain);
  }
  if (domain[0] == 0) {
    sscanf(url.c_str(), "android-app://%[^/^:]", domain);
  }
  
  return domain;
}



std::unordered_map<unsigned long, single_user_history *> history;
std::unordered_map<unsigned long, single_user_history *> histories[THREADS];

std::unordered_map<unsigned long, std::map<unsigned long, json_history_entry> > json_history;


rapidjson::Document * parse_json(char * line) {
  rapidjson::Document * d = new rapidjson::Document();
  
  char * tab = strchr(line, '\t');
  if (tab == NULL) return d;
  tab = strchr((tab + 1), '\t');
  if (tab == NULL) return d;
  std::string json = (tab+1);
  json = replace_all(json, "\\'","'");
  json = replace_all(json, "\\\\","\\");

  d->Parse(json.c_str());
  if (d->HasParseError()) {
    std::cerr << "json error\n" << json << "\n";
    return d;
  }

  
  
  for (int i = 0; i < (*d)["events"].Size(); i++) {
    if ((*d)["events"][i]["subids"].HasMember("ensighten") &&
	(*d)["events"][i]["subids"]["ensighten"].IsString()) {
      rapidjson::Document d2;
      std::string ensighten_json = (*d)["events"][i]["subids"]["ensighten"].GetString();
      d2.Parse(ensighten_json.c_str());
      (*d)["events"][i]["subids"]["ensighten"].CopyFrom(d2, (*d).GetAllocator());
    }
  }
  return d;
}


std::mutex line_read_mutex;
std::mutex result_processor_mutex;
bool has_more_lines = true;

// 20821717 users loaded for 7 days
// 275256   275256  5501534 conv_users.csv
// 1436394  1436394 28696915 long_users.csv
//FILE * file = fopen("../data/7days.tsv", "r");
FILE * file;

long get_next_line(char * line) {
  std::lock_guard<std::mutex> guard(line_read_mutex);
  if (!(has_more_lines)) {
    return -1;
  }
  auto file_position = ftell(file);
  if (fgets(line, 1024 * 1024 - 1, file) == NULL) {
    has_more_lines = false;
    return -1;
  }
  return file_position;
}


int counter;

int current_action = 0; // compute_converters


void process_result(rapidjson::Document * data, unsigned long file_position) {
  return;
  std::lock_guard<std::mutex> guard(result_processor_mutex);
  counter += 1;
  if (counter % 10000 == 0) {
    std::cerr << counter <<  "\n";
  }
  
  unsigned long vid = 0;
  unsigned long ts = 0;
  
  try {
    vid = (*data)["vid"].GetUint64();
  } catch (...) {
  }
  
  try {
    struct tm tm;
    auto str_ts = (*data)["events"][0]["ts"].GetString();
    strptime(str_ts, "%Y-%d-%mT%H:%M:%S", &tm);
    ts = mktime(&tm);
  } catch (...) {
  }
  
  auto it = json_history.find(vid);
  if (it == json_history.end()) {
    std::map<unsigned long, json_history_entry> suh;
    json_history[vid] = suh;
    it = json_history.find(vid);
  }
  it->second[ts].document = data;
}


void thread_runner(int id) {
  char * line = (char *) malloc(1024 * 1024 * 64); // 64 MB to be safe
  long file_position;
  while ((file_position = get_next_line(line)) >= 0) {
    auto result = parse_json(line);
    process_result(result, file_position);
  }
}

/*
1. load JSON into unordered_map
2. start server...
 */

void start_web_server(int port) {
  using namespace httplib;
  Server svr;


  svr.get("/exec", [](const auto& req, auto& res) {
      auto mod_it = req.params.find("mod");
      auto func_it = req.params.find("func");

      if (mod_it == req.params.end()) {
        std::cerr << "exec no mod param" << std::endl;
        return;
      }
      
      if (func_it == req.params.end()) {
        std::cerr << "exec no func param" << std::endl;
        return;
      }

      std::cerr << "exec " << mod_it->second << " " << func_it->second << std::endl;

      void * handle = dlopen(mod_it->second.c_str(), RTLD_LAZY | RTLD_GLOBAL);
      if (!handle) {
        std::cerr << "dlopen filed: " << dlerror() << std::endl;
        return;
      }

      dlerror();
      
      typedef char * (*data_proc_t)();

      data_proc_t procfunc = (data_proc_t)dlsym(handle, func_it->second.c_str());
      
      const char *dlsym_error = dlerror();
      if (dlsym_error) {
        std::cerr << "Cannot load symbol 'hello': " << dlsym_error << std::endl;
        dlclose(handle);
        return;
      }

      auto q = procfunc();
      //std::cerr << "q:" << q << "\n";
      dlclose(handle);
      
      res.set_content(q, "text/plain");
      free(q);
      return;
    });

  std::cerr << "starting server on port:" << port << "\n";
  svr.listen("localhost", port);
}


int main (int argc, char**argv) {
  if(cmdOptionExists(argv, argv+argc, "-data")) {
    char * filename = getCmdOption(argv, argv + argc, "-data");
    std::cerr << "loading file:" << filename << "\n";
    file = fopen(filename, "r");
  } else {
    file = fopen("1day.tsv", "r");
  }

  std::thread threads[THREADS];
  
  for (int i = 0; i < THREADS; i++) {
    threads[i] = std::thread(thread_runner, i);
  }
  for (int i = 0; i < THREADS; i++) {
    std::cerr << "join:" << i << "\n";
    threads[i].join();
    std::cerr << "\n";
  }
  // start_web_server(8080);
}
