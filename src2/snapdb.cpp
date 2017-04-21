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

#define THREADS 16

static const char* kTypeNames[] = 
  { "Null", "False", "True", "Object", "Array", "String", "Number" };


std::unordered_map<unsigned long, single_json_history *> json_history;



json_history_entry parse_data(char * line) {
  json_history_entry result = {};
  rapidjson::Document d;
  
  char * tab = strchr(line, '\t');
  if (tab == NULL) return result;
  tab = strchr((tab + 1), '\t');
  if (tab == NULL) return result;
  std::string json = (tab+1);
  json = replace_all(json, "\\'","'");
  json = replace_all(json, "\\\\","\\");

  d.Parse(json.c_str());
  if (d.HasParseError()) {
    std::cerr << "json error\n" << json << "\n";
    return result;
  }

  rapidjson::Document::AllocatorType& allocator = d.GetAllocator();
  
  for (int i = 0; i < d["events"].Size(); i++) {
    if (d["events"][i]["subids"].HasMember("ensighten") &&
	d["events"][i]["subids"]["ensighten"].IsString()) {
      rapidjson::Document d2;
      std::string ensighten_json = d["events"][i]["subids"]["ensighten"].GetString();
      d2.Parse(ensighten_json.c_str());
      d["events"][i]["subids"]["ensighten"].CopyFrom(d2, allocator);
    }
  }
  
  try {
    result.vid = d["vid"].GetUint64();
  } catch (...) {
  }
  try {
    result.id = d["id"].GetUint64();
  } catch (...) {
  }
  try {
    struct tm tm;
    if (d["events"][d["events"].Size()-1]["ts"].IsString()) {
      auto str_ts = d["events"][d["events"].Size()-1]["ts"].GetString();
      strptime(str_ts, "%Y-%d-%mT%H:%M:%S", &tm);
      result.ts = mktime(&tm) * 1000;
      int ms;
      char * dot = strchr(str_ts, '.');
      sscanf(dot+1, "%d", &ms);
      result.ts += ms;
    }
  } catch (...) {
  }

  bool is_active_event = false;
  for (int i = 0; i < d["events"].Size(); i++) {
    if (d["events"][i]["subids"].HasMember("ensighten") && d["events"][i]["subids"]["ensighten"].IsObject()) {
      result.active_event = i;
      is_active_event = true;
    } else {
      is_active_event = false;
    }
    json_simgle_event_type event;
    try {
      if (d["events"][i]["subids"].HasMember("location") && d["events"][i]["subids"]["location"].IsString()) {
	  event.location = d["events"][i]["subids"]["location"].GetString();
	}
    } catch (...) {}
    
    try {
      if (d["events"][i]["subids"].HasMember("referrer") && d["events"][i]["subids"]["referrer"].IsString()) {
	  event.referrer = d["events"][i]["subids"]["referrer"].GetString();
	}
    } catch (...) {}
    
    if (is_active_event) {
      event.ensighten.exists = true;
      try {
	if (d["events"][i]["subids"]["ensighten"].HasMember("browser") && d["events"][i]["subids"]["ensighten"]["browser"].IsString()) {
	  event.ensighten.browser = d["events"][i]["subids"]["ensighten"]["browser"].GetString();
	}
      } catch (...) {}

      try {
	if (d["events"][i]["subids"]["ensighten"].HasMember("pageType") && d["events"][i]["subids"]["ensighten"]["pageType"].IsString()) {
	  event.ensighten.pageType = d["events"][i]["subids"]["ensighten"]["pageType"].GetString();
	}
      } catch (...) {}
      
      try {
	if (d["events"][i]["subids"]["ensighten"].HasMember("pageName") && d["events"][i]["subids"]["ensighten"]["pageName"].IsString()) {
	  event.ensighten.pageName = d["events"][i]["subids"]["ensighten"]["pageName"].GetString();
	}
      } catch (...) {}
      
      try {
	if (d["events"][i]["subids"]["ensighten"].HasMember("camGroup") && d["events"][i]["subids"]["ensighten"]["camGroup"].IsString()) {
	  event.ensighten.camGroup = d["events"][i]["subids"]["ensighten"]["camGroup"].GetString();
	}
      } catch (...) {}

      try {
	if (d["events"][i]["subids"]["ensighten"].HasMember("camSource") && d["events"][i]["subids"]["ensighten"]["camSource"].IsString()) {
	  event.ensighten.camSource = d["events"][i]["subids"]["ensighten"]["camSource"].GetString();
	}
      } catch (...) {}


      if (d["events"][i]["subids"]["ensighten"].HasMember("items") && d["events"][i]["subids"]["ensighten"]["items"].IsArray()) {
	for (int j = 0; j < d["events"][i]["subids"]["ensighten"]["items"].Size(); j++) {
	  ensighten_item item;
	  try {
	    if (d["events"][i]["subids"]["ensighten"]["items"][j].HasMember("sku") && d["events"][i]["subids"]["ensighten"]["items"][j]["sku"].IsString()) {
	      item.sku = d["events"][i]["subids"]["ensighten"]["items"][j]["sku"].GetString();
	    }
	  } catch (...) {}
	  
	  try {
	    if (d["events"][i]["subids"]["ensighten"]["items"][j].HasMember("productName") &&
		d["events"][i]["subids"]["ensighten"]["items"][j]["productName"].IsString()) {
	      item.productName = d["events"][i]["subids"]["ensighten"]["items"][j]["productName"].GetString();
	    }
	  } catch (...) {}

	  try {
	    if (d["events"][i]["subids"]["ensighten"]["items"][j].HasMember("tags") &&
		d["events"][i]["subids"]["ensighten"]["items"][j]["tags"].IsArray() &&
		d["events"][i]["subids"]["ensighten"]["items"][j]["tags"].Size() > 0 &&
		d["events"][i]["subids"]["ensighten"]["items"][j]["tags"][0].IsString()) {
	      item.tag = d["events"][i]["subids"]["ensighten"]["items"][j]["tags"][0].GetString();
	    }
	  } catch (...) {}
	  event.ensighten.items.push_back(item);
	}
      }
      
    }
    result.events.push_back(event);
  }
  
  return result;
  
}



std::mutex line_read_mutex;
std::mutex result_processor_mutex;
bool has_more_lines = true;


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


void process_result(json_history_entry data, unsigned long file_position) {
  if (data.vid == 0 || data.ts == 0) return;
  result_processor_mutex.lock();

  unsigned long vid = data.vid;
  unsigned long ts = data.ts;
  
  counter += 1;
  if (counter % 10000 == 0) {
    std::cerr << counter <<  "\n";
  }
  
  auto it = json_history.find(vid);
  if (it == json_history.end()) {
    single_json_history * suh = new single_json_history;
    json_history[vid] = suh;
    it = json_history.find(vid);
  }
  result_processor_mutex.unlock();
  
  it->second->row_mutex.lock();
  if (it->second->history.find(ts) != it->second->history.end()) {
    std::cerr << "collision:" << vid << " : " << ts << "\n";
  }
  it->second->history[ts] = data;
  it->second->row_mutex.unlock(); 
}


void thread_runner(int id) {
  char * line = (char *) malloc(1024 * 1024 * 64); // 64 MB to be safe
  long file_position;
  while ((file_position = get_next_line(line)) >= 0) {
    auto result = parse_data(line);
    process_result(result, file_position);
  }
}


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

  std::cerr << json_history.size() << " users loaded\n";
  start_web_server(8080);
}
