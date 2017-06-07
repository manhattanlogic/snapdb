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
#include "util.hpp"

FILE * file;


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


int THREADS = 32;

static const char* kTypeNames[] = 
  { "Null", "False", "True", "Object", "Array", "String", "Number" };

std::unordered_set<unsigned long> ids;
std::unordered_set<unsigned long> history_filter;
std::unordered_map<unsigned long, single_json_history *> json_history;
std::unordered_set<unsigned long> valid_users;


rapidjson::Document parse_json(char * line) {
  rapidjson::Document d;
  char * tab_p = strchr(line, '\t');
  if (tab_p == NULL) return d;
  char * tab = strchr((tab_p + 1), '\t');
  if (tab == NULL) return d;
  std::string json = (tab+1);
  json = replace_all(json, "\\'","'");
  json = replace_all(json, "\\\\","\\");

  *tab = 0;
  
  d.Parse(json.c_str());
  if (d.HasParseError()) {
    std::cerr << "json error\n" << json << "\n";
    return d;
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

  rapidjson::Value pixel_value(rapidjson::kStringType);
  pixel_value.SetString(tab_p, strlen(tab_p), allocator);
  
  d.AddMember("__pixels__", pixel_value, allocator);
  
  return d;
  
}


char * line_buffer = NULL;
extern
rapidjson::Document load_json_at_position(unsigned long position) {
  if (line_buffer == NULL) {
    line_buffer = (char *)malloc(1024 * 1024 * 4);
  }
  fseek(file, position, SEEK_SET);
  fgets(line_buffer, 1024 * 1024 * 4, file);
  auto parsed_json = parse_json(line_buffer);
  return parsed_json;
}


json_history_entry parse_data(char * line, bool preprocess) {
  json_history_entry result = {};
  
  rapidjson::Document d;
  rapidjson::Document p_d;
  
  char * tab_p = strchr(line, '\t');
  if (tab_p == NULL) return result;
  
  char * tab = strchr((tab_p + 1), '\t');
  if (tab == NULL) return result;

  *tab = 0;
  
  result.pixels = tab_p + 1;
  std::string pixel_json = tab_p + 1;
  pixel_json = replace_all(pixel_json, "'", "");
  p_d.Parse(pixel_json.c_str());
  if (d.HasParseError()) {
    std::cerr << "PIXEL PARSE ERROR !!!\n";
  }
  
  if ((p_d.Size() < 1) || (p_d[p_d.Size()-1].GetInt() != 1310)) {
    return result;
  }

  
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
    struct tm tm = {};
    if (d["events"][d["events"].Size()-1]["pix"].IsInt()) {
      int pix = d["events"][d["events"].Size()-1]["pix"].GetInt();
      if (pix == 1310) {
	if (d["events"][d["events"].Size()-1]["ts"].IsString()) {
	  auto str_ts = d["events"][d["events"].Size()-1]["ts"].GetString();
	  strptime(str_ts, "%Y-%m-%dT%H:%M:%S", &tm);
	  result.ts = mktime(&tm);
	  if (result.ts == 1483488000) {
	    std::cerr << str_ts << "\n";
	  }
	  result.ts *= 1000;
	  //std::cerr << str_ts << ":" << result.ts << "\n";
	  int ms;
	  char * dot = strchr((char *)str_ts, '.');
	  ms = strtoul(dot+1, NULL, 0);
	  if (ms > 1000) {
	    std::cerr << "ms too big:" << ms << "\n";
	  }
	  result.ts += ms;
	}
      }
    }
  } catch (...) {
  }
  /*
  if (preprocess) {
    return result;
  }
  if (valid_users.find(result.vid) == valid_users.end()) {
    return result;
    }*/
  result.events = new std::vector<json_simgle_event_type>;
  
  bool is_active_event = false;
  for (int i = 0; i < d["events"].Size(); i++) {
    if (d["events"][i]["subids"].HasMember("ensighten") && d["events"][i]["subids"]["ensighten"].IsObject()) {
      result.active_event = i;
      is_active_event = true;
    } else {
      is_active_event = false;
    }
    json_simgle_event_type event;

    /*
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
    */

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

      try {
	if (d["events"][i]["subids"]["ensighten"].HasMember("searchTerm") && d["events"][i]["subids"]["ensighten"]["searchTerm"].IsString()) {
	  event.ensighten.searchTerm = d["events"][i]["subids"]["ensighten"]["searchTerm"].GetString();
	}
      } catch (...) {}

      


      
      try {
	if (d["events"][i]["subids"]["ensighten"].HasMember("crumbs") && d["events"][i]["subids"]["ensighten"]["crumbs"].IsArray()) {
	  for (int j = 0; j < d["events"][i]["subids"]["ensighten"]["crumbs"].Size(); j++) {
	    if (d["events"][i]["subids"]["ensighten"]["crumbs"][j].IsString()) {
	      event.ensighten.crumbs.push_back(d["events"][i]["subids"]["ensighten"]["crumbs"][j].GetString());
	    }
	  }
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
	    if (d["events"][i]["subids"]["ensighten"]["items"][j].HasMember("price") && d["events"][i]["subids"]["ensighten"]["items"][j]["price"].IsString()) {
	      item.price = std::atof(d["events"][i]["subids"]["ensighten"]["items"][j]["price"].GetString());
	    }
	  } catch (...) {}

	  try {
	    if (d["events"][i]["subids"]["ensighten"]["items"][j].HasMember("quantity") && d["events"][i]["subids"]["ensighten"]["items"][j]["quantity"].IsInt()) {
	      item.quantity = d["events"][i]["subids"]["ensighten"]["items"][j]["quantity"].GetInt();
	    }
	  } catch (...) {}


	  
	  try {
	    if (d["events"][i]["subids"]["ensighten"]["items"][j].HasMember("subCatIds") &&
		d["events"][i]["subids"]["ensighten"]["items"][j]["subCatIds"].IsArray()) {
	      for (int k = 0; k < d["events"][i]["subids"]["ensighten"]["items"][j]["subCatIds"].Size(); k++) {
		if (d["events"][i]["subids"]["ensighten"]["items"][j]["subCatIds"][k].IsString()) {
		  item.subCatIds.push_back(d["events"][i]["subids"]["ensighten"]["items"][j]["subCatIds"][k].GetString());
		}
	      }
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
    result.events->push_back(event);
  }




  
  if (result.events->size() > 100) {
    std::cerr << "many events:" << result.events->size()  << "\n";
  }

  if ((preprocess) || (valid_users.find(result.vid) == valid_users.end())) {
    delete (result.events);
  }



  
  return result;
  
}


std::mutex web_mutex;
std::mutex line_read_mutex;
std::mutex result_processor_mutex;
volatile bool has_more_lines = true;




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

  unsigned long id = data.id;
  unsigned long vid = data.vid;
  unsigned long ts = data.ts;

  ids.insert(id);
  
  counter += 1;
  if (counter % 10000 == 0) {
    std::cerr << "    " << counter <<  "\n" << "\033[1A";
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
    ts ++;
    if (it->second->history.find(ts) != it->second->history.end()) {
      ts++;
      if (it->second->history.find(ts) != it->second->history.end()) {
	ts++;
	if (it->second->history.find(ts) != it->second->history.end()) {
	  std::cerr << "collision:" << vid << " : " << ts << "\n";
	}
      }
    }
  }
  it->second->history[ts] = data;
  it->second->row_mutex.unlock(); 
}


void thread_runner(int id, bool preprocess) {
  char * line = (char *) malloc(1024 * 1024 * 64); // 64 MB to be safe
  long file_position;
  while ((file_position = get_next_line(line)) >= 0) {
    auto result = parse_data(line, preprocess);
    if (!(preprocess)) {
      if (valid_users.find(result.vid) == valid_users.end()) continue;
    }
    result.file_position = file_position;
    process_result(result, file_position);
  }
  free(line);
}



unsigned long rand_between(unsigned long min, unsigned long max) {
  return (unsigned long)(min + rand() % (max - min));
}

#define MAX_ITERATIONS 10000

unsigned long get_random_user() {
  if (history_filter.size() == 0) {
    int counter = 0;
    for (int c = 0; c < MAX_ITERATIONS; c++) {
      auto random_it = std::next(std::begin(json_history), rand_between(0, json_history.size()));
      if (random_it->second->history.size() > 1) return random_it->first;
    }
  } else {
    for (int c = 0; c < MAX_ITERATIONS; c++) {
      auto filtered = std::next(std::begin(history_filter), rand_between(0, history_filter.size()));
      if (json_history[*filtered]->history.size() > 1) return (*filtered);
    }
  }
  return 0;
}


void start_web_server(int port) {
  using namespace httplib;
  Server svr;

  svr.get("/get_raw_user", [](const auto& req, auto& res) {
      std::lock_guard<std::mutex> guard(web_mutex);
      std::cerr << "get_raw_user celled\n";
      
      // char line[1024 * 1024 * 4];

      rapidjson::Document document;
      document.SetObject();
      rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
      rapidjson::Value array(rapidjson::kArrayType);

      unsigned long vid;
      
      auto vid_it = req.params.find("vid");
      if (vid_it == req.params.end()) {
	std::cerr << "looking for a user\n";
	vid = get_random_user();
      } else {
	vid = strtoul(vid_it->second.c_str(), NULL, 0);
      }
      
      if (vid == 0) {
	res.set_content("{user find failed}", "text/plain");
	return;
      }


      auto history_it = json_history.find(vid);
      if (history_it == json_history.end()) {
	res.set_content("{found bad user}", "text/plain");
	return;
      }

      std::cerr << "loading user:" << vid << " width " << history_it->second->history.size() << " events\n";
      for (auto i = history_it->second->history.begin(); i != history_it->second->history.end(); i++) {
	//fseek(file, i->second.file_position, SEEK_SET);
	//fgets(line, 1024 * 1024 * 4, file);
	//auto parsed_json = parse_json(line);
	auto parsed_json = load_json_at_position(i->second.file_position);
	rapidjson::Value event(rapidjson::kObjectType);
	event.CopyFrom(parsed_json, allocator);
	array.PushBack(event, allocator);	
      }
      std::cerr << "user loaded\n";

      
      rapidjson::Value vid_value(rapidjson::kStringType);
      vid_value.SetString(std::to_string(vid).c_str(), std::to_string(vid).size(), allocator);

      document.AddMember("data", array, allocator);
      document.AddMember("id", vid_value, allocator);
      
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      document.Accept(writer);
      
      res.set_content(buffer.GetString(), "text/plain");
      
    });

  svr.get("/exec", [](const auto& req, auto& res) {
      std::lock_guard<std::mutex> guard(web_mutex);
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
        std::cerr << "Cannot load symbol : " << dlsym_error << std::endl;
        dlclose(handle);
        return;
      }

      auto q = procfunc();
      //std::cerr << "q:" << q << "\n";
      std::cerr << "dlclose returned: " << dlclose(handle) << "\n";
      
      res.set_content(q, "text/plain");
      free(q);
      return;
    });

  std::cerr << "starting server on port:" << port << "\n";
  svr.listen("localhost", port);
}


int main (int argc, char**argv) {
  bool inverted = false;
  std::string filename = "";
  if(cmdOptionExists(argv, argv+argc, "-data")) {
    filename = getCmdOption(argv, argv + argc, "-data");
    std::cerr << "loading file:" << filename << "\n";
    file = fopen(filename.c_str(), "r");
  } else {
    file = fopen("1day.tsv", "r");
  }

  if(cmdOptionExists(argv, argv+argc, "-threads")) {
    THREADS = std::stoul(getCmdOption(argv, argv + argc, "-threads"), NULL, 0);
  }
  
  if(cmdOptionExists(argv, argv+argc, "-inverted")) {
    inverted = true;
  }

  std::cerr << "inverted status:" << inverted << "\n";
  
  std::thread threads[THREADS];
  
  for (int i = 0; i < THREADS; i++) {
    threads[i] = std::thread(thread_runner, i, true);
  }
  for (int i = 0; i < THREADS; i++) {
    threads[i].join();
  }

  std::cerr << "total users:" << json_history.size() << "\n";
  
  for (auto it = json_history.begin(); it != json_history.end(); it ++) {
    auto first = it->second->history.begin();
    auto last = it->second->history.rbegin();
    auto diatance = last->second.ts - first->second.ts;
    if (diatance > 100) {
      valid_users.insert(it->first);
    }
  }

  
  if (inverted) {
    std::cerr << "inverting validity map\n";
    std::unordered_set<unsigned long> invalid_users;
    for (auto i = json_history.begin(); i != json_history.end(); i++) {
      if (valid_users.find(i->first) == valid_users.end()) {
	invalid_users.insert(i->first);
      }
    }
    valid_users = invalid_users;
  }


  
  std::cerr << "valid users:" << valid_users.size() << "\n";
  json_history.clear();
  fclose(file);
  file = fopen(filename.c_str(), "r");

  has_more_lines = true;
  counter = 0;
  
  for (int i = 0; i < THREADS; i++) {
    threads[i] = std::thread(thread_runner, i, false);
  }
  
  for (int i = 0; i < THREADS; i++) {
    // std::cerr << "join:" << i << "\n";
    threads[i].join();
    // std::cerr << "\n";
  }

  valid_users.clear();
  
  
  std::cerr << json_history.size() << " users loaded\n";
  start_web_server(8080);
}

/*
define funnel steps
step 1: site visit
?
?
?
last step: purchase

define in terms of distance to order

 */
