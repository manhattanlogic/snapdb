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


std::unordered_map<std::string, std::vector<float> > word_2_vec;

void load_word_2_vec() {
  char line[1024 * 1024];
  FILE * file = fopen("../data/wiki.en.vec","r");
  while (fgets(line, 1024 * 1024 - 1, file) != NULL) {
    char* token = strtok(line, " ");
    if (token != NULL) {
      std::string word = token;
      std::vector<float> vector;
      while (token) {
	token = strtok(NULL, " ");
	if (token != NULL && token[0] != '\n') {
	  float value = std::stof(token);
	  vector.push_back(value);
	}
      }
      word_2_vec[word] = vector;
    }
  }
  fclose(file);
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





#if 0
struct ensighten_item {
  std::string sku;
  std::string tag = "";
  std::string productName;
};

struct ensighten_type {
  bool exists = false;
  std::string browser;
  std::vector<std::string> crumbs;
  std::string pageType;
  std::vector<ensighten_item *> items;
};

struct parsed_result {
  unsigned long ts;
  unsigned long vid;
  std::string referrer = "";
  std::string location = "";
  ensighten_type ensighten;
  unsigned long file_position = 0;
};

struct single_user_history {
  std::map<unsigned long, parsed_result *> history;
};
#endif
std::unordered_map<unsigned long, single_user_history *> history;

std::unordered_map<unsigned long, single_user_history *> histories[THREADS];


rapidjson::Document parse_json(char * line) {
  rapidjson::Document d;
  
  char * tab = strchr(line, '\t');
  if (tab == NULL) return d;
  tab = strchr((tab + 1), '\t');
  if (tab == NULL) return d;
  std::string json = (tab+1);
  json = replace_all(json, "\\'","'");
  json = replace_all(json, "\\\\","\\");
  d.Parse(json.c_str());
  if (d.HasParseError()) {
    std::cerr << "json error\n" << json << "\n";
    return d;
  }

  for (int i = 0; i < d["events"].Size(); i++) {
    if (d["events"][i]["subids"].HasMember("ensighten") &&
	d["events"][i]["subids"]["ensighten"].IsString()) {
      rapidjson::Document d2;
      std::string ensighten_json = d["events"][i]["subids"]["ensighten"].GetString();
      d2.Parse(ensighten_json.c_str());
      d["events"][i]["subids"]["ensighten"].CopyFrom(d2, d.GetAllocator());
    }
  }
  return d;
}



parsed_result * parse_event(rapidjson::Document &_d, char * line) {
  rapidjson::Document d;

  char * tab_1 = strchr(line, '\t');
  if (tab_1 == NULL) return NULL;
  char * tab = strchr((tab_1 + 1), '\t');
  if (tab == NULL) return NULL;

  parsed_result * result = new parsed_result;

  *tab = 0;
  
  std::string json = (tab+1);

  json = replace_all(json, "\\'","'");
  json = replace_all(json, "\\\\","\\");


  // std::cerr << "\n" << json << "\n";
  
  
  d.Parse(json.c_str());


  // this logic seems to work - only pick up "ensighten" events for now events 
  /*
  std::cerr << tab_1 << " ";
  for (int i = 0; i < d["events"].Size(); i++) {
    if (d["events"][i]["subids"].HasMember("ensighten") && d["events"][i]["subids"]["ensighten"].IsString()) {
      std::cerr << " " << i;
    }
  }
  std::cerr << "\n";
  */
  
  if (d.HasParseError()) {
    std::cerr << "json error\n" << json << "\n";
  } else {
    result->vid = d["vid"].GetUint64();
    result->id = d["id"].GetUint64();
    if (d["events"][0]["subids"].HasMember("referrer")) {
      if (d["events"][0]["subids"]["referrer"].IsString()) {
	result->referrer = gat_domain(d["events"][0]["subids"]["referrer"].GetString());
      }
      //std::cerr << result->referrer << "\n";
    }

    int valid_event = 0;
    for (int i = 0; i < d["events"].Size(); i++) {
      if (d["events"][i]["subids"].HasMember("ensighten") && d["events"][i]["subids"]["ensighten"].IsString()) {
	valid_event = i;
      }
    }


    
    if (d["events"][valid_event]["subids"].HasMember("location")) {
      // result->location = d["events"][0]["subids"]["location"].GetString();
    }
    if (d["events"][valid_event].HasMember("ts")) {
      // 2017-03-31T23:57:31.033Z
      struct tm tm;
      if (d["events"][valid_event]["ts"].IsString()) {
	auto str_ts = d["events"][valid_event]["ts"].GetString();
	strptime(str_ts, "%Y-%d-%mT%H:%M:%S", &tm);
	unsigned long ts = mktime(&tm);
	result->ts = ts;
      } else {
	result->ts = 0;
      }
    }

    
    if (d["events"][valid_event]["subids"].HasMember("ensighten") &&
	d["events"][valid_event]["subids"]["ensighten"].IsString()) {
      std::string ensighten_json = d["events"][valid_event]["subids"]["ensighten"].GetString();
      d.Parse(ensighten_json.c_str());
      if (d.HasParseError()) {
	return result;
      } else {
	result->ensighten.exists = true;
	
      
	if (d.HasMember("browser")) {
	  if (d["browser"].IsString()) {
	    result->ensighten.browser = d["browser"].GetString();
	  }
	}
	if (d.HasMember("pageType")) {
	  if (d["pageType"].IsString()) {
	    result->ensighten.pageType = d["pageType"].GetString();
	  }
	}
	
	if (d.HasMember("crumbs")) {
	  for ( int i = 0; i < d["crumbs"].Size(); i++) {
	    if (d["crumbs"][i].IsString()) {
	      result->ensighten.crumbs.push_back(d["crumbs"][i].GetString());
	    }
	  }
	}
      
	if (d.HasMember("items") &&
	    d.IsObject()) {
	
	  for (int i = 0; i < d["items"].Size(); i++) {
	    
	    ensighten_item * item = new ensighten_item;
	    if (d["items"][i].HasMember("sku")) {
	      if (d["items"][i]["sku"].IsString()) {
		item->sku = d["items"][i]["sku"].GetString();
	      }
	    }
	  
	    if (d["items"][i].HasMember("tags") &&
		d["items"][i]["tags"].IsArray()) {
	      if (d["items"][i]["tags"][0].IsString()) {
		item->tag = d["items"][i]["tags"][0].GetString();
	      }
	    }

	    if (d["items"][i].HasMember("productName")) {
	      if (d["items"][i]["productName"].IsString()) {
		item->productName = d["items"][i]["productName"].GetString();
	      }
	    }
	    result->ensighten.items.push_back(item);
	  }
	}
      }
    }
  }
  return result;
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

std::unordered_set<unsigned long> converters;
std::unordered_set<unsigned long> users;


void process_result_lockless(parsed_result * data, int id) {
  auto it = histories[id].find(data->vid);
  if (it == histories[id].end()) {
    single_user_history * suh = new single_user_history;
    histories[id][data->vid] = suh;
    it = histories[id].find(data->vid);
  }
  it->second->history[data->ts] = data;
}

void process_result(parsed_result * data) {
  std::lock_guard<std::mutex> guard(result_processor_mutex);
  counter += 1;
  if (counter % 10000 == 0) {
    std::cerr << counter << "," << users.size() << "," << converters.size() << "\n";
  }


  auto it = history.find(data->vid);
  if (it == history.end()) {
    single_user_history * suh = new single_user_history;
    history[data->vid] = suh;
    it = history.find(data->vid);
  }
  it->second->history[data->ts] = data;
}


void thread_runner(int id) {
  char * line = (char *) malloc(1024 * 1024);
  rapidjson::Document d;
  long file_position;
  while ((file_position = get_next_line(line)) >= 0) {
    auto result = parse_event(d, line);
    result->file_position = file_position;
    process_result(result);
  }
}

unsigned long rand_between(unsigned long min, unsigned long max) {
  return (unsigned long)(min + rand() % (max - min));
}


/*
struct ensighten_item {
    std::string sku;
    std::string tag = "";
    std::string productName;
};

struct ensighten_type {
    bool exists = false;
    std::string browser;
    std::vector<std::string> crumbs;
    std::string pageType;
    std::vector<ensighten_item *> items;
};

struct parsed_result {
  unsigned long ts;
  unsigned long vid;
  std::string referrer = "";
  std::string location = "";
  ensighten_type ensighten;
};
*/

std::string test_string = "TEST TEST TEST";

void start_web_server() {
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

  svr.get("/get_raw_user", [](const auto& req, auto& res) {
      rapidjson::Document document;
      document.SetObject();
      
      rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
      rapidjson::Value array(rapidjson::kArrayType);
      
      char line[1024 * 1024];

      auto random_it = history.begin();
      auto vid_it = req.params.find("vid");
      
      if (vid_it == req.params.end()) {
	std::cerr << "no vid passed\n";
	if (req.params.find("_conv") != req.params.end()) {
	  std::cerr << "looking for a converter\n";
	  auto random_conv = std::next(std::begin(converters), rand_between(0, converters.size()));
	  random_it = history.find(*random_conv);
	  std::cerr << "converter:" << *random_conv << "\n";
	} else {
	  random_it = std::next(std::begin(history), rand_between(0, history.size()));
	  while (random_it->second->history.size() < 2) {
	    random_it = std::next(std::begin(history), rand_between(0, history.size()));
	  }
	}
      } else {
	unsigned long vid = strtoul(vid_it->second.c_str(), NULL, 0);
	std::cerr << "vid passed:" << vid << "\n";
	random_it = history.find(vid);
	if (random_it == history.end()) {
	  res.set_content("{}", "text/plain");
	  return;
	}
      }

      unsigned long vid_value = 0;
      
      for (auto i = random_it->second->history.begin(); i != random_it->second->history.end(); i++) {
	fseek(file, i->second->file_position, SEEK_SET);
	fgets(line, 1024 * 1024 - 1, file);

	auto parsed_json = parse_json(line);
	rapidjson::Value event(rapidjson::kObjectType);
	event.CopyFrom(parsed_json, allocator);
	array.PushBack(event, allocator);
	
	vid_value = parsed_json["vid"].GetInt64();
      }

      rapidjson::Value vid(rapidjson::kStringType);
      vid.SetString(std::to_string(vid_value).c_str(), std::to_string(vid_value).size(), allocator);
      
      document.AddMember("data", array, allocator);
      document.AddMember("id", vid, allocator);
      
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
      document.Accept(writer);
      
      
      res.set_content(buffer.GetString(), "text/plain");
    });



  svr.get("/get_user", [](const auto& req, auto& res) {

      auto random_it = history.begin();
      auto vid_it = req.params.find("vid");

      for (auto q = req.params.begin(); q != req.params.end(); q++){
	std::cerr << q->first << "," << q->second << "\n";
      }

      if (vid_it == req.params.end()) {
	std::cerr << "no vid passed\n";
	if (req.params.find("conv") != req.params.end()) {
	  std::cerr << "looking for a converter\n";
	  auto random_conv = std::next(std::begin(converters), rand_between(0, converters.size()));
	  random_it = history.find(*random_conv);
	  std::cerr << "converter:" << *random_conv << "\n";
	} else {
	  random_it = std::next(std::begin(history), rand_between(0, history.size()));
	  while (random_it->second->history.size() < 2) {
	    random_it = std::next(std::begin(history), rand_between(0, history.size()));
	  }
	}
      } else {
	unsigned long vid = strtoul(vid_it->second.c_str(), NULL, 0);
	std::cerr << "vid passed:" << vid << "\n";
	random_it = history.find(vid);
	if (random_it == history.end()) {
	  res.set_content("{}", "text/plain");
	  return;
	}
      }
      
      rapidjson::Document document;
      document.SetObject();
      
      bool vid_set = false;
      

      rapidjson::Document::AllocatorType& allocator = document.GetAllocator();
      rapidjson::Value array(rapidjson::kArrayType);

            
      for (auto i = random_it->second->history.begin(); i != random_it->second->history.end(); i++) {

	
	rapidjson::Value events(rapidjson::kArrayType);
	rapidjson::Value full_event(rapidjson::kObjectType);
	
	rapidjson::Value datum(rapidjson::kObjectType);

	rapidjson::Value vid(rapidjson::kNumberType);
	vid.SetInt64(i->second->vid);
	full_event.AddMember("vid", vid, allocator);

	if (!(vid_set)) {
	  rapidjson::Value vid(rapidjson::kStringType);
	  vid.SetString(std::to_string(i->second->vid).c_str(), std::to_string(i->second->vid).size(), allocator);
	  document.AddMember("id", vid, allocator);
	  vid_set = true;
	}

	
	rapidjson::Value referrer(rapidjson::kStringType);
	referrer.SetString(i->second->referrer.c_str(), i->second->referrer.size(), allocator);
	datum.AddMember("referrer", referrer, allocator);

	rapidjson::Value location(rapidjson::kStringType);
	location.SetString(i->second->location.c_str(), i->second->location.size(), allocator);
	datum.AddMember("location", location, allocator);

	std::tm * ptm = std::gmtime((const time_t *)&i->second->ts);
	char date_buffer[256];
	std::strftime(date_buffer, 256, "%Y-%m-%dT%H:%M:%S.000Z", ptm);
	rapidjson::Value ts(rapidjson::kStringType);
	ts.SetString(date_buffer, strlen(date_buffer), allocator);
	datum.AddMember("ts", ts, allocator);
	
	rapidjson::Value subids(rapidjson::kObjectType);
	

	rapidjson::Value ensighten(rapidjson::kObjectType);
	if (i->second->ensighten.exists) {
	  

	  if (i->second->ensighten.browser != "") {
	    rapidjson::Value browser(rapidjson::kStringType);
	    browser.SetString(i->second->ensighten.browser.c_str(), i->second->ensighten.browser.size(), allocator);
	    ensighten.AddMember("browser", browser, allocator);
	  }
	  
	  if (i->second->ensighten.pageType != "") {
	    rapidjson::Value pageType(rapidjson::kStringType);
	    pageType.SetString(i->second->ensighten.pageType.c_str(), i->second->ensighten.pageType.size(), allocator);
	    ensighten.AddMember("pageType", pageType, allocator);
	  }

	  if (i->second->ensighten.crumbs.size() > 0) {
	    rapidjson::Value crumbs(rapidjson::kArrayType);
	    for (int j = 0; j < i->second->ensighten.crumbs.size(); j++) {
	      rapidjson::Value crumb(rapidjson::kStringType);
	      crumb.SetString(i->second->ensighten.crumbs[j].c_str(), i->second->ensighten.crumbs[j].size(), allocator);
	      crumbs.PushBack(crumb, allocator);
	    }
	    ensighten.AddMember("crumbs", crumbs, allocator);
	  }


	  
	  if (i->second->ensighten.items.size() > 0) {
	    rapidjson::Value items(rapidjson::kArrayType);
	    for (int j = 0; j < i->second->ensighten.items.size(); j++) {
	      rapidjson::Value item(rapidjson::kObjectType);
	      if (i->second->ensighten.items[j]->sku != "") {
		rapidjson::Value sku(rapidjson::kStringType);
		sku.SetString(i->second->ensighten.items[j]->sku.c_str(), i->second->ensighten.items[j]->sku.size(), allocator);
		item.AddMember("sku", sku, allocator);
	      }


	      if (i->second->ensighten.items[j]->productName != "") {
		rapidjson::Value productName(rapidjson::kStringType);
		productName.SetString(i->second->ensighten.items[j]->productName.c_str(), i->second->ensighten.items[j]->productName.size(), allocator);
		item.AddMember("productName", productName, allocator);
	      }

	      
	      if (i->second->ensighten.items[j]->tag != "") {
		rapidjson::Value tags(rapidjson::kArrayType);
		rapidjson::Value tag(rapidjson::kStringType);
		tag.SetString(i->second->ensighten.items[j]->tag.c_str(), i->second->ensighten.items[j]->tag.size(), allocator);
		tags.PushBack(tag, allocator);
		item.AddMember("tags", tags, allocator);
	      }
	      
	      items.PushBack(item, allocator);
	    }
	    ensighten.AddMember("items", items, allocator);
	  }

	  
	  
	}

	subids.AddMember("ensighten", ensighten, allocator);
	
	datum.AddMember("subids", subids, allocator);
	
	events.PushBack(datum, allocator);
	full_event.AddMember("events", events, allocator);
	
	array.PushBack(full_event, allocator);
      }
      
      
      
      document.AddMember("data", array, allocator);
      
      
      
      rapidjson::StringBuffer buffer;
      rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);

      document.Accept(writer);

      std::cerr << "done\n";
      
      res.set_content(buffer.GetString(), "text/plain");
    });
  std::cerr << "starting server" << "\n";
  svr.listen("localhost", 8080);
}


void dump_skus_for_w2v(std::string filename) {
  std::ofstream sku_dump_file(filename);
  for (auto i = history.begin(); i != history.end(); i++) {
    std::stringstream ss;
    int dumped = 0;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      bool isProduct = false;
      if (j->second->ensighten.pageType == "PRODUCT") {
	isProduct = true;
      }
      for (int item = 0; item < j->second->ensighten.items.size(); item++) {
	if ((j->second->ensighten.items[item]->tag == "productpage") || isProduct) {
	  if (dumped > 0) ss << " ";
	  ss << j->second->ensighten.items[item]->sku;
	  dumped++;
	}
      }
    }
    if (dumped > 1) {
      sku_dump_file << ss.str() << "\n";
    }
  }
  
}


int main (int argc, char**argv) {
  if(cmdOptionExists(argv, argv+argc, "-data")) {
    char * filename = getCmdOption(argv, argv + argc, "-data");
    std::cerr << "loading file:" << filename << "\n";
    file = fopen(filename, "r");
  } else {
    file = fopen("1day.tsv", "r");
  }


  
  std::ofstream long_file("long_users.csv") ;
  std::ofstream conv_file("conv_users.csv");
  

  
  std::thread threads[THREADS];
  
  for (int i = 0; i < THREADS; i++) {
    threads[i] = std::thread(thread_runner, i);
  }
  for (int i = 0; i < THREADS; i++) {
    std::cerr << "join:" << i << "\n";
				 threads[i].join();
				 std::cerr << "\n";
  }
      std::cerr << history.size() << " users loaded\n";


   

    // index on users
    for (auto i = history.begin(); i != history.end(); i++) {
      //if (i->second->history.size() > 3) {
      //long_file << i->first << "\n";
      //}
      for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
	for (int item = 0; item < j->second->ensighten.items.size(); item++) {
	  if (j->second->ensighten.items[item]->tag == "order") {
	    converters.insert(i->first);
	    // conv_file << i->first << "\n";
	  }
	}
      }
    }

    //return 0;

    std::cerr << "dumping skus\n";
    dump_skus_for_w2v("w2v_source.txt");
    std::cerr << "starting web server\n";
    start_web_server();
    return(0);

    
  
    std::unordered_map<int, std::vector<int> > stats; // history len->stats (users, converters, carters)
    std::unordered_map<std::string, std::vector<int> > ref_stats; // referer domain -> stats (users, converters, carts)
    std::unordered_map<std::string, std::vector<int> > crumb_counts; 
  
    for (auto i = history.begin(); i != history.end(); i++) {
      int len = i->second->history.size();
      bool converter = false;
      bool cart = false;
      std::unordered_map<std::string, int> local_referers;
      std::unordered_map<std::string, int> local_crumbs;

    
    
      for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
	local_referers[j->second->referrer] = 1;
	for (int k = 0; k < j->second->ensighten.items.size(); k++) {
	  converter |= j->second->ensighten.items[k]->tag == "order";
	  cart |= j->second->ensighten.items[k]->tag == "cart";
	}
	for (int k = 0; k < j->second->ensighten.crumbs.size(); k++) {
	  auto crumb = j->second->ensighten.crumbs[k];
	  //auto upped_crumb = get_true_top_category(crumb);
	  auto upped_crumb = crumb;
	  if (upped_crumb != "") {
	    auto it = local_crumbs.find(upped_crumb);
	    if (it == local_crumbs.end()) {
	      local_crumbs[upped_crumb] = 0;
	      it = local_crumbs.find(upped_crumb);
	    }
	    local_crumbs[upped_crumb] ++;
	  }
	}
      }

      for (auto c = local_crumbs.begin(); c != local_crumbs.end(); c++) {
	auto it = crumb_counts.find(c->first);
	if (it == crumb_counts.end()) {
	  std::vector<int> q;
	  q.push_back(0);q.push_back(0);q.push_back(0);
	  crumb_counts[c->first] = q;
	  it = crumb_counts.find(c->first);
	}
	it->second [0] ++;
	if (cart) it->second[1] ++;
	if (converter) it->second[2] ++;
      }


    
      for (auto r = local_referers.begin(); r != local_referers.end(); r++) {
	auto rr = ref_stats.find(r->first);
	if (rr == ref_stats.end()) {
	  std::vector<int> q;
	  q.push_back(0);q.push_back(0);q.push_back(0);
	  ref_stats[r->first] = q;
	  rr = ref_stats.find(r->first);
	}
	rr->second [0] ++;
	if (cart) rr->second[1] ++;
	if (converter) rr->second[2] ++;
      }
    
    
      auto it = stats.find(len);
      if (it == stats.end()) {
	std::vector<int> q;
	q.push_back(0);q.push_back(0);q.push_back(0);
	stats[len] = q;
	it = stats.find(len);
      }
      stats[len][0] ++;
      if (cart) stats[len][1] ++;
      if (converter) stats[len][2] ++;
    }
    /*
      for (auto i = stats.begin(); i != stats.end(); i++) {
      std::cout << i->first << "\t" << i->second[0] << "\t" << i->second[1] << "\t" << i->second[2] << "\n";
      }
    */
    /*
      for (auto i = ref_stats.begin(); i != ref_stats.end(); i++) {
      std::cout << i->first << "\t" << i->second[0] << "\t" << i->second[1] << "\t" << i->second[2] << "\n";
      }
    */
    for (auto i = crumb_counts.begin(); i != crumb_counts.end(); i++) {
      std::cout << "\"" << i->first << "\"\t" << i->second[0] << "\t" << i->second[1] << "\t"
		<< i->second[2] << "\n";
    }
}
