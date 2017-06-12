#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include <unordered_set>
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include <mutex>

struct ensighten_item {
  std::string sku;
  std::string tag = "";
  std::string productName;
  float price;
  int quantity;
  std::vector<std::string> subCatIds;
};

struct ensighten_type {
  bool exists = false;
  std::string browser; //
  std::vector<std::string> crumbs;
  std::string pageType;
  std::string pageName;
  std::vector<ensighten_item> items;
  std::string camGroup;
  std::string camSource;
  std::string searchTerm;
  
};

struct json_simgle_event_type {
  //std::string location;
  //std::string referrer;
  ensighten_type ensighten;
  std::string ip;
  std::string browser;
  std::string device_model;
};

struct json_history_entry {
  unsigned long ts = 0;
  unsigned long vid = 0;
  unsigned long id = 0;
  std::vector<json_simgle_event_type> * events;
  unsigned long file_position = 0;
  unsigned int active_event = 0;
  std::string pixels;
};

struct single_json_history {
  std::map<unsigned long, json_history_entry> history;
  std::mutex row_mutex;
};



extern std::unordered_map<unsigned long, single_json_history *> json_history;
extern std::unordered_set<unsigned long> history_filter;
extern rapidjson::Document load_json_at_position(unsigned long position);
