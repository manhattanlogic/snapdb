#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include <mutex>

struct ensighten_item {
  std::string sku;
  std::string tag = "";
  std::string productName;
};

struct ensighten_type {
  bool exists = false;
  std::string browser; //
  std::vector<std::string> crumbs;
  std::string pageType;
  std::string pageName;
  std::vector<ensighten_item *> items;
  std::string camGroup;
  std::string camSource;
};

struct parsed_result {
  unsigned long ts;
  unsigned long vid;
  unsigned long id;
  std::string referrer = "";
  std::string location = "";
  ensighten_type ensighten;
  unsigned long file_position = 0;
};

struct single_user_history {
  std::map<unsigned long, parsed_result *> history;
  std::mutex row_mutex;
};


struct json_simgle_event_type {
  std::string location;
  std::string referrer;
  ensighten_type ensighten;
};

struct json_history_entry {
  unsigned long ts = 0;
  unsigned long vid = 0;
  unsigned long id = 0;
  std::vector<json_simgle_event_type> events;
  unsigned long file_position = 0;
  unsigned int active_event = 0;
};

struct single_json_history {
  std::map<unsigned long, json_history_entry> history;
  std::mutex row_mutex;
};


extern std::unordered_map<unsigned long, single_user_history *> history;
extern std::unordered_map<unsigned long, single_json_history *> json_history;
