#include <string>
#include <vector>
#include <map>
#include <unordered_map>
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"

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
};




struct json_history_entry {
  rapidjson::Document * document;
  unsigned long file_position = 0;
};

struct single_json_history {
  std::unordered_map<unsigned long, json_history_entry *> history;
};


extern std::unordered_map<unsigned long, single_user_history *> history;
extern std::unordered_map<unsigned long, single_json_history * > json_history;
