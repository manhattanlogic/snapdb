#include <string>
#include <vector>
#include <map>
#include <unordered_map>

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

extern std::unordered_map<unsigned long, single_user_history *> history;

