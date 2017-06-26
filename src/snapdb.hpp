#ifndef SNAPDB_HPP
#define SNAPDB_HPP
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
#include "util.hpp"

struct ensighten_item {
  std::string sku;
  std::string tag = "";
  std::string productName;
  float price;
  int quantity;
  
  // std::vector<std::string> subCatIds;
};

struct ensighten_type {
  bool exists = false;
  std::string browser;
  //char * browser = NULL;
  std::vector<std::string> crumbs;
  std::string pageType;
  //char * pageType = NULL;
  
  // std::string pageName;
  std::vector<ensighten_item> items;
  std::string camGroup;
  std::string camSource;
  //std::string searchTerm;

  unsigned int invoice_id;
  
};

struct json_simgle_event_type {
  //std::string location;
  //std::string referrer;
  ensighten_type ensighten;
  std::string ip;
  std::string browser;
  std::string _os;
  std::string _device_type;
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


void str_to_char_ptr(char ** chr, std::string value) {
  *chr = (char *)malloc(value.size() + 1);
  memcpy(*chr, value.c_str(), value.size() + 1);
}


struct user_info_struct {
  bool is_valid = false;
  bool is_converter = false;
  bool is_clicker = false;
  std::unordered_map<std::string, float> order_skus;
  std::unordered_set<std::string> cart_skus;
  std::unordered_set<std::string> product_skus;
  
  std::unordered_map<unsigned int, std::map<std::string, float> > invoices;
  
  float order_value = 0.0;

  std::string browser;
  std::string os;
  std::string device;
};

user_info_struct get_user_info(unsigned long vid) {
  user_info_struct result;
  auto it = json_history.find(vid);
  if (it == json_history.end()) return result;
  result.is_valid = true;
  for (auto j = it->second->history.begin(); j != it->second->history.end(); j++) {
    if (j->second.events == NULL) continue;
    for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
      if (!(e->ensighten.exists)) continue;

      result.browser = e->ensighten.browser;
      if (result.os == "" && e->_os != "") result.os = e->_os;
      if (result.device == "" && e->_device_type != "") result.device = e->_device_type;
      
      auto pixels = basic_split_string(replace_all(replace_all(replace_all(j->second.pixels, "'", ""), "]", ""), "[", ""), ",");
      for (auto p=pixels.begin(); p != pixels.end(); p++) {
	if (*p == "79") result.is_clicker = true;
      }
      /*
      if ((e->ensighten.camSource == "DglBrand") || (e->ensighten.camSource == "Digital Brand") ||
	  (e->ensighten.camSource == "RevJet Acq")) {
	result.is_clicker = true;
      }
      if ((e->ensighten.camGroup == "DglBrand") || (e->ensighten.camGroup == "Digital Brand") ||
	  (e->ensighten.camGroup == "RevJet Acq")) {
	result.is_clicker = true;
      }
      */
      bool productpage = (std::string(e->ensighten.pageType) == "PRODUCT");

      unsigned int invoice = e->ensighten.invoice_id;
      if (invoice > 0) {
	if (result.invoices.find(invoice) != result.invoices.end()) continue;
	std::map<std::string, float> skus;
	result.invoices[invoice] = skus;
      }

      
      for (int itt = 0; itt < e->ensighten.items.size(); itt ++) {
	if (e->ensighten.items[itt].tag == "order") {
	  
	  float value = e->ensighten.items[itt].price * e->ensighten.items[itt].quantity;
	  result.is_converter = true;
	  auto it = result.order_skus.find(e->ensighten.items[itt].sku);
	  if (it == result.order_skus.end()) {
	    result.order_skus[e->ensighten.items[itt].sku] = value;
	  } else {
	    it->second += value;
	  }
				   
	  result.order_value += value;
	  result.invoices[invoice][e->ensighten.items[itt].sku] = value;
	}
	if (productpage || e->ensighten.items[itt].tag == "productpage") {
	  result.product_skus.insert(e->ensighten.items[itt].sku);
	}
	if (e->ensighten.items[itt].tag == "cart") {
	  result.cart_skus.insert(e->ensighten.items[itt].sku);
	}
      }
    }
  }
  return (result);
}
#endif
