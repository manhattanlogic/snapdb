#include <iostream>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <memory.h>
#include <fstream>
#include <algorithm>
#include <string>

extern std::string test_string;

std::vector<std::unordered_map<std::string, long> > crumb_stats;
std::unordered_set<std::string> crumbs;

std::unordered_map <std::string, unsigned long> flat_crumb_stats;

struct hash_struct {
  long users;
  long converters;
  std::unordered_map<std::string, int> hash;
};

std::unordered_map<std::string, hash_struct> global_crumb_stats;


extern "C"
char * query() {
  std::ofstream file("flat_crumb_stats.csv");
  
  std::unordered_map<int, int> vector_stats;
  std::stringstream result;
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    bool is_completed = false;
    
    std::unordered_set<std::string> cart;
    bool cart_first = true;
    std::vector<std::string> hash_string;

    std::unordered_set<std::string> user_crumbs;
    
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      bool is_product = false;
      if (j->second.events == NULL) continue;
      
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;

	std::string event_type = "X";

	std::unordered_set <std::string> new_cart;
	for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  if (ii -> tag == "cart") {
	    new_cart.insert(ii->sku);
	  }
	}
	if (e -> ensighten.searchTerm != "") {
	  event_type = "search";
	} else if (e->ensighten.items.size() > 0) {
	  for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	    if (ii -> tag == "cart") {
	      if (new_cart.size() != 0) {
		if (cart_first) {
		  cart = new_cart;
		  event_type = "cart_first";
		  cart_first = false;
		}
	      } else {
		if (abs((int)new_cart.size() - (int)cart.size()) != 1) {
		  std::cerr << "CART DIFF:" << new_cart.size() << " - " << cart.size() << "\n";
		}
		if (new_cart.size() > cart.size()) {
		  event_type = "cart_add";
		} else if (new_cart.size() < cart.size()) {
		  event_type = "cart_remove";
		} else {
		  event_type = "cart_view";
		}
	      }
	    } else if ((ii->tag == "productpage") || (e -> ensighten.pageType == "PRODUCT")) {
	      event_type = "productpage";
	    } else if (ii->tag == "order") {
	      event_type = "order";
	      is_converter = true;
	    } else if (ii->tag == "featured") {
	      event_type = "featured";
	    } else {
	      event_type = "mistake";
	    }
	  }
	} else {
	  event_type = "listing";
	}

	if (!(is_completed)) hash_string.push_back(event_type);
	if (is_converter) is_completed = true;
	



	
	for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  if ((e -> ensighten.pageType == "PRODUCT") || (ii -> tag == "productpage")) {
	    is_product = true;
	    break;
	  }
	}
	
	if (is_product) {
	  std::string crumb_key = "";
	  if (e->ensighten.crumbs.size() != 4) continue;
	  if (e->ensighten.crumbs[0] == "O.biz" || e->ensighten.crumbs[0] == "Eziba") continue;
	  for (int q = 0; q < e->ensighten.crumbs.size(); q++) {
	    if (crumb_key != "") crumb_key += "|";
	    crumb_key += e->ensighten.crumbs[q];
	    user_crumbs.insert(crumb_key);
	  }
	  auto it = flat_crumb_stats.find(crumb_key);
	  if (it == flat_crumb_stats.end()) {
	    flat_crumb_stats[crumb_key] = 1;
	  } else {
	    it->second++;
	  }
	}
      } // event
    } // time
    

    for (auto c = user_crumbs.begin(); c != user_crumbs.end(); c++) {
      auto ci = global_crumb_stats.find(*c);
      if (ci == global_crumb_stats.end()) {
	hash_struct hs = {};
	global_crumb_stats[*c] = hs;
	ci = global_crumb_stats.find(*c);
      }
      ci->second.users ++;
      if (is_converter) {
	ci->second.converters++;
	std::string true_hash_string;
	for (auto h = hash_string.begin(); h != hash_string.end(); h++) {
	  if (true_hash_string != "") true_hash_string += "|";
	  true_hash_string += (*h);
	}
	auto ch = ci->second.hash.find(true_hash_string);
	if (ch == ci->second.hash.end()) {
	  ci->second.hash[true_hash_string] = 1;
	} else {
	  ci->second.hash[true_hash_string] ++;
	}
      }
    }


    
  } // json hostory

 

  for (auto it = global_crumb_stats.begin(); it != global_crumb_stats.end(); it++) {
    file << it->first << "\t" << it->second.users << "\t" << it->second.converters << "\t";
    std::multimap<long, std::string> inverter;
    int test = 0;
    for (auto it2 = it->second.hash.begin(); it2 != it->second.hash.end(); it2++) {
      test ++;
      inverter.insert(std::pair<long, std::string>(it2->second, it2->first));
    }
    int limit = 10;
    long coverage = 0;
    std::string json_out = "[";
    for (auto it2 = inverter.rbegin(); it2 != inverter.rend(); it2++) {
      if (json_out != "[") json_out += ",";
      json_out += ",\"" + it2->second + "\",\"" + std::to_string(it2->first) + "\"";
      coverage += it2->first;
      limit --;
      if (limit == 0) break;
    }
    json_out += "]";
    file << json_out << "\n";
  }
  


 

  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}


/*
O.biz
Eziba
*/
