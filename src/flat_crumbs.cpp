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
  double order_total;
  double exact_order_total;
};

std::unordered_map<std::string, hash_struct> global_crumb_stats;


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

std::vector<std::string> split_string(std::string line, const char * sep = " ") {
  std::vector <std::string> result;
  char * pch = strtok ((char *)line.c_str(), sep);
  while (pch != NULL) {
    result.push_back(pch);
    pch = strtok (NULL, sep);
  }
  return result;
}

std::unordered_map<std::string, std::vector<std::string> > sku_crumbs;

extern "C"
char * query() {
  std::ofstream file("flat_crumb_stats.csv");
  
  std::unordered_map<int, int> vector_stats;
  std::stringstream result;

  /* build crambs dictionary & subcatId dictionary for product crumb reporting*/
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (e->ensighten.items.size() > 0) {
	  if ((e->ensighten.items[0].tag == "productpage") || (e -> ensighten.pageType == "PRODUCT")) {
	    sku_crumbs[e->ensighten.items[0].sku] = e->ensighten.crumbs;
	  }
	}
      }
    }
  }

  std::cerr << sku_crumbs.size() << " crumbs loaded\n";

  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    bool is_completed = false;
    
    std::unordered_set<std::string> cart;
    bool cart_first = true;
    std::vector<std::string> hash_string;
    std::unordered_set<std::string> user_crumbs;
    double order_total = 0.0;

    

    
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      bool is_product = false;
      if (j->second.events == NULL) continue;
      
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;

	std::string event_type = "X";
	std::string order_sku = "";
	std::unordered_set <std::string> new_cart;
	for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  if (ii -> tag == "cart") {
	    new_cart.insert(ii->sku);
	  }
	}
	if (e -> ensighten.searchTerm != "") {
	  event_type = "search";
	} else if (e->ensighten.items.size() > 0) {
	  auto ii = &e->ensighten.items[0];
	  //for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  if (ii -> tag == "cart") {
	    if (cart_first) {
	      cart = new_cart;
	      event_type = "cart_first";
	      cart_first = false;
	    } else {
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
	    order_sku = ii->sku;
	    if (!(is_completed)) order_total += ii->price * ii->quantity;
	  } else if (ii->tag == "featured") {
	    event_type = "featured";
	  } else {
	    event_type = "mistake";
	    std::cerr << "mistake:" << ii->tag << "\n";
	  }
	  //}
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

	if (is_converter) {
	  std::string crumb_key = "";
	  auto it = sku_crumbs.find(order_sku);
	  if (it == sku_crumbs.end()) {
	    std::cerr << "bad order sku:" << order_sku << "\n";
	  } else {
	    for (int q = 0; q < sku_crumbs[order_sku].size(); q++) {
	      std::string current_crumb = replace_all(sku_crumbs[order_sku][q], "&amp;", "&");
	      if (crumb_key != "") crumb_key += "|";
	      auto ci = global_crumb_stats.find(crumb_key);
	      if (ci == global_crumb_stats.end()) {
		hash_struct hs = {};
		global_crumb_stats[crumb_key] = hs;
		ci = global_crumb_stats.find(crumb_key);
	      }
	      ci->second.exact_order_total += order_total;
	    }
	  }
	}
	
	if (is_product) {
	  std::string crumb_key = "";
	  if (e->ensighten.crumbs.size() != 4) continue;
	  if (e->ensighten.crumbs[0] == "O.biz" || e->ensighten.crumbs[0] == "Eziba") continue;
	  for (int q = 0; q < e->ensighten.crumbs.size(); q++) {
	    std::string current_crumb = replace_all(e->ensighten.crumbs[q], "&amp;", "&");
	    if (crumb_key != "") crumb_key += "|";
	    crumb_key += current_crumb;
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
	ci->second.order_total += order_total;
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
    std::map<std::string, long> spectrum;
    int test = 0;
    for (auto it2 = it->second.hash.begin(); it2 != it->second.hash.end(); it2++) {
      test ++;
      inverter.insert(std::pair<long, std::string>(it2->second, it2->first));

      auto seq_parts = split_string(it2->first, "|");

      for (auto itp = seq_parts.begin(); itp != seq_parts.end(); itp++) {
	auto its = spectrum.find(*itp);
	if (its == spectrum.end()) {
	  spectrum[*itp] = 1;
	} else {
	  its->second++;
	}
      }
    }

    std::string spectrum_out = "{";
    for (auto its = spectrum.begin(); its != spectrum.end(); its++) {
      if (spectrum_out != "{") spectrum_out += ",";
      spectrum_out += "\"" + its->first + "\":" + std::to_string(its->second);
    }
    spectrum_out += "}";
    int limit = 20;
    long coverage = 0;
    std::string json_out = "[";
    
    for (auto it2 = inverter.rbegin(); it2 != inverter.rend(); it2++) {
      if (coverage != 0) json_out += ",";
      json_out += "\"" + it2->second + "\",\"" + std::to_string(it2->first) + "\"";
      coverage += it2->first;
      limit --;
      if (limit == 0) break;
    }
    json_out += "]";
    file << coverage << "\t" << json_out << "\t" << it->second.order_total << "\t" << spectrum_out << "\t"
	 << it->second.exact_order_total << "\n";
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
