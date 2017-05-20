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

extern "C"
char * query() {
  std::ofstream file("flat_crumb_stats.csv");
  
  std::unordered_map<int, int> vector_stats;
  std::stringstream result;
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      bool is_product = false;
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
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
	  }
	  auto it = flat_crumb_stats.find(crumb_key);
	  if (it == flat_crumb_stats.end()) {
	    flat_crumb_stats[crumb_key] = 1;
	  } else {
	    it->second++;
	  }
	}
      }
    }
  }

 

  for (auto it = flat_crumb_stats.begin(); it != flat_crumb_stats.end(); it++) {
    file << it->first << "\t" << it->second << "\n";
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
