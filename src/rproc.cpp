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

extern "C"
char * query() {
  std::ofstream file("sku_stats.csv");
  
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
	  for (int q = 0; q < e->ensighten.crumbs.size(); q++) {	    
	    if (q >= crumb_stats.size()) {
	      std::unordered_map<std::string, long> map;
	      crumb_stats.push_back(map);
	    }
	    auto it = crumb_stats[q].find(e->ensighten.crumbs[q]);
	    crumbs.insert(e->ensighten.crumbs[q]);
	    if (it == crumb_stats[q].end()) {
	      crumb_stats[q][e->ensighten.crumbs[q]] = 1;
	    } else {
	      it->second++;
	    }
	  }
	}
      }
    }
  }

  
  std::ofstream out_file("crumbs_w2v_source.txt");
  

  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    bool printed = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	
	for (int q = 0; q < e->ensighten.crumbs.size(); q++) {	    
	  std::string crumb = e->ensighten.crumbs[q];
	  if (crumbs.find(crumb) != crumbs.end()) {
	    if (printed) out_file << " ";
	    std::replace(crumb.begin(), crumb.end(), ' ', '_');
	    std::replace(crumb.begin(), crumb.end(), '\t', '_');
	    std::replace(crumb.begin(), crumb.end(), '\n', '_');
	    out_file << crumb;
	    printed = true;
	  }
	}
	
      }
    }
    if (printed) out_file << "\n";
  }



  


  
  for (int i = 0; i < crumb_stats.size(); i++) {
    file << "------------------- " << i << " -------------------\n";
    for (auto it = crumb_stats[i].begin(); it != crumb_stats[i].end(); it++) {
      file << it->first << "\t" << it->second << "\n";
    }
  }
 

  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
