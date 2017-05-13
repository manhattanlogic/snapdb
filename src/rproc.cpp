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

	    /*
	    if ((q == 0) && (e->ensighten.crumbs[q] == "Eziba" ||
			     e->ensighten.crumbs[q] == "black consoles" ||
			     e->ensighten.crumbs[q] == "Full xfirm memory foam, gel")) {
	      result << i->first << "\n";
	    }
	    */
	    
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
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      bool printed = false;
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	
	for (int q = 0; q < e->ensighten.crumbs.size(); q++) {	    
	  std::string crumb = e->ensighten.crumbs[q];
	  if (crumbs.find(crumb) != crumbs.end()) {
	    if (printed) out_file << " ";
	    std::replace(crumb.begin(), crumb.end(), ' ', '_');
	    out_file << crumb;
	    printed = true;
	  }
	}
	
      }
      if (printed) out_file << "\n";
    }
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

/*

this user lives on 2 devices:
1612018255760278685
find the rest of these people

us is recorded by RevJet hence is more reliable

check pixel id for collisions

*/


/*
4702268199690718765
4808269916610931011
4702268199514061604
4702268199427755771
4714864204549145934
4808269916367956676
4808269916367956676
4808269916367956676
4714864204462500049
1611114259681282812
4714864204267032044
4808269915834083035
4702268185006065633
4702268185006065633
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
1701298266453447442
4714899388310437072
4808269915747422351
4702268198759337056
4702268198759337056
4702268198759337056
4702268198759337056
1703147617659203727
4808269915660600441
4702268198563738868
1702176900456452981
1702176900456452981
1702176900456452981
1702176900456452981
1702176900456452981
1702176900456452981
1702176900456452981
4702268198576350329
4702268198576350329
4702268198576350329
4702277008403267969
4702268198441394752
4808269915487935557
4702276993296148735
4808269915410445973
4808269915410445973
4702268198392640392
4702268189267296091
4702268198340583713
4808269915337182084
4808269915337182084
4808269915337182084
4808269915337182084
4808269915337182084
4702268198304300068
4702268199168445550
4714899387797984317
4808269915154873608
4702268192436540344
4702268192436540344
4808269915142255905
4702268198045115269
4702268197958784177
*/



