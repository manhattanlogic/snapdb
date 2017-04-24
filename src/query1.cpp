#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>




extern "C"
char * query() {
  std::stringstream result;
  // end of common code

  // aggregate vars definition

  /*
  
  "camSource" -> item["subids"]["ensighten"]["camSource"]

  */

  std::unordered_map<std::string, std::vector<long> > camSourceStats;
  std::unordered_map<std::string, std::vector<long> > camGroupStats;

  
  int dual_users = 0;
  int dual_converters = 0;

  int dump = 100;
  
   for (auto i = json_history.begin(); i != json_history.end(); i++) {
     bool  is_converter = false;
     std::unordered_set<std::string> camSources;
     std::unordered_set<std::string> camGroups;
     std::unordered_set<std::string> browsers;

     auto start =  i->second->history.begin()->second.ts;
     auto end =  i->second->history.rbegin()->second.ts;
     if (end - start < 1000) continue;
     
      for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
	for (int e = 0; e < j->second.events.size(); e++) {
	  auto event = j->second.events[e];
	  if (event.ensighten.exists) {
	    camSources.insert(event.ensighten.camSource);
	    camGroups.insert(event.ensighten.camGroup);
	    auto browser = event.ensighten.browser;
	    browsers.insert(browser);
	    //if (browser != "") browsers.insert(browser);
	    for (int it = 0; it < event.ensighten.items.size(); it ++) {
	      if (event.ensighten.items[it].tag == "order") is_converter = true;
	    }
	  }
	}
	if (is_converter) break;
      }
      /*
      if (browsers.size() > 1) {
	dual_users++;
	if (is_converter) {
	  dual_converters ++;
	}
      }
      if (browsers.size() > 2) {
	for (auto b = browsers.begin(); b != browsers.end(); b++) {
	  result << "[" << (*b) << "]";
	}
	result << "\n";
	dump--;
	if (dump <= 0) break;
      }
      */
      if (browsers.size() == 1) {
	for (auto c = camSources.begin(); c != camSources.end(); c++ ) {
	  std::string key = (*c) + "\t" + (*(browsers.begin()));
	  auto it = camSourceStats.find(key);
	  if (it == camSourceStats.end()) {
	    camSourceStats[key] = {0,0};
	    it = camSourceStats.find(key);
	  }
	  it->second[0]++;
	  if (is_converter) it->second[1] ++;
	}

	for (auto c = camGroups.begin(); c != camGroups.end(); c++ ) {
	  std::string key = (*c) + "\t" + (*(browsers.begin()));
	  auto it = camGroupStats.find(key);
	  if (it == camGroupStats.end()) {
	    camGroupStats[key] = {0,0};
	    it = camGroupStats.find(key);
	  }
	  it->second[0]++;
	  if (is_converter) it->second[1] ++;
	}

	
      }
   }

   result << "-----  camGroups -----\n";
   for (auto c = camGroupStats.begin(); c != camGroupStats.end(); c++) {
     result << c->first << "\t" << c->second[0] << "\t" << c->second[1] << "\n"; 
   }

   result << "-----  camSources -----\n";
   for (auto c = camSourceStats.begin(); c != camSourceStats.end(); c++) {
     result << c->first << "\t" << c->second[0] << "\t" << c->second[1] << "\n"; 
   }
   
   //result << dual_users << "," << dual_converters << "\n";
  
  // end of custom code
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}

/*
[d][t][m]
[t][m][d]
[t][m][d]
[d][t][m]
[m][d][t]
[t][m][d]
[t][m][d]
[t][m][d]
[d][m][t]
2432,145 [2 browser types users/converters] - 5.96% conversion rate (!)
[d][t][m]
[t][m][d]
[d][t][m]
[m][d][t]
[t][m][d]
[t][m][d]
[t][m][d]
[d][m][t]
2377,90   [2 browser types users/converters, stop at conversion] - 3.7% conversion rate
*/
