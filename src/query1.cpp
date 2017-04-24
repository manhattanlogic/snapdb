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
  
   for (auto i = json_history.begin(); i != json_history.end(); i++) {
     bool  is_converter = false;
     std::unordered_set<std::string> camSources;
     std::unordered_set<std::string> camGroups;
     std::unordered_set<std::string> browsers;
     
      for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
	for (int e = 0; e < j->second.events.size(); e++) {
	  auto event = j->second.events[e];
	  if (event.ensighten.exists) {
	    std::string camSource = event.ensighten.camSource;
	    std::string camGroup = event.ensighten.camGroup;
	    std::string browser = event.ensighten.browser;
	    for (int it = 0; it < event.ensighten.items.size(); it ++) {
	      if (event.ensighten.items[it].tag == "order") is_converter = true;
	    }
	  }
	}
	// if (is_converter) break;
      }
      if (browsers.size() > 1) {
	dual_users++;
	if (is_converter) {
	  dual_converters ++;
	}
      }
   }

   result << dual_users << "," << dual_converters << "\n";
  
  // end of custom code
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}




