#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>




extern "C"
char * query_x() {
  std::stringstream result;
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    std::vector <std::string> skus;
    auto start =  i->second->history.begin()->second.ts;
    auto end =  i->second->history.rbegin()->second.ts;
    if (end - start < 1000) continue;
     
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events.size() != 1) continue;
      for (int e = 0; e < j->second.events.size(); e++) {
	auto event = j->second.events[e];
	if (event.ensighten.exists) {
	  bool is_product = event.ensighten.pageType == "PRODUCT";
	  for (int it = 0; it < event.ensighten.items.size(); it ++) {
	    if ((is_product) || (event.ensighten.items[it].tag == "productpage")) {
		skus.push_back(event.ensighten.items[it].sku);
	      }
	  } // items
	} // ensighten
      } // enents
    } // history
  

    if (skus.size() < 3) continue;
    for (int j = 0; j < skus.size(); j++) {
      if (j > 0) result << " ";
      result << skus[j];
    }
    result << "\n";
  } // user
  
  // end of custom code
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}

