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
  history_filter.clear();
  
  std::stringstream result;
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_revjet = false;
    bool is_converter = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	//for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  
	  if ((e->ensighten.camSource == "DglBrand") || (e->ensighten.camSource == "Digital Brand") ||
	      (e->ensighten.camSource == "RevJet Acq")) {
	    is_revjet = true;
	  }
	  if ((e->ensighten.camGroup == "DglBrand") || (e->ensighten.camGroup == "Digital Brand") ||
	      (e->ensighten.camGroup == "RevJet Acq")) {
	    is_revjet = true;
	  }

	  
	  
	  //}
      }
    }
    if (is_revjet) {
      history_filter.insert(i->first);
    }
  }

  


 
  result << "history_filter.size()=" << history_filter.size() << "\n";
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
