#include <iostream>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>

extern std::string test_string;


extern "C"
char * f1() {
  std::stringstream result;
  history_filter.clear();
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    if (i->second->history.size() < 2) continue;
    bool is_valid = false;
    bool is_converter = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events.size() <= 0) continue;
      for (int e = 0; e < j->second.events.size(); e++) {
	auto event = j->second.events[e];
	if (!(event.ensighten.exists)) continue;

	if ((event.ensighten.camGroup == "DglBrand") ||
	  (event.ensighten.camSource == "Digital Brand") ||
	    (event.ensighten.camSource == "RevJet Acq")) {
	  is_valid = true;
	}
	
	for (int k = 0; k < event.ensighten.items.size(); k++) {
	  if (event.ensighten.items[k].tag == "order") {
	    is_converter = true;
	  }
	}
      }
    }
    if (is_valid)  {
      history_filter.insert(i->first);
    }
  }

  result << history_filter.size() << " users found\n";
  
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


