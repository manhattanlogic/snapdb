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

  std::vector<int> stats[4] = {{0,0},{0,0},{0,0},{0,0}};
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    if (i->second->history.size() < 2) continue;

    auto first = i->second->history.begin();
    auto last = i->second->history.rbegin();

    if (last->second.ts - first->second.ts < 1000) continue;
    
    bool is_valid = false;
    bool is_converter = false;
    // all single(1), all plural(2), some single-some plural(3)
    int plural  = 0;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events.size() <= 0) continue;
      bool comma = j->second.pixels.find_first_of(",") != std::string::npos;
      switch (plural) {
      case 0:
	if (comma) {
	  plural = 2;
	} else {
	  plural = 1; 
	}
	break;
      case 1:
	if (comma) plural = 3;
	break;
      case 2:
	if (!(comma)) plural = 3;
	break;
      }
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

    if (true) {
      stats[plural][0]++;
      if (is_converter) {
	stats[plural][1]++;
      }
    }
    
  }

  result << history_filter.size() << " users found\n";

  result << stats[0][0] << "," << stats[0][1] << "\n";
  result << stats[1][0] << "," << stats[1][1] << "\n";
  result << stats[2][0] << "," << stats[2][1] << "\n";
  result << stats[3][0] << "," << stats[3][1] << "\n";
  
  
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


