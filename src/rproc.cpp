#include <iostream>
#include <string>
#include <stdlib.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>

extern std::string test_string;


void show_single_history(single_user_history *sh) {
  for (auto it = sh->history.begin(); it != sh->history.end(); it++) {
    std::cout << "\t" << it->first << " " << it->second->vid << std::endl;
  }
}

extern "C"
char * f1() {
  std::unordered_set <unsigned long> ids;
  int users = 0;
  int converters = 0;
  for (auto i = history.begin(); i != history.end(); i++) {
    bool is_conveter = false;

    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      for (int item = 0; item < j->second->ensighten.items.size(); item++) {
	if (j->second->ensighten.items[item]->tag == "order") {
	  is_conveter = true;
	}
      }

      auto test_it = ids.find(j->second->id);
      if (test_it != ids.end()) {
	std::cerr << "collision:" << j->second->id << "\n";
      } else {
	ids.insert(j->second->id);
      }
      
      
    }
    
    users ++;
    if (is_conveter) {
      converters++;
    }
  }

  std::stringstream result;
  
  result << (float)converters / users * 100 << "%\n";

  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
