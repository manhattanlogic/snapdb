#include <iostream>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>

extern std::string test_string;


void show_single_history(single_user_history *sh) {
  for (auto it = sh->history.begin(); it != sh->history.end(); it++) {
    std::cout << "\t" << it->first << " " << it->second->vid << std::endl;
  }
}

extern "C"
char * f1() {
  std::unordered_set <unsigned long> ids;
  

  std::stringstream result;

  long users = 0;
  long events = 0;
  long tiny_events = 0;

  /*
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    users++;
    for (auto j = i->second.begin(); j != i->second.end(); j++) {
      events++;
      try {
	tiny_events += (*j->second.document)["events"].Size();
      } catch (...) {
      }
    }
  }

  result << "users:" << users << ", events:" << events << ", tiny events:" << tiny_events << "\n";
  */

  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
