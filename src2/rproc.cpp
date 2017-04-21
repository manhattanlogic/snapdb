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
  std::unordered_set <unsigned long> ids;
  

  std::stringstream result;

  long users = 0;
  long events = 0;
  long tiny_events = 0;

  std::vector<unsigned int> global_stats = {0,0};
  
  std::unordered_map<std::string, std::vector<unsigned int> > camGroupStats; // users, converters
  std::unordered_map<std::string, std::vector<unsigned int> > camSourceStats;
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    if (i->second->history.size() < 2) continue;
    std::unordered_set<std::string> camGroups;
    std::unordered_set<std::string> camSources;
    bool is_converter = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events.size() <= 0) continue;
      auto event = j->second.events[j->second.active_event];
      if (!(event.ensighten.exists)) continue;
      camGroups.insert(event.ensighten.camGroup);
      camSources.insert(event.ensighten.camSource);
      for (int k = 0; k < event.ensighten.items.size(); k++) {
	if (event.ensighten.items[k].tag == "order") {
	  is_converter = true;
	}
      }
    }
    for (auto j = camGroups.begin(); j != camGroups.end(); j++) {
      if (camGroupStats.find(*j) == camGroupStats.end()) camGroupStats[*j] = {0,0};
      camGroupStats[*j][0] ++;
      if (is_converter) {
	camGroupStats[*j][1] ++;
      }
    }
    for (auto j = camSources.begin(); j != camSources.end(); j++) {
      if (camSourceStats.find(*j) == camSourceStats.end()) camSourceStats[*j] = {0,0};
      camSourceStats[*j][0] ++;
      if (is_converter) {
	camSourceStats[*j][1] ++;
      }
    }
    global_stats[0] ++;
    if (is_converter) {
      global_stats[1] ++;
    }
  }


  result << "------------- GLOBAL ------------\n";
  result<< global_stats[0] << "\t" << global_stats[1] << "\n";
  
  result << "------------- CAM GROUPS ------------\n";
  
  for (auto i = camGroupStats.begin(); i != camGroupStats.end(); i++) {
    result << i->first << "\t" << i->second[0] << "\t" << i->second[1] << "\n";
  }

  result << "------------- CAM SOURCES ------------\n";
  
  for (auto i = camSourceStats.begin(); i != camSourceStats.end(); i++) {
    result << i->first << "\t" << i->second[0] << "\t" << i->second[1] << "\n";
  }
  
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
