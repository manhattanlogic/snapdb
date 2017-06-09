#include <iostream>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include "util.hpp"
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <memory.h>
#include <fstream>
#include <algorithm>
#include <string>



struct uuid_count {
  unsigned long vid;
  unsigned int len;
};

extern "C"
char * query() {
  std::stringstream result;
  long file_size = get_filesize("vid_map.dat");
  uuid_count * data = (uuid_count *)malloc(file_size);
  FILE * f = fopen("vid_map.dat", "rb");
  fread(data, file_size, 1, f);
  fclose(f);

  unsigned long users = 0;
  unsigned long converters = 0;
  unsigned long users_intersection = 0;
  unsigned long converters_intersection = 0;
  
  std::unordered_map<unsigned long, unsigned int> impressions;
  for (int i = 0; i < (file_size / 12); i++) {
    impressions[data[i].vid] = data[i].len;
  }
  std::cerr << "loaded\n";
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    
    bool is_converter = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  if (ii -> tag == "order") {
	    is_converter = true;
	  }
	}
      }
    }
    users ++;
    if (impressions.find(i->first) != impressions.end()) {
      users_intersection ++;
    }
    if (is_converter) {
      converters ++;
      if (impressions.find(i->first) != impressions.end()) {
	converters_intersection ++;
      }
    }
  }



  
  free(data);
  result << "users:" << users << "\n";
  result << "converters:" << converters << "\n";
  result << "users_intersection:" << users_intersection << "\n";
  result << "converters_intersection:" << converters_intersection << "\n";
  
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
