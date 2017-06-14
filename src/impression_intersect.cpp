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

std::unordered_map<std::string, unsigned long> ua_stats;
std::unordered_map<std::string, unsigned long> conv_ua_stats;
std::unordered_set<std::string> ips;

extern "C"
char * query() {
  std::stringstream result;
  unsigned long file_size = get_filesize("vid_map.dat");
  uuid_count * data = (uuid_count *)malloc(file_size);
  FILE * f = fopen("vid_map.dat", "rb");
  fread(data, file_size, 1, f);
  fclose(f);

  unsigned long users = 0;
  unsigned long converters = 0;
  unsigned long users_intersection = 0;
  unsigned long converters_intersection = 0;
  
  std::unordered_map<unsigned long, unsigned int> impressions;
  for (unsigned long i = 0; i < (file_size / 12); i++) {
    impressions[data[i].vid] = data[i].len;
  }
  
  std::cerr << "loaded\n";
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    std::string ua = "";
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) { 
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	// ips.insert(e->ip);
	if (ua == "") {
	  ua = e->device_model + ":" + e->browser;
	}
	for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  if (ii -> tag == "order") {
	    is_converter = true;
	  }
	}
      }
    }
    if (ua != "") {
      auto it = ua_stats.find(ua);
      if (it == ua_stats.end()) {
	ua_stats[ua] = 1;
      } else {
	it->second++;
      }
      if (is_converter) {
	auto it = conv_ua_stats.find(ua);
	if (it == conv_ua_stats.end()) {
	  conv_ua_stats[ua] = 1;
	} else {
	  it->second++;
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
  result << "ips:" << ips.size() << "\n";
  result << "users_intersection:" << users_intersection << "\n";
  result << "converters_intersection:" << converters_intersection << "\n";

  result << "---------------  UA STATS ---------------\n";
  for (auto it = ua_stats.begin(); it != ua_stats.end(); it++) {
    result << it->first << ":" << it->second << "\n";
  }
  result << "------------- CONV UA STATS ---------\n";
  for (auto it = conv_ua_stats.begin(); it != conv_ua_stats.end(); it++) {
    result << it->first << ":" << it->second << "\n";
  }

  
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}