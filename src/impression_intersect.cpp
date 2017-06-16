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
} __attribute__((packed));



extern "C"
char * query() {

  std::unordered_map<std::string, unsigned long> ua_stats;
  std::unordered_map<std::string, unsigned long> conv_ua_stats;
  
  
  std::stringstream result;
  unsigned long file_size = get_filesize("vid_map.dat");
  std::cerr << "file_size:" << file_size << "\n";
  void * data = malloc(file_size);
  std::cerr << "data:" << (unsigned long)data << "\n";
  FILE * f = fopen("vid_map.dat", "rb");
  auto X = fread(data, file_size, 1, f);
  std::cerr << "X:" << X << "\n";
  std::cerr << "sizeof(uuid_count):" << sizeof(uuid_count) << "\n";
  std::cerr << "sizeof(unsigned long):" << sizeof(unsigned long) << "\n";
  std::cerr << "sizeof(unsigned int):" << sizeof(unsigned int) << "\n";
  fclose(f);

  struct stats_struct {
    unsigned long users = 0;
    unsigned long meaningful_users = 0;
    unsigned long converters = 0;
    unsigned long users_intersection = 0;
    unsigned long meaningful_users_intersection = 0;
    unsigned long converters_intersection = 0;
    std::unordered_set<std::string> ips;
  };

  std::map<std::string, stats_struct> stats;
  
  std::unordered_map<unsigned long, unsigned int> impressions;
  for (unsigned long i = 0; i < (file_size / (sizeof(unsigned long) + sizeof(unsigned int))); i++) {
    unsigned long idx = i * (sizeof(unsigned long) + sizeof(unsigned int));
    unsigned long vid = *(unsigned long *)((char *)data + idx);
    unsigned int len = *(unsigned int *)((char *)data + idx + sizeof(unsigned long));
    impressions[vid] = len;
  }
  
  std::cerr << "loaded\n";
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    bool is_meaningful = false;
    std::string browser = "";

    std::string ua = "";
    
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) { 
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	browser = e->ensighten.browser;
	
	if (stats.find(browser) == stats.end()) stats[browser] = {};
	stats[browser].ips.insert(e->ip);
	
	if (ua == "") {
	  ua = e->device_model + ":" + e->browser;
	}
	
	if (e->ensighten.pageType == "PRODUCT")  is_meaningful = true;
	for (auto ii = e->ensighten.items.begin(); ii != e->ensighten.items.end(); ii++) {
	  if (ii->tag == "order") is_converter = true;
	  if (ii->tag == "productpage") is_meaningful = true;
	  if (ii->tag == "featured") is_meaningful = true;	  
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
    
    stats[browser].users ++;
    if (impressions.find(i->first) != impressions.end()) {
      stats[browser].users_intersection ++;
    }
    if (is_converter) {
      stats[browser].converters ++;
      if (impressions.find(i->first) != impressions.end()) {
	stats[browser].converters_intersection ++;
      }
    }
    if (is_meaningful) {
      stats[browser].meaningful_users++;
      if (impressions.find(i->first) != impressions.end()) {
	stats[browser].meaningful_users_intersection++;
      }
    }
  }



  
  free(data);

  
  for (auto b = stats.begin(); b != stats.end(); b++) {
    result << "----------------- " << b->first << " -----------------\n";
    result << "users:" << b->second.users << "\n";
    result << "meaningful_users:" << b->second.meaningful_users << "\n";
    result << "converters:" << b->second.converters << "\n";
    result << "ips:" << b->second.ips.size() << "\n";
    result << "users_intersection:" << b->second.users_intersection << "\n";
    result << "meaningful_users_intersection:" << b->second.meaningful_users_intersection << "\n";
    result << "converters_intersection:" << b->second.converters_intersection << "\n";
  }


  /*
  result << "---------------  UA STATS ---------------\n";
  for (auto it = ua_stats.begin(); it != ua_stats.end(); it++) {
    result << it->first << ":" << it->second << "\n";
  }
  std::cerr << "CP1\n";
  result << "------------- CONV UA STATS ---------\n";
  for (auto it = conv_ua_stats.begin(); it != conv_ua_stats.end(); it++) {
    result << it->first << ":" << it->second << "\n";
  }

  std::cerr << "finished. sending back result\n";
  */
  
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
