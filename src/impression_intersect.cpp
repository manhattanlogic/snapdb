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
    unsigned long alt_converters = 0;
    unsigned long users_intersection = 0;
    unsigned long meaningful_users_intersection = 0;
    unsigned long converters_intersection = 0;
    std::unordered_set<std::string> ips;
  };

  std::map<std::string, stats_struct> stats;
  stats["all"] = {};
  
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
    bool is_revjet = false;
    std::string browser = "";

    std::string ua = "";
    
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) { 
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	browser = e->ensighten.browser;

	if ((e->ensighten.camSource == "DglBrand") || (e->ensighten.camSource == "Digital Brand") ||
	    (e->ensighten.camSource == "RevJet Acq")) {
	  is_revjet = true;
	}
	if ((e->ensighten.camGroup == "DglBrand") || (e->ensighten.camGroup == "Digital Brand") ||
	    (e->ensighten.camGroup == "RevJet Acq")) {
	  is_revjet = true;
	}


	
	if (stats.find(browser) == stats.end()) stats[browser] = {};
	stats[browser].ips.insert(e->ip);
	stats["all"].ips.insert(e->ip);
	
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

    std::vector<std::string> tmp = {"all", browser};

    for (auto b = tmp.begin(); b != tmp.end(); b++) {
      std::string browser = *b;
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


/*

LONG HISTORY STATS

-----------------  -----------------
users:632
meaningful_users:0
converters:0
ips:12
users_intersection:0
meaningful_users_intersection:0
converters_intersection:0
----------------- all -----------------
users:5959605
meaningful_users:4451918
converters:246177
ips:4717674
users_intersection:230168
meaningful_users_intersection:154076
converters_intersection:8487
----------------- d -----------------
users:3495095
meaningful_users:2681712
converters:179504
ips:2831045
users_intersection:146018
meaningful_users_intersection:116594
converters_intersection:7080
----------------- m -----------------
users:1830465
meaningful_users:1308233
converters:47078
ips:1537321
users_intersection:20778
meaningful_users_intersection:16676
converters_intersection:787
----------------- t -----------------
users:633413
meaningful_users:461973
converters:19595
ips:644818
users_intersection:63372
meaningful_users_intersection:20806
converters_intersection:620


SHORT HISTORY USERS

-----------------  -----------------
users:775497
meaningful_users:0
converters:0
ips:3
users_intersection:53
meaningful_users_intersection:0
converters_intersection:0
----------------- all -----------------
users:53621785
meaningful_users:22395023
converters:215648
ips:7090841
users_intersection:130937
meaningful_users_intersection:38284
converters_intersection:3
----------------- d -----------------
users:16854826
meaningful_users:7313297
converters:81938
ips:2327695
users_intersection:61947
meaningful_users_intersection:33209
converters_intersection:1
----------------- m -----------------
users:21375387
meaningful_users:9428582
converters:88351
ips:3854375
users_intersection:8300
meaningful_users_intersection:2515
converters_intersection:1
----------------- t -----------------
users:14616075
meaningful_users:5653144
converters:45359
ips:1418302
users_intersection:60637
meaningful_users_intersection:2560
converters_intersection:1



*/
