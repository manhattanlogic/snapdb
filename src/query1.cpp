#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>
#include <iostream>
#include "util.hpp"

// history_filter contains filter for the display application

extern "C"
char * query_1() {
  std::stringstream result;

  std::ofstream vids("long_vids.dat",std::ios::binary);
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    vids.write((char *)&i->first, sizeof(unsigned long));
  }
  
  // end of custom code
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}


extern "C"
char * query() {
  std::stringstream result;
  struct vid_record {
    unsigned long vid;
    unsigned long users;
    unsigned long crossusers;
  };
  
  result << "sizeof(vid_record):" << sizeof(vid_record) << "\n";
  auto l = get_filesize("vid_map_dual.dat");
  vid_record * data = (vid_record *)malloc(l);
  FILE * f = fopen("vid_map_dual.dat","rb");
  auto j = fread(data, l / sizeof(vid_record), sizeof(vid_record), f);
  fclose(f);

  struct info_struct {
    unsigned long users = 0;
    unsigned long crossusers = 0;
    unsigned long imps_users = 0;
    unsigned long imps_crossusers = 0;
  };

  std::unordered_map<std::string, info_struct> imp_stats;
  
  for (long i = 0; i < l / sizeof(vid_record); i++) {

    auto it = json_history.find(data[i].vid);
    if (it == json_history.end()) continue;
    
    if (data[i].users > 0) {
      imp_stats["all"].users++;
    }
    if (data[i].crossusers > 0) {
      imp_stats["all"].crossusers++;
      // result << data[i].vid << "\n";
    }
    imp_stats["all"].imps_users += data[i].users;
    imp_stats["all"].imps_crossusers += data[i].crossusers;
  }

  for (auto it = imp_stats.begin(); it != imp_stats.end(); it++) {
    result << "-----     " << it->first << "     -----\n";
    result << "users: " << it->second.users << "\ncrossusers: " << it->second.crossusers << "\n";
    result << "imps_users: " << it->second.imps_users << "\nimps_crossusers: " << it->second.imps_crossusers << "\n";

  }
  
  
  // end of custom code
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
