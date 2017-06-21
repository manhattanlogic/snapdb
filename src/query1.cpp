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


  unsigned long users = 0;
  unsigned long crossusers = 0;
  unsigned long imps_users = 0;
  unsigned long imps_crossusers = 0;
  
  for (long i = 0; i < l / sizeof(vid_record); i++) {
    //std::cerr << data[i].vid << " " << data[i].users << " " << data[i].crossusers << "\n";
    if (data[i].users > 0) {
      users++;
    }
    if (data[i].crossusers > 0) {
      crossusers++;
    }
    imps_users += data[i].users;
    imps_crossusers += data[i].crossusers;
  }
  result << "users: " << users << "\ncrossusers: " << crossusers << "\n";
  result << "imps_users: " << imps_users << "\nimps_crossusers: " << imps_crossusers << "\n";


  
  
  // end of custom code
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
