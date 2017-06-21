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
char * query_2() {
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

std::string ts_to_time(unsigned long tt) {
  struct tm * ptm = localtime((const time_t *)&tt);
  char buf[30];
  strftime (buf, 30, "%a, %d %b %YYYY %HH:%MM:%SS",  ptm);
  return buf;
}


extern "C"
char * query() {
  std::stringstream result;


  std::ifstream imp_data("impressions_compact.csv");
  struct stats_struct {
    std::unordered_set<unsigned long> users;
    unsigned long impressions;
    std::unordered_set<unsigned long> overstock_users;
    unsigned long overstock_impressions;
    std::unordered_set<unsigned long> converter_users;
    unsigned long converter_impressions;
    std::unordered_set<unsigned long> clicker_users;
    unsigned long clicker_impressions;
    std::unordered_set<unsigned long> clicker_converter_users;
    unsigned long clicker_converter_impressions;
    unsigned long min_time;
    unsigned long max_time;
  };
  std::string line;

  std::unordered_map<std::string, stats_struct> stats;
  
  while (std::getline(imp_data, line)) {
    auto parts = split_string(line, "\t");
    if (parts.size() < 6) continue;
    auto tags = split_string(parts[5], ",");
    if (tags.size() != 4) {
      std::cerr << parts[5] << "\n";
      continue;
    }
    std::string record_id = parts[2] + "\t" + parts[3] + "\t" + parts[4] + "\t" + tags[2] + "\t" + tags[3];
    auto it = stats.find(record_id);
    if (it == stats.end()) {
      stats_struct ss = {};
      stats[record_id] = ss;
      it = stats.find(record_id);
    }
    it->second.impressions ++;
    it->second.users.insert(std::stoul(parts[0]));

    if (it->second.min_time == 0) {
      it->second.min_time = std::stoul(parts[2]);
      it->second.max_time = std::stoul(parts[2]);
    } else {
      it->second.min_time = std::min(it->second.min_time, std::stoul(parts[2]));
      it->second.max_time = std::max(it->second.min_time, std::stoul(parts[2])); 
    }

    
    
    auto it2 = json_history.find(std::stoul(parts[0]));
    if (it2 != json_history.end()) {
      it->second.overstock_impressions ++;
      it->second.overstock_users.insert(std::stoul(parts[0]));
      bool is_converter = false;
      bool is_clicker = false;
      for (auto j = it2->second->history.begin(); j != it2->second->history.end(); j++) {
	if (j->second.events == NULL) continue;
	for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	  if (!(e->ensighten.exists)) continue;

	  if ((e->ensighten.camSource == "DglBrand") || (e->ensighten.camSource == "Digital Brand") ||
	      (e->ensighten.camSource == "RevJet Acq")) {
	    is_clicker = true;
	  }
	  if ((e->ensighten.camGroup == "DglBrand") || (e->ensighten.camGroup == "Digital Brand") ||
	      (e->ensighten.camGroup == "RevJet Acq")) {
	    is_clicker = true;
	  }
	  for (int itt = 0; itt < e->ensighten.items.size(); itt ++) {
	    if (e->ensighten.items[itt].tag == "order") is_converter = true;
	  }
	}
      }
      if (is_converter) {
	it->second.converter_impressions++;
	it->second.converter_users.insert(std::stoul(parts[0]));
      }
      if (is_clicker) {
	it->second.clicker_impressions++;
	it->second.clicker_users.insert(std::stoul(parts[0]));
      }
      if (is_converter && is_clicker) {
	it->second.clicker_converter_impressions++;
	it->second.clicker_converter_users.insert(std::stoul(parts[0]));
      }
    }
  }

  for (auto i = stats.begin(); i != stats.end(); i++) {
    result << i->first << "\t" << i->second.users.size() << "\t" << i->second.impressions << "\t";
    result << i->second.overstock_users.size() << "\t" << i->second.overstock_impressions << "\t";
    result << i->second.converter_users.size() << "\t" << i->second.converter_impressions << "\t";
    result << i->second.clicker_users.size() << "\t" << i->second.clicker_impressions << "\t";
    result << i->second.clicker_converter_users.size() << "\t" << i->second.clicker_converter_impressions << "\t";
    result << ts_to_time(i->second.min_time) << "\t" << ts_to_time(i->second.max_time) << "\n";
  }

  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
