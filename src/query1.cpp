#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <set>
#include <memory.h>
#include <fstream>
#include <iostream>
#include "util.hpp"
#include <vector>

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

std::string ts_to_time(unsigned long _tt) {
  unsigned long tt = _tt;
  if (tt > 2000000000) tt /= 1000;
  struct tm * ptm = localtime((const time_t *)&tt);
  char buf[30];
  strftime (buf, 30, "%d %b %Y",  ptm);
  return buf;
}

std::string device_to_device_type(std::string os, std::string device) {
  if (device == "AppleDesktop" || device == "WindowsDesktop") {
    return "desktop";
  }
  if ((device == "Unknown" && os == "Mac OS") || (device == "Unknown" && os == "Windows") || (device == "Unknown" && os == "Linux")) {
    return "desktop";
  }
  if (device == "AndroidPhone" || device == "BlackBerry" || device == "iPhone" || device == "UnknownMobile" || device == "Windows Phone") {
    return "mobile";
  }
  if (device == "Unknown" && os == "iPhone OS") {
    return "mobile";
  }
  if (device == "AndroidTablet" || device == "iPad" || device == "iPod" || device == "Kindle Fire") {
    return "tablet";
  }
  if (device == "Unknown" && os == "CrOS") {
    return "tablet";
  }
  return "other";
}


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
  std::map<std::string, float> order_category_value;
};

std::unordered_map<std::string, std::string> sku_category;
std::set<std::string> top_categories;
std::unordered_map<std::string, stats_struct> stats;

void update_stats(unsigned long vid, unsigned long ts, std::string os, std::string device, std::string chennel,
		  std::string group, std::string creative, bool is_treated) {
  std::string record_id = os + "\t" + device+ "\t" + device_to_device_type(os, device) + "\t" + chennel+ "\t" + group + "\t" + creative;
  auto it = stats.find(record_id);
  if (it == stats.end()) {
    stats_struct ss = {};
    for (auto it_2 = top_categories.begin(); it_2 != top_categories.end(); it_2++) {
      ss.order_category_value[*it_2] = 0;
    }
    stats[record_id] = ss;
    it = stats.find(record_id);
  }

  if (is_treated) {
    it->second.impressions ++;
    it->second.users.insert(vid);
  }
  
  if (it->second.min_time == 0) {
    it->second.min_time = ts;
    it->second.max_time = ts;
  } else {
    it->second.min_time = std::min(it->second.min_time, ts);
    it->second.max_time = std::max(it->second.min_time, ts); 
  }

  auto user_info = get_user_info(vid);

  if (user_info.is_valid) {
    it->second.overstock_users.insert(vid);
    if (is_treated) {
      it->second.overstock_impressions++;
    }
    if (user_info.order_skus.size() > 0) {
      if (is_treated) {
	it->second.converter_impressions++;
      }
      it->second.converter_users.insert(vid);
      for (auto it_s = user_info.order_skus.begin(); it_s != user_info.order_skus.end(); it_s++) {
	auto it_3 = sku_category.find(it_s->first);
	std::string category = "UNKNOWN";
	if (it_3 != sku_category.end()) category = it_3->second;
	it->second.order_category_value[category] += it_s->second;
      }
    }

    if (is_treated) {
      if (user_info.is_clicker) {
	it->second.clicker_impressions++;
	it->second.clicker_users.insert(vid);
      }
      if (user_info.order_skus.size() && user_info.is_clicker) {
	it->second.clicker_converter_impressions++;
	it->second.clicker_converter_users.insert(vid);
      }
    }
  }
}

struct tags_struct {
  std::string og;
  std::string crv;
  bool valid = false;
};

tags_struct get_tags(std::string tags_in) {
  tags_struct result;
  auto tags = basic_split_string(tags_in, ",");
  if (tags.size() == 4) {
    result.og = tags[2];
    result.crv = tags[3];
    result.valid = true;
    return result;
  }
  if (tags.size() < 2) return result;
  std::string og, crv;
  for (int i = 0; i < tags.size(); i++) {
    if (tags[i].substr(0, 2) == "og") og = tags[i];
    if (tags[i].substr(0, 3) == "crv") crv = tags[i];
  }
  if (og != "" && crv != "") {
    result.og = og;
    result.crv = crv;
    result.valid = true;
    return result;
  }
  return result;
}


extern "C"
char * query() {
  std::stringstream result;
  std::string line;
  
  std::ifstream sku_crumbs_file("sku_crumbs.csv");
  while (std::getline(sku_crumbs_file, line)) {
    auto parts = basic_split_string(line, "\t");
    std::string val = parts[1];
    if (val== "Home & Garden") {
      val = "# " + parts[2];
    }
    top_categories.insert(val);
    sku_category[parts[0]] = val;
  }
  top_categories.insert("UNKNOWN");
  

  std::cerr << "time test:" << ts_to_time(1492049877) << "\n";
  std::cerr << "time test:" << ts_to_time(0) << "\n";
  
  std::ifstream imp_data("impressions_compact.csv");


  std::unordered_set<unsigned long> impression_vids;

  while (std::getline(imp_data, line)) {
    auto parts = basic_split_string(line, "\t");
    if (parts.size() < 6) continue;
    auto tags = get_tags(parts[5]);
    if (!(tags.valid)) {
      std::cerr << parts[5] << "\n";
      continue;
    }

    unsigned long vid = std::stoul(parts[0]);
    unsigned long ts = std::stoul(parts[1]);
    
    impression_vids.insert(vid);

    update_stats(vid, ts, parts[2], parts[3], parts[4], tags.og, tags.crv, true);
    
  }

  std::cerr << "done processing impressions\n";
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    auto vid = i->first;
    if (impression_vids.find(vid) != impression_vids.end()) continue;
    auto user_info = get_user_info(vid);
    if (user_info.is_valid) {
      update_stats(vid, 0, user_info.os, user_info.device, "---", "NONE", "NONE", false);
    }
  }

  
  result << "os\tdevice\tdev_type\tchannel\tgroup\tcreative\t";
  result << "rj users\trj impressions\t";
  result << "os users\tos impressions\t";
  result << "converters\tconv impressions\t";
  result << "clickers\tclick impressions\t";
  result << "clickthrough converters\tclickthrough impressions\t";
  result << "first\tlast";
  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << *it;
  }
  result << "\tTOTAL";
  result << "\n";
    
  for (auto i = stats.begin(); i != stats.end(); i++) {
    result << i->first << "\t" << i->second.users.size() << "\t" << i->second.impressions << "\t";
    result << i->second.overstock_users.size() << "\t" << i->second.overstock_impressions << "\t";
    result << i->second.converter_users.size() << "\t" << i->second.converter_impressions << "\t";
    result << i->second.clicker_users.size() << "\t" << i->second.clicker_impressions << "\t";
    result << i->second.clicker_converter_users.size() << "\t" << i->second.clicker_converter_impressions << "\t";
    result << ts_to_time(i->second.min_time) << "\t" << ts_to_time(i->second.max_time);
    float total = 0;
    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      total += i->second.order_category_value[*it];
    }
    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << i->second.order_category_value[*it];
    }
    result << "\t" << total;
    result << "\n";
  }

  
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
