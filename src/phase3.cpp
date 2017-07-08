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



/*
WE NEED TO ADD:
1. purchased product counter
2. observed  product counter
 */



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
  if (device == "AndroidPhone" || device == "BlackBerry" || device == "iPhone" || device == "iPod" || device == "UnknownMobile" || device == "Windows Phone") {
    return "mobile";
  }
  if (device == "Unknown" && os == "iPhone OS") {
    return "mobile";
  }
  if (device == "AndroidTablet" || device == "iPad" ||  device == "Kindle Fire") {
    return "tablet";
  }
  if (device == "Unknown" && os == "CrOS") {
    return "tablet";
  }
  return "other";
}


struct stats_struct {
  std::unordered_set<unsigned long> users;
  unsigned long flat_users = 0;
  std::map<std::string, std::unordered_set<unsigned long> > converters_category;
  std::map<std::string, std::unordered_set<unsigned long> > producters_category;
  std::map<std::string, std::unordered_set<unsigned long> > carters_category;

  
  unsigned long viewed_items;
  std::map<std::string, unsigned long> viewed_items_category;
  
  unsigned long cart_users;
  std::map<std::string, unsigned long> cart_users_category;
  
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
  std::map<std::string, std::set<unsigned int> > invoices_category; // stores invoice_ids
  std::set<unsigned int> invoices;
  
  std::map<std::string, std::set<std::string> > skus_category;
  std::set<std::string> skus;

  std::set<std::string> product_skus;
  std::map<std::string, std::set<std::string> > product_skus_category;

  std::set<unsigned long> product_users;
  std::map<std::string, std::set<unsigned long> > product_users_category;

  unsigned long purchaed_items;
  std::map<std::string, unsigned long> purchaed_items_category;

  
  
};

std::unordered_map<std::string, std::string> sku_category;
std::set<std::string> top_categories;
std::unordered_map<std::string, stats_struct> stats;

void update_stats(unsigned long vid, unsigned long ts, std::string os, std::string device, std::string chennel,
		  std::string group, std::string creative, bool is_treated,
		  std::string country, std::string state, std::string city, std::string metro, std::string weekday, std::string hour) {



  

  /* group by state, weekday, hour */
  
  std::string record_id = os + "\t" + device+ "\t" + device_to_device_type(os, device) + "\t" + chennel+ "\t" + group + "\t" + creative +
    "\t" + country + "\t" + state + "\t" + city + "\t" + metro;
  auto it = stats.find(record_id);
  if (it == stats.end()) {
    stats_struct ss = {};
    for (auto it_2 = top_categories.begin(); it_2 != top_categories.end(); it_2++) {
      ss.order_category_value[*it_2]   =  0;
      ss.invoices_category[*it_2]      = {};
      ss.skus_category[*it_2]          = {};
      ss.product_skus_category[*it_2]  = {};
      ss.product_users_category[*it_2] = {};
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

  /*
    1. spending in category
    2. order skus in category 
   */

  
  if (user_info.is_valid) {
    it->second.overstock_users.insert(vid);
    if (is_treated) {
      it->second.overstock_impressions++;
    }

    // std::cerr << "user_info.invoices.size():" << user_info.invoices.size() << "\n";
    
    if (user_info.invoices.size() > 0) {
      std::cerr << user_info.invoices.begin()->first << "\n";
      if (is_treated) {
	it->second.converter_impressions++;
      }
      it->second.converter_users.insert(vid);

      for (auto it_i = user_info.invoices.begin(); it_i != user_info.invoices.end(); it_i++) {
	for (auto it_s = it_i->second.begin(); it_s != it_i->second.end(); it_s++) {
	  auto it_3 = sku_category.find(it_s->first);
	  std::string category = "UNKNOWN";
	  if (it_3 != sku_category.end()) category = it_3->second;
	  it->second.order_category_value[category] += it_s->second;
	  
	  it->second.invoices_category[category].insert(it_i->first);
	  it->second.invoices.insert(it_i->first);

	  it->second.skus_category[category].insert(it_s->first);
	  it->second.skus.insert(it_s->first);
	}
      }
    }

    if (user_info.product_skus.size() > 0) {
      it->second.product_users.insert(vid);
      
      for (auto p = user_info.product_skus.begin(); p != user_info.product_skus.end(); p++) {
	it->second.product_skus.insert(*p);
	
	auto it_3 = sku_category.find(*p);
	std::string category = "UNKNOWN";
	if (it_3 != sku_category.end()) category = it_3->second;

	it->second.product_skus_category[category].insert(*p);
	it->second.product_users_category[category].insert(vid);
	
	
      }
    }

    
    if (is_treated) {
      if (user_info.is_clicker) {
	it->second.clicker_impressions++;
	it->second.clicker_users.insert(vid);
      }
      if ((user_info.invoices.size() > 0) && user_info.is_clicker) {
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



struct marginal_user_struct {
  std::string weekday;
  std::string daytime;
  std::string state;
  std::string os;
  std::string device;
};

std::unordered_map<unsigned long, std::map<unsigned long, marginal_user_struct> > treated_users;

std::string hour_transformer(std::string hour) {
  int h = 0;
  try {
    h = std::stoul(hour);
  } catch(...) {
    std::cerr << "HOUR:" << hour << "\n";
    return ("---");
  }
  if (h < 4) return  ("0-3:59");
  if (h < 8) return  ("4-7:59");
  if (h < 12) return ("8-11:59");
  if (h < 16) return ("12-15:59");
  if (h < 20) return ("16-19:59");
  return ("20-23");
}

extern "C"
char * query() {
  /*
   * - add unique users

   */

  
  std::stringstream result_str;
  std::string line;

  std::ofstream result("result_phase_3.csv");
  
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

  long linelimit = 100000;
  
  std::ifstream imp_data("impressions_compact_v3.csv");

  while (std::getline(imp_data, line)) {
    // if (linelimit-- <= 0) break;
    auto parts = basic_split_string(line, "\t");

    if (parts.size() < 12) {
      std::cerr << "===>" << parts.size() << line << "\n"; 
      continue;
    }
    unsigned long vid = std::stoul(parts[0]);
    unsigned long ts = std::stoul(parts[1]) / 1000;

    std::string os = parts[2];
    std::string device = parts[3];
    
    std::string country = parts[6];
    std::string state = parts[7];
    std::string weekday = parts[10];
    

    
    
    if (country != "US" || state == "") {
      // std::cerr << "loaction: " << country << " " << state << "\n";
      continue;
    }

    if (state == "NONE") {
      continue;
    }

    std::string dayhour = hour_transformer(parts[11]);
    
    if (dayhour == "---") {
      std::cerr << country << " " << state << "\n";
      std::cerr << line << "\n";
      continue;
    }
    
    auto info = get_user_info(vid);
    if (!(info.is_valid)) {
      // std::cerr << "OS\n";
      continue;
    }
    auto it = treated_users.find(vid);
    if (it == treated_users.end()) {
      treated_users[vid] = {};
      it = treated_users.find(vid);
    }
    it->second[ts] = {weekday, dayhour, state, os, device};
    // std::cerr << "treated_users:" << treated_users.size() << "\n";
  }

  std::cerr << treated_users.size() << " impression users loaded\n";

  for (auto it = treated_users.begin(); it != treated_users.end(); it++) {
    auto info = get_user_info(it->first, it->second.rbegin()->first);
    if (!(info.is_valid)) continue;
    std::string record_id = device_to_device_type(it->second.rbegin()->second.os, it->second.rbegin()->second.device) + 
      "\t" + it->second.rbegin()->second.state + "\t" + it->second.rbegin()->second.weekday +
      "\t" + it->second.rbegin()->second.daytime + "\t" + std::to_string(std::min(it->second.size(), (unsigned long)5));

    auto sit = stats.find(record_id);

    if (sit == stats.end()) {
      stats_struct ss = {};
      for (auto it_2 = top_categories.begin(); it_2 != top_categories.end(); it_2++) {
	ss.converters_category[*it_2]    = {};
	ss.producters_category[*it_2]         = {};
	ss.carters_category[*it_2]          = {};
	ss.flat_users = 0;
      }
      stats[record_id] = ss;
      sit = stats.find(record_id);
    }

    sit->second.flat_users += 1;
    

    for (auto it_i = info.invoices.begin(); it_i != info.invoices.end(); it_i++) {
      for (auto it_s = it_i->second.begin(); it_s != it_i->second.end(); it_s++) {
	auto it_3 = sku_category.find(it_s->first);
	std::string category = "UNKNOWN";
	if (it_3 != sku_category.end()) category = it_3->second;
	sit->second.converters_category[category].insert(it->first);
      }
    }

    for (auto p = info.cart_skus.begin(); p != info.cart_skus.end(); p++) {
      auto it_3 = sku_category.find(*p);
      std::string category = "UNKNOWN";
      if (it_3 != sku_category.end()) category = it_3->second;
      sit->second.carters_category[category].insert(it->first);
    }
    
    for (auto p = info.product_skus.begin(); p != info.product_skus.end(); p++) {
      auto it_3 = sku_category.find(*p);
      std::string category = "UNKNOWN";
      if (it_3 != sku_category.end()) category = it_3->second;
      sit->second.producters_category[category].insert(it->first);
    }
    
    
  }


  result << "device_type\t";
  result << "state\tweekday\tdaytime\t";
  result << "impressions\tusers";

  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "CONV:" << *it;
  }

  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "CART:" << *it;
  }

  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "PROD:" << *it;
  }

  result << "\n";
  
  for (auto lit = stats.begin(); lit != stats.end(); lit++) {
    result << lit->first;
    result << "\t" << lit->second.flat_users;
    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << lit->second.converters_category[*it].size();
    }
    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << lit->second.carters_category[*it].size();
    }
    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << lit->second.producters_category[*it].size();
    }
    result << "\n";
  }
  



  
  /*
  std::unordered_set<unsigned long> users;
  std::map<std::string, unsigned long> converters_category;
  std::map<std::string, unsigned long> items_category;
  std::map<std::string, unsigned long> cart_category;
  */



  
  /*
  
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

    update_stats(vid, ts, parts[2], parts[3], parts[4], tags.og, tags.crv, true, parts[6], parts[7], parts[8], parts[9],
		 parts[10], parts[11]);
    
  }

  std::cerr << "done processing impressions\n";
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    auto vid = i->first;
    if (impression_vids.find(vid) != impression_vids.end()) continue;
    auto user_info = get_user_info(vid);
    if (user_info.is_valid) {
      update_stats(vid, 0, user_info.os, user_info.device, "---", "NONE", "NONE", false, "NONE","NONE","NONE","NONE","NONE","NONE");
    }
  }

  
  result << "os\tdevice\tdev_type\tchannel\tgroup\tcreative\tcountry\tstate\tcity\tmetro\t";
  result << "rj users\trj impressions\t";
  result << "os users\tos impressions\t";
  result << "converters\tconv impressions\t";
  result << "clickers\tclick impressions\t";
  result << "clickthrough converters\tclickthrough impressions\t";
  result << "first\tlast";

  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "VAL:" << *it;
  }
  result << "\tVAL:TOTAL";

  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "SKU:" << *it;
  }
  result << "\tSKU:TOTAL";

  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "INV:" << *it;
  }
  result << "\tINV:TOTAL";

  // ---------------------------

  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "PKU:" << *it;
  }
  result << "\tPKU:TOTAL";


  for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
    result << "\t" << "PRD:" << *it;
  }
  result << "\tPRD:TOTAL";


  
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

    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << i->second.invoices_category[*it].size();
    }
    result << "\t" << i->second.invoices.size();

    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << i->second.skus_category[*it].size();
    }
    result << "\t" << i->second.skus.size();


    //--------------------------

    
    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << i->second.product_users_category[*it].size();
    }
    result << "\t" << i->second.product_users.size();

    for (auto it = top_categories.begin(); it != top_categories.end(); it++) {
      result << "\t" << i->second.product_skus_category[*it].size();
    }
    result << "\t" << i->second.product_skus.size();



    
    result << "\n";
  }


  */

  result_str << "ok\n";
  
  char * buffer = (char *)malloc(result_str.str().size() + 1);
  memcpy(buffer, result_str.str().c_str(), result_str.str().size());
  buffer[result_str.str().size()] = 0;
  return buffer;
}
