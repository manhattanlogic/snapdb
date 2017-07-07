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

  unsigned long viewed_items;
  std::map<std::string, unsigned long> viewed_items_category;
  
};

std::unordered_map<std::string, std::string> sku_category;
std::set<std::string> top_categories;
std::unordered_map<std::string, stats_struct> stats;

void update_stats(unsigned long vid, unsigned long ts, std::string os, std::string device, std::string chennel,
		  std::string group, std::string creative, bool is_treated,
		  std::string country, std::string state, std::string city, std::string metro) {
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



// step 1
// colect and assemble this data for the Impressions X OS dataset
struct impression_summary_struct {
  int num_impressions = 0; // # of impressions shown to the user
  unsigned long ts = 0;    // time of the last impressions
  std::string day_of_week; // for the last impression
  std::string day_part;    // for the last impression
};
std::unordered_map<unsigned long, impression_summary_struct> impressions_summary;




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
  unsigned long vid;
  unsigned long ts; // last impressions's timestamp
  std::string day_of_week;
  std::string time_of_day;
};


extern "C"
char * query() {
  std::stringstream result_str;
  std::string line;

  std::ofstream result("result_p3.csv");
  
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
  
  std::ifstream imp_data("impressions_compact_v3.csv");


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

    update_stats(vid, ts, parts[2], parts[3], parts[4], tags.og, tags.crv, true, parts[6], parts[7], parts[8], parts[9]);
    
  }

  std::cerr << "done processing impressions\n";
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    auto vid = i->first;
    if (impression_vids.find(vid) != impression_vids.end()) continue;
    auto user_info = get_user_info(vid);
    if (user_info.is_valid) {
      update_stats(vid, 0, user_info.os, user_info.device, "---", "NONE", "NONE", false, "NONE","NONE","NONE","NONE");
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


  result_str << "ok\n";
  
  char * buffer = (char *)malloc(result_str.str().size() + 1);
  memcpy(buffer, result_str.str().c_str(), result_str.str().size());
  buffer[result_str.str().size()] = 0;
  return buffer;
}
