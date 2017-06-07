#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>

// history_filter contains filter for the display application


struct accumulator_struct {
  unsigned long users = 0;
  unsigned long converters = 0;
  unsigned long producters = 0;
  unsigned long carters = 0;
  unsigned long group_to_revjet = 0;
  unsigned long group_from_revjet = 0;
  unsigned long source_to_revjet = 0;
  unsigned long source_from_revjet = 0;

  unsigned long conv_group_to_revjet = 0;
  unsigned long conv_group_from_revjet = 0;
  unsigned long conv_source_to_revjet = 0;
  unsigned long conv_source_from_revjet = 0;
  
  std::map<unsigned int, unsigned long> hist_len;
  std::map<unsigned int, unsigned long> conv_hist_len;
};


std::unordered_map<std::string, long> cam_name_stats;
std::unordered_map<std::string, long> unique_cam_name_stats;
extern "C"
char * query() {
  history_filter.clear();
  std::stringstream result;
  // end of common code

  // aggregate vars definition

  /*
  
    "camSource" -> item["subids"]["ensighten"]["camSource"]

  */

  std::unordered_map<std::string, accumulator_struct > camSourceStats;
  std::unordered_map<std::string, accumulator_struct > camGroupStats;

  
  int dual_users = 0;
  int dual_converters = 0;

  int dump = 100;
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    bool is_carter = false;
    bool is_producter = false;
    bool group_to_revjet = false;
    bool group_from_revjet = false;
    bool source_to_revjet = false;
    bool source_from_revjet = false;
    
    std::unordered_set<std::string> camSources;
    std::unordered_set<std::string> camGroups;
    std::unordered_set<std::string> cams;
    std::unordered_set<std::string> browsers;

    std::string last_cam_group = "";
    std::string last_cam_source = "";
    
    auto start =  i->second->history.begin()->second.ts;
    auto end =  i->second->history.rbegin()->second.ts;
    if (end - start < 100) continue;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events != NULL) {
	for (int e = 0; e < j->second.events->size(); e++) {
	  auto event = (*j->second.events)[e];
	  if (event.ensighten.exists) {
	    if (j != i->second->history.begin()) {
	      if ((event.ensighten.camSource == "DglBrand") || (event.ensighten.camSource == "Digital Brand") ||
		  (event.ensighten.camSource == "RevJet Acq")) {
		if ((last_cam_source != "DglBrand") && (last_cam_source != "Digital Brand") &&
		    (last_cam_source != "RevJet Acq")) {
		  source_to_revjet = true;
		}
	      } else {
		if ((last_cam_source == "DglBrand") || (last_cam_source == "Digital Brand") ||
		    (last_cam_source == "RevJet Acq")) {
		  source_from_revjet = true;
		}
	      }
	    }
	    
	    if (j != i->second->history.begin()) {
	      if ((event.ensighten.camGroup == "DglBrand") || (event.ensighten.camGroup == "Digital Brand") ||
		  (event.ensighten.camGroup == "RevJet Acq")) {
		if ((last_cam_group != "DglBrand") && (last_cam_group != "Digital Brand") &&
		    (last_cam_group != "RevJet Acq")) {
		  group_to_revjet = true;
		}
	      } else {
		if ((last_cam_group == "DglBrand") || (last_cam_group == "Digital Brand") ||
		    (last_cam_group == "RevJet Acq")) {
		  group_from_revjet = true;
		}
	      }
	    }
	    last_cam_group = event.ensighten.camGroup;
	    last_cam_source = event.ensighten.camSource;

	    std::string full_cam_name = event.ensighten.camGroup + ":" + event.ensighten.camSource;
	    auto it = cam_name_stats.find(full_cam_name);
	    if (it == cam_name_stats.end()) {
	      cam_name_stats[full_cam_name] = 1;
	    } else {
	      it->second++;
	    }
	    cams.insert(full_cam_name);
	    camSources.insert(event.ensighten.camSource);
	    camGroups.insert(event.ensighten.camGroup);
	    auto browser = event.ensighten.browser;
	    browsers.insert(browser);
	    //if (browser != "") browsers.insert(browser);
	    if (event.ensighten.pageType == "PRODUCT") {
	      is_producter = true;
	    }
	    for (int it = 0; it < event.ensighten.items.size(); it ++) {
	      if (event.ensighten.items[it].tag == "order") is_converter = true;
	    }
	    for (int it = 0; it < event.ensighten.items.size(); it ++) {
	      if (event.ensighten.items[it].tag == "cart") is_carter = true;
	    }
	    for (int it = 0; it < event.ensighten.items.size(); it ++) {
	      if (event.ensighten.items[it].tag == "productpage") is_producter = true;
	    }
	  }
	}
      }
    }


    long hist_len = i->second->history.size();

    for (auto c = cams.begin(); c != cams.end(); c++) {
      auto it = unique_cam_name_stats.find(*c);
      if (it == unique_cam_name_stats.end()) {
	unique_cam_name_stats[*c] = 1;
      } else {
	it->second++;
      }
    }
    
     
    for (auto c = camSources.begin(); c != camSources.end(); c++) {
      if ((*c == "DglBrand") || (*c == "Digital Brand") || (*c == "RevJet Acq")) {
	history_filter.insert(i->first);
      }
      
      if (camSourceStats.find(*c) == camSourceStats.end()) {
	accumulator_struct a;
	camSourceStats[*c] = a;
      }
      camSourceStats[*c].users ++;
      if (is_converter) {
	camSourceStats[*c].converters++;
      }
      if (is_carter) {
	camSourceStats[*c].carters++;
      }
      if (is_producter) {
	camSourceStats[*c].producters++;
      }
      if (source_to_revjet) {
	camSourceStats[*c].source_to_revjet++;
      }
      if (source_from_revjet) {
	camSourceStats[*c].source_from_revjet++;
      }
      if (is_converter) {
	if (source_to_revjet) {
	  camSourceStats[*c].conv_source_to_revjet++;
	  //result << i->first << "\n";
	}
	if (source_from_revjet) {
	  camSourceStats[*c].conv_source_from_revjet++;
	}
      }
      auto it = camSourceStats[*c].hist_len.find(hist_len);
      if (it == camSourceStats[*c].hist_len.end()) {
	camSourceStats[*c].hist_len[hist_len] = 1;
      } else {
	it->second++;
      }
      if (is_converter) {
	auto it = camSourceStats[*c].conv_hist_len.find(hist_len);
	if (it == camSourceStats[*c].conv_hist_len.end()) {
	  camSourceStats[*c].conv_hist_len[hist_len] = 1;
	} else {
	  it->second++;
	}
      }
    }


    for (auto c = camGroups.begin(); c != camGroups.end(); c++) {

      if ((*c == "DglBrand") || (*c == "Digital Brand") || (*c == "RevJet Acq")) {
	history_filter.insert(i->first);
      }
      
      if (camGroupStats.find(*c) == camGroupStats.end()) {
	accumulator_struct a;
	camGroupStats[*c] = a;
      }
      camGroupStats[*c].users ++;
      if (is_converter) {
	camGroupStats[*c].converters++;
      }
      if (is_carter) {
	camGroupStats[*c].carters++;
      }
      if (is_producter) {
	camGroupStats[*c].producters++;
      }
      if (group_to_revjet) {
	camGroupStats[*c].group_to_revjet++;
      }
      if (group_from_revjet) {
	camGroupStats[*c].group_from_revjet++;
      }
      if (is_converter) {
	if (group_to_revjet) {
	  camGroupStats[*c].conv_group_to_revjet++;
	}
	if (group_from_revjet) {
	  camGroupStats[*c].conv_group_from_revjet++;
	}
      }
      auto it = camGroupStats[*c].hist_len.find(hist_len);
      if (it == camGroupStats[*c].hist_len.end()) {
	camGroupStats[*c].hist_len[hist_len] = 1;
      } else {
	it->second++;
      }
      if (is_converter) {
	auto it = camGroupStats[*c].conv_hist_len.find(hist_len);
	if (it == camGroupStats[*c].conv_hist_len.end()) {
	  camGroupStats[*c].conv_hist_len[hist_len] = 1;
	} else {
	  it->second++;
	}
      }
    }
  }

  std::ofstream cam_source_stats("cam_source_stats.tsv");
  std::ofstream cam_group_stats("cam_group_stats.tsv");

  for (auto c = camSourceStats.begin(); c != camSourceStats.end(); c++) {
    cam_source_stats << c->first << "\t" << c->second.users << "\t" << c->second.converters << "\t" << c->second.producters << "\t" << c->second.carters << "\t";
    cam_source_stats << c->second.source_to_revjet << "\t" <<  c->second.source_from_revjet << "\t";
    cam_source_stats << c->second.conv_source_to_revjet << "\t" <<  c->second.conv_source_from_revjet << "\t";
    cam_source_stats << "\"";
    for (auto l = c->second.hist_len.rbegin(); l != c->second.hist_len.rend(); l++) {
      if (l != c->second.hist_len.rbegin()) cam_source_stats << ",";
      cam_source_stats << l->first << ";" << l->second;
    }
    cam_source_stats << "\"";
    cam_source_stats << "\t";
    cam_source_stats << "\"";
    for (auto l = c->second.conv_hist_len.rbegin(); l != c->second.conv_hist_len.rend(); l++) {
      if (l != c->second.conv_hist_len.rbegin()) cam_source_stats << ",";
      cam_source_stats << l->first << ";" << l->second;
    }
    cam_source_stats << "\"";
    cam_source_stats << "\n";
  }

  for (auto c = camGroupStats.begin(); c != camGroupStats.end(); c++) {
    cam_group_stats << c->first << "\t" << c->second.users << "\t" << c->second.converters << "\t" << c->second.producters << "\t" << c->second.carters << "\t";
    cam_group_stats << c->second.group_to_revjet << "\t" <<  c->second.group_from_revjet << "\t";
    cam_group_stats << c->second.conv_group_to_revjet << "\t" <<  c->second.conv_group_from_revjet << "\t";
    cam_group_stats << "\"";
    for (auto l = c->second.hist_len.rbegin(); l != c->second.hist_len.rend(); l++) {
      if (l != c->second.hist_len.rbegin()) cam_group_stats << ",";
      cam_group_stats << l->first << ";" << l->second;
    }
    cam_group_stats << "\"";
    cam_group_stats << "\t";
    cam_group_stats << "\"";
    for (auto l = c->second.conv_hist_len.rbegin(); l != c->second.conv_hist_len.rend(); l++) {
      if (l != c->second.conv_hist_len.rbegin()) cam_group_stats << ",";
      cam_group_stats << l->first << ";" << l->second;
    }
    cam_group_stats << "\"";
    cam_group_stats << "\n";
  }

  for (auto it = cam_name_stats.begin(); it != cam_name_stats.end(); it++) {
    result << it->first << ":" << it->second << ":";
    auto it2 = unique_cam_name_stats.find(it->first);
    if (it2 == unique_cam_name_stats.end()) {
      result << "-";
    } else {
      result << it2->second;
    }
    result << "\n";
  }
  
  
  // end of custom code
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}


/*
1. overstock data. rj data. by changing creative show possible influence on the funnel.
2. 
*/
