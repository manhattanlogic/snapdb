#include <iostream>
#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <unordered_map>
#include <memory.h>
#include <fstream>
#include <algorithm>
#include <string>
#include "util.hpp"

/*
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
*/

extern std::string test_string;


extern "C"
char * query() {
  struct stats_struct {
    unsigned long _meaningful = 0;
    unsigned long _clickers = 0;
    unsigned long clickers_converters = 0;
    unsigned long clickers_converters_alt = 0;
    unsigned long clickers_meaningful = 0;
    unsigned long clickers_meaningful_alt = 0;
  };

  std::map<std::string, stats_struct> stats;
  
  history_filter.clear();
  
  std::stringstream result;
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    
    bool is_revjet = false;
    bool is_converter = false;
    bool is_clicker = false;
    bool is_meaningful = false;
    std::string browser = "";
    
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      auto pixel_string = replace_all(j->second.pixels, "'", "");
      rapidjson::Document d;
      d.Parse(pixel_string.c_str());
      if (d.HasParseError()) {
      } else {
	for (int q = 0; q < d.Size(); q++) {
	  if (d[q].GetInt() == 79) {
	    is_clicker = true;
	  }
	}
      }
      
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

	if (e->ensighten.pageType == "PRODUCT") {
	  is_meaningful = true;
	}
	
	for (int it = 0; it < e->ensighten.items.size(); it ++) {
	  if (e->ensighten.items[it].tag == "order") is_converter = true;
	  if (e->ensighten.items[it].tag == "productpage") is_meaningful = true;
	  if (e->ensighten.items[it].tag == "featured") is_meaningful = true;
	  
	}
      }
    }

    if (stats.find(browser) == stats.end()) {
      stats[browser] = {};
    }
    
    if (is_revjet) {
      history_filter.insert(i->first);
    }
    if (is_clicker) stats[browser]._clickers++;
    if (is_meaningful) stats[browser]._meaningful++;
    
    if (is_clicker && is_converter) {
      stats[browser].clickers_converters ++;
    }
    if (is_revjet && is_converter) {
      stats[browser].clickers_converters_alt++;
    }


    if (is_clicker && is_meaningful) {
      stats[browser].clickers_meaningful ++;
    }
    if (is_revjet && is_meaningful) {
      stats[browser].clickers_meaningful_alt++;
    }

    
  }


  for (auto b = stats.begin(); b != stats.end(); b++) {
    result << "----------- browser:" << b->first << "\n";
    result << "clickers:" << b->second._clickers << "\n";
    result << "meaningful:" << b->second._meaningful << "\n";
    result << "clickers_converters:" << b->second.clickers_converters << "\n";
    result << "clickers_converters_alt:" << b->second.clickers_converters_alt << "\n";
    result << "clickers_meaningful:" << b->second.clickers_meaningful << "\n";
    result << "clickers_meaningful_alt:" << b->second.clickers_meaningful_alt << "\n";
    
  }

  result << "history_filter.size()=" << history_filter.size() << "\n";
  
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}

/*
1201                    4) Product Page              6403          
1301                    5) Shopping Cart             616           
1305                    1) Home Page                 19449         
1306                    3) Category Page             8514          
1307                    2) Search Page               1203          
1310                    Site Event Tracking          6625600    
*/

/*
event.type = [4, "click"], in pixel # 79


1703155222749281285
1610272168073204652
4702268193757272683

 user has camsource but not the click

 */
