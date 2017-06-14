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
#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"

extern std::string test_string;

std::vector<std::unordered_map<std::string, long> > crumb_stats;
std::unordered_set<std::string> crumbs;

unsigned long clickers = 0;
unsigned long clickers_converters = 0;

extern "C"
char * query() {
  history_filter.clear();
  
  std::stringstream result;
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    
    bool is_revjet = false;
    bool is_converter = false;
    bool is_clicker = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      auto pixel_string = replace_all(j->second.pixels, "'", "");
      rapidjson::Document d;
      d.Parse(pixel_string.c_str());
      if (d.HasParseError()) {
      } else {
	for (int i = 0; i < d.Size(); i++) {
	  if (d[i].GetInt() == 79) {
	    is_clicker = true;
	  }
	}
      }
      std::cerr << j->second.pixels << "\n";
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;

	if ((e->ensighten.camSource == "DglBrand") || (e->ensighten.camSource == "Digital Brand") ||
	    (e->ensighten.camSource == "RevJet Acq")) {
	  is_revjet = true;
	}
	if ((e->ensighten.camGroup == "DglBrand") || (e->ensighten.camGroup == "Digital Brand") ||
	    (e->ensighten.camGroup == "RevJet Acq")) {
	  is_revjet = true;
	}
      }
    }
    if (is_revjet) {
      history_filter.insert(i->first);
    }
    if (is_clicker) clickers++;
    if (is_clicker && is_converter) {
      clickers_converters ++;
    }
  }

  


  result << "clickers:" << clickers << "\n";
  result << "clickers_converters:" << clickers_converters << "\n";
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
