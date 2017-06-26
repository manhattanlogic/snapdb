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

  unsigned long file_size = get_filesize("vid_map.dat");
  std::cerr << "file_size:" << file_size << "\n";
  void * data = malloc(file_size);
  std::cerr << "data:" << (unsigned long)data << "\n";
  FILE * f = fopen("vid_map.dat", "rb");
  auto X = fread(data, file_size, 1, f);
  std::cerr << "X:" << X << "\n";
  
  std::cerr << "sizeof(unsigned long):" << sizeof(unsigned long) << "\n";
  std::cerr << "sizeof(unsigned int):" << sizeof(unsigned int) << "\n";
  fclose(f);

  std::unordered_map<unsigned long, unsigned int> impressions;
  for (unsigned long i = 0; i < (file_size / (sizeof(unsigned long) + sizeof(unsigned int))); i++) {
    unsigned long idx = i * (sizeof(unsigned long) + sizeof(unsigned int));
    unsigned long vid = *(unsigned long *)((char *)data + idx);
    unsigned int len = *(unsigned int *)((char *)data + idx + sizeof(unsigned long));
    impressions[vid] = len;
  }
  
  std::cerr << "impressions loaded\n";


  
  struct stats_struct {
    unsigned long converters = 0;
    unsigned long converters_rj = 0;
    unsigned long converters_rj_vid = 0;
    unsigned long converters_rj_alt = 0;
    unsigned long converters_rj_alt_vid = 0;
    unsigned long _meaningful = 0;
    unsigned long _clickers = 0;
    unsigned long _clickers_vid = 0;
    unsigned long clickers_converters = 0;
    unsigned long clickers_converters_vid = 0;
    unsigned long clickers_converters_alt = 0;
    unsigned long clickers_converters_alt_vid = 0;
    unsigned long clickers_meaningful = 0;
    unsigned long clickers_meaningful_vid = 0;
    unsigned long clickers_meaningful_alt = 0;
    unsigned long clickers_meaningful_alt_vid = 0;
    std::unordered_set<unsigned long> alt_clickers;
    std::unordered_set<unsigned long> alt_clickers_vid;
  };

  std::map<std::string, stats_struct> stats;
  stats["all"] = {};
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
	
	if ((e->ensighten.camSource == std::string("DglBrand")) || (e->ensighten.camSource == std::string("Digital Brand")) ||
	    (e->ensighten.camSource == std::string("RevJet Acq"))) {
	  is_revjet = true;
	}
	if ((e->ensighten.camGroup == std::string("DglBrand")) || (e->ensighten.camGroup == std::string("Digital Brand")) ||
	    (e->ensighten.camGroup == std::string("RevJet Acq"))) {
	  is_revjet = true;
	}

	if (e->ensighten.pageType == std::string("PRODUCT")) {
	  is_meaningful = true;
	}
	
	for (int it = 0; it < e->ensighten.items.size(); it ++) {
	  if (e->ensighten.items[it].tag == "order") is_converter = true;
	  if (e->ensighten.items[it].tag == "productpage") is_meaningful = true;
	  if (e->ensighten.items[it].tag == "featured") is_meaningful = true; 
	}
      }
    }


    std::vector<std::string> tmp = {"all", browser}; 

    for (auto b = tmp.begin(); b != tmp.end(); b++) {
      std::string browser = *b;
      if (stats.find(browser) == stats.end()) {
	stats[browser] = {};
      }
      if (is_revjet) {
	stats[browser].alt_clickers.insert(i->first);
      }
      if (is_clicker) stats[browser]._clickers++;
      if (is_meaningful) stats[browser]._meaningful++;
    
      if (is_clicker && is_converter) {
	stats[browser].clickers_converters ++;
	history_filter.insert(i->first);
      }
      if (is_revjet && is_converter) {
	stats[browser].clickers_converters_alt++;
	history_filter.insert(i->first);
      }

      if (is_converter) {
	stats[browser].converters ++;
      }
    
      if (is_clicker && is_meaningful) {
	stats[browser].clickers_meaningful ++;
      }
      if (is_revjet && is_meaningful) {
	stats[browser].clickers_meaningful_alt++;
      }
    }

    // VID updates
    if (impressions.find(i->first) != impressions.end()) {
      for (auto b = tmp.begin(); b != tmp.end(); b++) {
	std::string browser = *b;
	if (is_revjet) {
	  stats[browser].alt_clickers_vid.insert(i->first);
	}
	if (is_clicker) stats[browser]._clickers_vid++;
	
    
	if (is_clicker && is_converter) {
	  stats[browser].clickers_converters_vid++;
	}
	if (is_revjet && is_converter) {
	  stats[browser].clickers_converters_alt_vid++;
	}
    
	if (is_clicker && is_meaningful) {
	  stats[browser].clickers_meaningful_vid ++;
	}
	if (is_revjet && is_meaningful) {
	  stats[browser].clickers_meaningful_alt_vid++;
	}
      }
    }

    
  }




  

  for (auto b = stats.begin(); b != stats.end(); b++) {
    result << "----------- browser:" << b->first << "\n";
    result << "clickers:" << b->second._clickers << "\n";
    result << "clickers_vid:" << b->second._clickers_vid << "\n";
    result << "clickers_alt:" << b->second.alt_clickers.size() << "\n";
    result << "clickers_alt_vid:" << b->second.alt_clickers_vid.size() << "\n";
    result << "meaningful:" << b->second._meaningful << "\n";
    result << "clickers_converters:" << b->second.clickers_converters << "\n";
    result << "clickers_converters_vid:" << b->second.clickers_converters_vid << "\n";
    result << "clickers_converters_alt:" << b->second.clickers_converters_alt << "\n";
    result << "clickers_converters_alt_vid:" << b->second.clickers_converters_alt_vid << "\n";
    result << "clickers_meaningful:" << b->second.clickers_meaningful << "\n";
    result << "clickers_meaningful_vid:" << b->second.clickers_meaningful_vid << "\n";
    result << "clickers_meaningful_alt:" << b->second.clickers_meaningful_alt << "\n";
    result << "clickers_meaningful_alt_vid:" << b->second.clickers_meaningful_alt_vid << "\n";
    
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


/*

LONG HISTORY STATS

----------- browser:
clickers:0
clickers_alt:0
meaningful:0
clickers_converters:0
clickers_converters_alt:0
clickers_meaningful:0
clickers_meaningful_alt:0
----------- browser:all
clickers:63505
clickers_alt:63059
meaningful:4451918
clickers_converters:615
clickers_converters_alt:429
clickers_meaningful:19325
clickers_meaningful_alt:17153
----------- browser:d
clickers:6524
clickers_alt:10320
meaningful:2681712
clickers_converters:195
clickers_converters_alt:239
clickers_meaningful:4045
clickers_meaningful_alt:5892
----------- browser:m
clickers:820
clickers_alt:1701
meaningful:1308233
clickers_converters:22
clickers_converters_alt:16
clickers_meaningful:545
clickers_meaningful_alt:1125
----------- browser:t
clickers:56161
clickers_alt:51038
meaningful:461973
clickers_converters:398
clickers_converters_alt:174
clickers_meaningful:14735
clickers_meaningful_alt:10136
history_filter.size()=770


SHORT HISTORY STATS

----------- browser:
clickers:11
clickers_alt:0
meaningful:0
clickers_converters:0
clickers_converters_alt:0
clickers_meaningful:0
clickers_meaningful_alt:0
----------- browser:all
clickers:65950
clickers_alt:120806
meaningful:22395023
clickers_converters:6
clickers_converters_alt:37
clickers_meaningful:2175
clickers_meaningful_alt:14655
----------- browser:d
clickers:5491
clickers_alt:31693
meaningful:7313297
clickers_converters:0
clickers_converters_alt:13
clickers_meaningful:359
clickers_meaningful_alt:5177
----------- browser:m
clickers:702
clickers_alt:10948
meaningful:9428582
clickers_converters:0
clickers_converters_alt:14
clickers_meaningful:63
clickers_meaningful_alt:4559
----------- browser:t
clickers:59746
clickers_alt:78165
meaningful:5653144
clickers_converters:6
clickers_converters_alt:10
clickers_meaningful:1753
clickers_meaningful_alt:4919
history_filter.size()=42






=================
LONG


----------- browser:
clickers:0
clickers_vid:0
clickers_alt:0
clickers_alt_vid:0
meaningful:0
clickers_converters:0
clickers_converters_vid:0
clickers_converters_alt:0
clickers_converters_alt_vid:0
clickers_meaningful:0
clickers_meaningful_vid:0
clickers_meaningful_alt:0
clickers_meaningful_alt_vid:0
----------- browser:all
clickers:63505
clickers_vid:56547
clickers_alt:63059
clickers_alt_vid:54937
meaningful:4451918
clickers_converters:615
clickers_converters_vid:307
clickers_converters_alt:429
clickers_converters_alt_vid:250
clickers_meaningful:19325
clickers_meaningful_vid:13605
clickers_meaningful_alt:17153
clickers_meaningful_alt_vid:12128
----------- browser:d
clickers:6524
clickers_vid:5037
clickers_alt:10320
clickers_alt_vid:5377
meaningful:2681712
clickers_converters:195
clickers_converters_vid:114
clickers_converters_alt:239
clickers_converters_alt_vid:113
clickers_meaningful:4045
clickers_meaningful_vid:2757
clickers_meaningful_alt:5892
clickers_meaningful_alt_vid:2887
----------- browser:m
clickers:820
clickers_vid:537
clickers_alt:1701
clickers_alt_vid:653
meaningful:1308233
clickers_converters:22
clickers_converters_vid:7
clickers_converters_alt:16
clickers_converters_alt_vid:7
clickers_meaningful:545
clickers_meaningful_vid:324
clickers_meaningful_alt:1125
clickers_meaningful_alt_vid:404
----------- browser:t
clickers:56161
clickers_vid:50973
clickers_alt:51038
clickers_alt_vid:48907
meaningful:461973
clickers_converters:398
clickers_converters_vid:186
clickers_converters_alt:174
clickers_converters_alt_vid:130
clickers_meaningful:14735
clickers_meaningful_vid:10524
clickers_meaningful_alt:10136
clickers_meaningful_alt_vid:8837
history_filter.size()=770


SHORT

----------- browser:all
clickers:65950
clickers_vid:63463
clickers_alt:120806
clickers_alt_vid:63265
meaningful:22395023
clickers_converters:6
clickers_converters_vid:1
clickers_converters_alt:37
clickers_converters_alt_vid:0
clickers_meaningful:2175
clickers_meaningful_vid:1213
clickers_meaningful_alt:14655
clickers_meaningful_alt_vid:495
----------- browser:d
clickers:5491
clickers_vid:5196
clickers_alt:31693
clickers_alt_vid:5967
meaningful:7313297
clickers_converters:0
clickers_converters_vid:0
clickers_converters_alt:13
clickers_converters_alt_vid:0
clickers_meaningful:359
clickers_meaningful_vid:184
clickers_meaningful_alt:5177
clickers_meaningful_alt_vid:104
----------- browser:m
clickers:702
clickers_vid:577
clickers_alt:10948
clickers_alt_vid:601
meaningful:9428582
clickers_converters:0
clickers_converters_vid:0
clickers_converters_alt:14
clickers_converters_alt_vid:0
clickers_meaningful:63
clickers_meaningful_vid:14
clickers_meaningful_alt:4559
clickers_meaningful_alt_vid:14
----------- browser:t
clickers:59746
clickers_vid:57682
clickers_alt:78165
clickers_alt_vid:56697
meaningful:5653144
clickers_converters:6
clickers_converters_vid:1
clickers_converters_alt:10
clickers_converters_alt_vid:0
clickers_meaningful:1753
clickers_meaningful_vid:1015
clickers_meaningful_alt:4919
clickers_meaningful_alt_vid:377
history_filter.size()=42


*/
