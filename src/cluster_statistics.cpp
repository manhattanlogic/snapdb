#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>
#include <iostream>
#include <map>

std::unordered_map<unsigned long, int> clusters;

void load_cluster_data(std::string filename) {
  std::ifstream file(filename);
  std::string line;
  while (std::getline(file, line)) {
    char * comma = strchr((char *)line.c_str(), ',');
    if (comma == NULL) continue;
    unsigned long vid = strtoul (line.c_str(), NULL, 0);
    int cluster = strtoul (comma + 1, NULL, 0);
    clusters[vid] = cluster;
  }
}


struct u_stats {
  long users = 0;
  long converters = 0;
  long event_history_length = 0;
  long tempral_history_length = 0;
  long sku_observed = 0;
  long sku_purchased = 0;
  double dollars_spent = 0;
  double dollars_observed = 0;
  std::unordered_map<std::string, long> purchased_skus;
  std::unordered_map<std::string, long> observed_skus;
  long total_bought;
  std::unordered_map<std::string, long> crumbs;
};

/*
hitory length in events
history length in seconds
$$$ spend per user
sku price change within single history 
 */


extern "C"
char * query() {
  history_filter.clear();
  load_cluster_data("../tsne/clusters.csv");
  std::cerr << clusters.size() << " clustered users loaded\n";

  std::map<int, u_stats> cluster_totals;
  std::unordered_map<std::string, std::string> sku_names;
  
  for (auto c = clusters.begin(); c != clusters.end(); c++) {
    auto it = json_history.find(c->first);
    if (it == json_history.end()) continue;

    bool converter = false;

    double spending = 0;

    unsigned long last_order_time = 0;
    unsigned long last_order_treshold = 1000 * 60 * 60; // 1 hour

    std::unordered_set <std::string> purchased_skus;
    std::unordered_set <std::string> observed_skus;

    long total_bought = 0;

    std::vector<std::vector<std::string> > crumbs;
    
    for (auto h = it->second->history.begin(); h != it->second->history.end(); h++) {
      if (h->second.events == NULL) continue;
      for (auto e = h->second.events->begin(); e != h->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	crumbs.push_back(e->ensighten.crumbs);
	unsigned long next_last_order_time = 0;
	for (auto i = e->ensighten.items.begin(); i != e->ensighten.items.end(); i++) {
	  if (i->tag == "order") {
	    converter = true;
	    history_filter.insert(c->first);
	    if (!((purchased_skus.find(i->sku) != purchased_skus.end()) && (h->first - last_order_time < last_order_treshold))) {
	      purchased_skus.insert(i->sku);
	      next_last_order_time = h->first;
	      
	      spending += i->price * i->quantity;
	      total_bought += i->quantity;
	    }
	  } 
	  if ((e->ensighten.pageType == std::string("PRODUCT")) || (i->tag == "productpage")) {
	      observed_skus.insert(i->sku);
	  }
	}
	if (next_last_order_time != 0) {
	  last_order_time = next_last_order_time;
	}
      }
    }


    auto it2 = cluster_totals.find(c->second);
    if (it2 == cluster_totals.end()) {
      cluster_totals[c->second] = {};
      it2 = cluster_totals.find(c->second);
    } 
    

    for (auto s = observed_skus.begin(); s != observed_skus.end(); s++) {
      auto si = it2->second.observed_skus.find(*s);
      if (si == it2->second.observed_skus.end()) {
	it2->second.observed_skus[*s] = 1;
      } else {
	si->second++;
      }
    }

    for (auto s = purchased_skus.begin(); s != purchased_skus.end(); s++) {
      auto si = it2->second.purchased_skus.find(*s);
      if (si == it2->second.purchased_skus.end()) {
	it2->second.purchased_skus[*s] = 1;
      } else {
	si->second++;
      }
    }

    it2->second.total_bought += total_bought;
    it2->second.users++;
    it2->second.event_history_length   += it->second->history.size();
    it2->second.tempral_history_length += (it->second->history.rbegin()->first - it->second->history.begin()->first) / 1000 / 60 / 60;
    it2->second.sku_observed += observed_skus.size();
    it2->second.dollars_spent += spending; 
    if (converter) {
      it2->second.converters++;
      it2->second.sku_purchased += purchased_skus.size();
    }

    for (auto i = crumbs.begin(); i != crumbs.end(); i++) {
      for (auto j = i->begin(); j != i->end(); j++) {
	auto it = it2->second.crumbs.find(*j);
	if (it == it2->second.crumbs.end()) {
	  it2->second.crumbs[*j] = 1;
	} else {
	  it->second ++;
	}
      }
    }
    
  }

  std::stringstream str_result;

  for (auto it3 = cluster_totals.begin(); it3 != cluster_totals.end(); it3++) {
    str_result << it3->first << "," << it3->second.users << "," << it3->second.converters << "," <<
      it3->second.event_history_length << "," << it3->second.tempral_history_length << "," <<
      it3->second.sku_observed  << "," << it3->second.sku_purchased << "," <<
      it3->second.observed_skus.size()  << "," << it3->second.purchased_skus.size() << "," <<
      it3->second.dollars_spent << "," << it3->second.total_bought <<"\n";
  }

  /*
  for (auto it3 = cluster_totals.begin(); it3 != cluster_totals.end(); it3++) {
    std::multimap<long, std::string> _observed_skus;
    for (auto it = it3->second.observed_skus.begin(); it != it3->second.observed_skus.end(); it++) {
      _observed_skus.insert(std::pair<long, std::string>(it->second, it->first));
    }
    int limit = 10;
    for (auto it = _observed_skus.rbegin(); it != _observed_skus.rend(); it++) {
      str_result << it3->first << "," << it->second << "," << it->first << "\n";
      limit --;
      if (limit == 0) {
	break;
      }
    }
  }
  */

  
  for (auto it3 = cluster_totals.begin(); it3 != cluster_totals.end(); it3++) {
    std::multimap<long, std::string> _observed_skus;
    for (auto it = it3->second.crumbs.begin(); it != it3->second.crumbs.end(); it++) {
      _observed_skus.insert(std::pair<long, std::string>(it->second, it->first));
    }
    int limit = 5;
    for (auto it = _observed_skus.rbegin(); it != _observed_skus.rend(); it++) {
      str_result << it3->first << "," << it->second << "," << it->first << "\n";
      limit --;
      if (limit == 0) {
	break;
      }
    }
  }
  
  
  


  

  char * buffer = (char *)malloc(str_result.str().size() + 1);
  memcpy(buffer, str_result.str().c_str(), str_result.str().size());
  buffer[str_result.str().size()] = 0;
  return buffer;

  
}

int main(int argc, char ** argv) {
  query();
  return 0;
}
