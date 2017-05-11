#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>
#include <iostream>

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
};

extern "C"
char * query() {
  load_cluster_data("../tsne/clusters.csv");
  std::cerr << clusters.size() << " clustered users loaded\n";

  std::map<int, u_stats> cluster_totals;

  for (auto c = clusters.begin(); c != clusters.end(); c++) {
    auto it = json_history.find(c->first);
    if (it == json_history.end()) continue;

    bool converter = false;
    
    for (auto h = it->second->history.begin(); h != it->second->history.end(); h++) {
      if (h->second.events == NULL) continue;
      for (auto e = h->second.events->begin(); e != h->second.events->end(); e++) {
	if (!(e->ensighten.exists)) continue;
	
	for (auto i = e->ensighten.items.begin(); i != e->ensighten.items.end(); i++) {
	  if (i->tag == "order") {
	    converter = true;
	  }
	}
	
      }
    }

    auto it2 = cluster_totals.find(c->second);
    if (it2 == cluster_totals.end()) {
      cluster_totals[c->second] = {};
      it2 = cluster_totals.find(c->second);
    } 
    it2->second.users++;
    if (converter) {
      it2->second.converters++;
    }
  }

  std::stringstream str_result;

  for (auto it3 = cluster_totals.begin(); it3 != cluster_totals.end(); it3++) {
    str_result << it3->first << "\t" << it3->second.users << "\t" << it3->second.converters << "\n";
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
