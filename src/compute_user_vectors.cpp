#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>
#include <boost/algorithm/string.hpp>
#include <iostream>


std::unordered_map<std::string, std::vector<float> > w2v;
int w2v_size = 0;


void load_word2vec(std::string filename) {
  std::ifstream file(filename);
  std::string line;
  int skip = 2;
  while (std::getline(file, line)) {
    if (skip > 0) {
      skip--;
      continue;
    }
    std::vector<std::string> strs;
    boost::split(strs, line, boost::is_any_of(" "));
    std::vector<float> vector;
    for (int i = 1; i < strs.size() - 1; i++) {
      vector.push_back(std::stof(strs[i]));
    }
    w2v[strs[0]] = vector;
    if (w2v_size == 0) w2v_size = vector.size();
    if (w2v_size != vector.size()) {
      std::cerr << "vector file malformed\n";
    }
  }
}


extern "C"
char * query() {
  load_word2vec("sku_vectors.csv");
  std::stringstream result;
  std::vector<float> user_value;
  user_value.resize(w2v_size);

  int limit = 1000;
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    //if (limit == 0) break;
    //limit --;
    std::fill(user_value.begin(), user_value.end(), 0.0);
    std::vector <std::string> skus;
    int n = 0;
    
    auto start =  i->second->history.begin()->second.ts;
    auto end =  i->second->history.rbegin()->second.ts;
    if (end - start < 1000) continue;
    bool is_converter = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events.size() != 1) continue;
      for (int e = 0; e < j->second.events.size(); e++) {
	auto event = j->second.events[e];
	if (event.ensighten.exists) {
	  bool is_product = event.ensighten.pageType == "PRODUCT";
	  for (int it = 0; it < event.ensighten.items.size(); it ++) {
	    if ((is_product) || (event.ensighten.items[it].tag == "productpage")) {
	      skus.push_back(event.ensighten.items[it].sku);
	      auto sku_vector = w2v.find(event.ensighten.items[it].sku);
	      //result << event.ensighten.items[it].sku << "\n";
	      if (sku_vector != w2v.end()) {
		for (int z = 0; z < w2v_size; z++) {
		  user_value[z] += sku_vector->second[z];
		}
		n++;
	      }
	    }
	    if ((is_product) || (event.ensighten.items[it].tag == "order")) {
	      is_converter = true;
	    }
	  } // items
	} // ensighten
      } // enents
    } // history
    if (n > 0) {
      result << i->first << "\t" << is_converter;
      for (int z = 0; z < w2v_size; z++) {
	result << "\t" << user_value[z] / n;
      }
      result << "\n";
    }

  } // user
  
  // end of custom code
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}



/*
int main(int argc, char ** argv) {
  load_word2vec("sku_vectors.csv");
  for (int i = 0; i < w2v.begin()->second.size(); i++) {
    std::cerr << w2v.begin()->second[i] << " ";
  }
  std::cerr << "\n";
  return 0;
}
*/
