#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>
#include <iostream>






std::vector<std::string> split_string(std::string line) {
  std::vector <std::string> result;
  char * pch = strtok ((char *)line.c_str()," ");
  while (pch != NULL) {
    result.push_back(pch);
    pch = strtok (NULL, " ");
  }
  return result;
}


extern "C"
char * query_x() {
  std::unordered_map<std::string, std::vector<float> > * w2v = new std::unordered_map<std::string, std::vector<float> >;
  std::stringstream str_result;
  str_result << "done";
  std::ofstream result("w2v_result.csv");
  
  
  std::cerr << "query started\n";

  std::string filename = "sku_vectors.csv";
  
  if (true) {

    int w2v_size = 0;
    std::ifstream file(filename);
    std::string line;
    int skip = 2;
    while (std::getline(file, line)) {
      if (skip > 0) {
	skip--;
	continue;
      }
      std::vector<std::string> strs = split_string(line);
      std::vector<float> vector;
      for (int i = 1; i < strs.size(); i++) {
	vector.push_back(std::stof(strs[i]));
      }
      (*w2v)[strs[0]] = vector;
      if (w2v_size == 0) w2v_size = vector.size();
      if (w2v_size != vector.size()) {
	std::cerr << "vector file malformed\n";
      }
    }

    std::cerr << w2v->size() << " vectors of " << w2v_size <<  " loaded\n";

    std::vector<float> user_value;
    user_value.resize(w2v_size);
    for (auto i = json_history.begin(); i != json_history.end(); i++) {
      bool is_converter = false;
      int n = 0;
      std::fill(user_value.begin(), user_value.end(), 0.0);
      for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
	if (j->second.events->size() == 0) continue;
	auto event = (*(j->second.events))[j->second.events->size() - 1];
	if (event.ensighten.exists) {
	  bool is_product = event.ensighten.pageType == "PRODUCT";
	  for (int it = 0; it < event.ensighten.items.size(); it ++) {
	    if (event.ensighten.items[it].tag == "order") {
	      is_converter = true;
	    }
	    if ((is_product) || (event.ensighten.items[it].tag == "productpage")) {
	      auto sku_vector = w2v->find(event.ensighten.items[it].sku);
	      if (sku_vector != w2v->end()) {
		for (int z = 0; z < w2v_size; z++) {
		  user_value[z] += sku_vector->second[z];
		}
		n++;
	      }
	    }
	  }
	}
	if (is_converter) break;
      } //user
      if (n > 0) {
	result << i->first << "\t" << is_converter;
	for (int z = 0; z < w2v_size; z++) {
	  result << "\t" << user_value[z] / n;
	}
	result << "\n";
      }
    } // history
  }

  delete w2v;

  std::cerr << "result size: " << str_result.str().size() << "\n";
  
  // end of custom code
  char * buffer = (char *)malloc(str_result.str().size() + 1);
  memcpy(buffer, str_result.str().c_str(), str_result.str().size());
  buffer[str_result.str().size()] = 0;
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
