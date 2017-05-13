#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>
#include <iostream>
#include <algorithm>
#include <string>


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
char * query() {
  std::unordered_map<std::string, std::vector<float> > * w2v = new std::unordered_map<std::string, std::vector<float> >;
  std::stringstream str_result;
  str_result << "done";
  
  std::ofstream d_result("w2v_crumb_result_d.csv");
  std::ofstream m_result("w2v_crumb_result_m.csv");
  std::ofstream t_result("w2v_crumb_result_t.csv");
  std::ofstream o_result("w2v_crumb_result_o.csv");
  
  std::cerr << "query started\n";

  std::string filename = "crumb_vectors.csv";
  
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
      std::string browser = "";
      std::fill(user_value.begin(), user_value.end(), 0.0);
      for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
	if (j->second.events->size() == 0) continue;
	auto event = (*(j->second.events))[j->second.events->size() - 1];
	if (event.ensighten.exists) {
	  browser = event.ensighten.browser;
	  auto crumbs = event.ensighten.crumbs;
	  for (auto _crumb = crumbs.begin(); _crumb != crumbs.end(); _crumb++) {
	    auto crumb = *_crumb;
	    std::replace(crumb.begin(), crumb.end(), ' ', '_');
	    std::replace(crumb.begin(), crumb.end(), '\t', '_');
	    std::replace(crumb.begin(), crumb.end(), '\n', '_');
	    auto it = w2v->find(crumb);
	    if (it != w2v->end()) {
	      n++;
	      for (int k = 0; k < w2v_size; k++) {
		user_value[k] += it->second[k];
	      }
	    }
	  }
	  for (int it = 0; it < event.ensighten.items.size(); it ++) {
	    if (event.ensighten.items[it].tag == "order") {
	      is_converter = true;
	    }
	  }
	}
	if (is_converter) break;
      } //user

      auto result = &o_result;
      
      if (browser == "m") {
	result = &m_result;
      } else if (browser == "t") {
	result = &t_result;
      } else if (browser=="d") {
	result = &d_result;
      }
      if (n > 0) {
	(*result) << i->first << "\t" << is_converter;
	for (int z = 0; z < w2v_size; z++) {
	  (*result) << "\t" << user_value[z] / n;
	}
	(*result) << "\n";
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

