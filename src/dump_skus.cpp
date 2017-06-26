#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <iostream>


unsigned long total_length = 0;
unsigned long total_users = 0;

unsigned long min_history = 10000000;
unsigned long max_history = 0;
unsigned long max_user = 0;

#define SANITY_LENGTH 100

extern "C"
char * query() {
  std::stringstream result;

  unsigned long converters = 0;
  unsigned long users = 0;
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    bool is_converter = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events->size() != 1) continue;
      for (int e = 0; e < j->second.events->size(); e++) {
	auto event = (*j->second.events)[e];
	if (event.ensighten.exists) {
	  bool is_product = event.ensighten.pageType == std::string("PRODUCT");
	  for (int it = 0; it < event.ensighten.items.size(); it ++) {
	    if ((is_product) || (event.ensighten.items[it].tag == "productpage")) {
	      //skus.push_back(event.ensighten.items[it].sku);
	    } else if ( (it == 0) && (event.ensighten.items[it].tag  == "cart") ) {
	      //skus.push_back("cart");
	    } else if (event.ensighten.items[it].tag  == "order") {
	      is_converter = true;
	    }
	  } // items
	} // ensighten
      } // enents
    } // history
    if (is_converter) converters++;
    users++;
    /*
    if (skus.size() < 3) continue;
    for (int j = 0; j < skus.size(); j++) {
      if (j > 0) result << " ";
      result << skus[j];
    }
    result << "\n";
    */
  } // user

  result << "users:" << users << ",converters:" << converters << "\n";
  
  // end of custom code
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}

