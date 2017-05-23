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
#include <vector>

std::string replace_all(
			const std::string & str ,   // where to work
			const std::string & find ,  // substitute 'find'
			const std::string & replace //      by 'replace'
			) {
  using namespace std;
  string result;
  size_t find_len = find.size();
  size_t pos,from=0;
  while ( string::npos != ( pos=str.find(find,from) ) ) {
    result.append( str, from, pos-from );
    result.append( replace );
    from = pos + find_len;
  }
  result.append( str, from , string::npos );
  return result;
}

std::vector<std::string> split_string(std::string line, const char * sep = " ") {
  std::vector <std::string> result;
  char * pch = strtok ((char *)line.c_str(), sep);
  while (pch != NULL) {
    result.push_back(pch);
    pch = strtok (NULL, sep);
  }
  return result;
}


struct hash_struct {
  long users;
  long converters;
  long exact_converters;
  std::unordered_map<std::string, int> hash;
  std::unordered_map<std::string, int> exact_hash;
  double order_total;
  double exact_order_total;
};

std::unordered_map<std::string, std::vector<std::string> > sku_crumbs;

std::vector<std::string> get_crumbs_for_sku(std::string sku) {
  std::vector <std::string> zero;
  auto it = sku_crumbs.find(sku);
  if (it == sku_crumbs.end()) {
    return zero;
  } else {
    return it->second;
  }
}


extern "C"
char * query() {
  std::stringstream result;
  // index sku->crumbs
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (e->ensighten.items.size() > 0) {
	  if ((e->ensighten.items[0].tag == "featured") || (e->ensighten.items[0].tag == "productpage") || (e -> ensighten.pageType == "PRODUCT")) {
	    if (e->ensighten.crumbs.size() == 4 && (!(e->ensighten.crumbs[0] == "O.biz" || e->ensighten.crumbs[0] == "Eziba"))) {
	      std::vector <std::string> clean_crumbs;
	      for (auto s = e->ensighten.crumbs.begin(); s != e->ensighten.crumbs.end(); s++) {
		if ((*s == "O.biz") || (*s == "Eziba")) break;
		clean_crumbs.push_back(replace_all(*s, "&amp;", "&"));
	      }
	      if (clean_crumbs.size() == 4) sku_crumbs[e->ensighten.items[0].sku] = clean_crumbs;
	    }
	  }
	}
      }
    }
  }

  std::cerr << "done 0\n";

  struct matrix_entry {
    long converters = 0;
    double amount = 0.0;
  };
  
  std::map<std::string, std::map<std::string, matrix_entry> > sku_order_matrix;
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    std::string first_sku;
    std::vector <std::string> order_skus;
    std::vector <double> order_dollars;
    bool is_completed = false;
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	if (e->ensighten.items.size() > 0) {
	  if ((e->ensighten.items[0].tag == "featured") || (e->ensighten.items[0].tag == "productpage") || (e -> ensighten.pageType == "PRODUCT")) {
	    if (first_sku == "") {
	      first_sku = e->ensighten.items[0].sku;
	    }
	  } else if (e->ensighten.items[0].tag == "order") {
	    for (auto item = e->ensighten.items.begin(); item != e->ensighten.items.end(); item++) {
	      order_skus.push_back(item->sku);
	      order_dollars.push_back(item->price * item->quantity);
	    }
	  }
	}
      }
      if (order_skus.size() > 0) break;
    } // history

    

    
    if (order_skus.size() > 0) {
      auto source_crumbs = get_crumbs_for_sku(first_sku);
      if (source_crumbs.size() > 0) {
	auto it = sku_order_matrix.find(source_crumbs[0]);
	if (it == sku_order_matrix.end()) {
	  std::map<std::string, matrix_entry> element;
	  sku_order_matrix[source_crumbs[0]] = element;
	  it = sku_order_matrix.find(source_crumbs[0]);
	}
	for (int si = 0; si < order_skus.size(); si++) {
	  //for (auto s = order_skus.begin(); s != order_skus.end(); s++) {
	  auto s = &order_skus[si];
	  auto dest_crumbs = get_crumbs_for_sku(*s);
	  if (dest_crumbs.size() > 0) {
	    for (auto it3 = sku_order_matrix.begin(); it3 != sku_order_matrix.end(); it3++) {
	      auto it2 = it3->second.find(dest_crumbs[0]);
	      if (it2 == it3->second.end()) {
		matrix_entry me;
		it3->second[dest_crumbs[0]] = me;
	      }
	    }
	    sku_order_matrix[source_crumbs[0]][dest_crumbs[0]].converters ++;
	    sku_order_matrix[source_crumbs[0]][dest_crumbs[0]].amount += order_dollars[si];
	  }
	}
      }
    }
    
  } // json history

  

  std::ofstream matrix_file("order_matrix.csv");
  std::ofstream dollar_matrix_file("dollar_matrix.csv");
  bool first_line = true;
  for (auto i = sku_order_matrix.begin(); i != sku_order_matrix.end(); i++) {
    bool first_column = true;
    if (first_line) {
      for (auto j = i->second.begin(); j != i->second.end(); j++) {
	matrix_file << "\t" << j->first;
	dollar_matrix_file << "\t" << j->first;
      }
      matrix_file << "\n";
      dollar_matrix_file << "\n";
      first_line = false;
    }
    for (auto j = i->second.begin(); j != i->second.end(); j++) {
      if (first_column) {
	matrix_file << i->first;
	dollar_matrix_file << i->first;
	first_column = false;
      }
      matrix_file << "\t" << j->second.converters;
      dollar_matrix_file << "\t" << j->second.amount;
      
    }
    matrix_file << "\n";
    dollar_matrix_file << "\n";
  }
  
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
