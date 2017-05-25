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
#include <set>
#include "util.hpp"



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

struct hash_struct {
  long users; // done
  long converters; // done
  long exact_converters; // done
  long multicart_converters; // done
  long exact_multicart_converters; // done
  std::unordered_map<std::string, long> hash; // done
  std::unordered_map<std::string, long> exact_hash; // done
  std::unordered_map<std::string, long> multicart_hash; // done
  std::unordered_map<std::string, long> exact_multicart_hash; // done
  double order_total;
  double exact_order_total;
};

std::unordered_map<std::string, hash_struct> global_crumb_stats;


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
    std::set<std::string> last_cart;
    std::string last_cam_source;
    bool cam_source_changed = false;

    std::string tetris_string; 
    bool is_converter = false;
    int  cart_events  = 0;
    
    for (auto j = i->second->history.begin(); j != i->second->history.end(); j++) {
      if (j->second.events == NULL) continue;
      for (auto e = j->second.events->begin(); e != j->second.events->end(); e++) {
	// classify event type
	if (j == i->second->history.begin()) {
	  last_cam_source = e->ensighten.camSource;
	} else {
	  if (last_cam_source != e->ensighten.camSource) {
	    cam_source_changed = true;
	    last_cam_source = e->ensighten.camSource;
	  }
	}
	std::string event_type = "listing";
	if ((e->ensighten.searchTerm != "") || (e->ensighten.pageType == "SEARCH")) {
	  event_type = "search";
	} else if (e->ensighten.pageType == "HOMEPAGE") {
	  event_type = "homepage";
	} else  if (e->ensighten.pageType == "TAXONOMY") {
	  event_type = "taxonomy";
	}
	if (e->ensighten.items.size() > 0) {
	  if ((e->ensighten.items[0].tag == "featured") || (e->ensighten.items[0].tag == "productpage") || (e -> ensighten.pageType == "PRODUCT")) {
	    event_type = "productpage";
	  } else if (e->ensighten.items[0].tag == "order") {
	    event_type = "order";
	    is_converter = true;
	  } else if (e->ensighten.items[0].tag == "featured") {
	    event_type = "featured";
	  }
	  if (e->ensighten.items[0].tag == "cart") {
	    cart_events ++;
	    std::set<std::string> new_cart;
	    for (auto c = e->ensighten.items.begin(); c != e->ensighten.items.end(); c++) {
	      new_cart.insert(c->sku);
	    }
	    if (last_cart.size() == 0) {
	      event_type = "cart_first";
	    } else if (last_cart.size() > new_cart.size()) {
	      event_type = "cart_remove";
	    } else if (last_cart.size() < new_cart.size()) {
	      event_type = "cart_add";
	    } else {
	      event_type = "cart_view";
	      auto it_last = last_cart.begin();
	      for (auto it_new = new_cart.begin(); it_new != new_cart.end(); it_new++) {
		if (*it_last != *it_new) {
		  event_type = "cart_change";
		  break;
		}
		it_last++;
	      }
	    }
	    last_cart = new_cart;
	  }
	}
	// at this point event is classified
	if (tetris_string != "") tetris_string += "|";
	if (cam_source_changed) tetris_string += "cam_source_changed|";
	tetris_string += event_type;

	/* start/end matrix update */
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
      cam_source_changed = false;
      if (is_converter) break;
    } // history

    /* stats update */
    if (order_skus.size() > 0) {
      auto source_crumbs = get_crumbs_for_sku(first_sku);
      if (source_crumbs.size() > 0) {
	auto it = safe_find(sku_order_matrix, source_crumbs[0]);
	/* matrix update */
	std::unordered_set<std::string> order_categories;
	for (int si = 0; si < order_skus.size(); si++) {
	  auto s = &order_skus[si];
	  auto dest_crumbs = get_crumbs_for_sku(*s);
	  if (dest_crumbs.size() > 0) {
	    for (auto it3 = sku_order_matrix.begin(); it3 != sku_order_matrix.end(); it3++) {
	      auto it2 = safe_find(it3->second, dest_crumbs[0]);
	    }
	    std::string category_index = "";
	    for (auto crumb = dest_crumbs.begin(); crumb != dest_crumbs.end(); crumb++) {
	      if (category_index != "") category_index += "|";
	      category_index += *crumb;
	      order_categories.insert(category_index);
	    }
	    sku_order_matrix[source_crumbs[0]][dest_crumbs[0]].converters ++;
	    sku_order_matrix[source_crumbs[0]][dest_crumbs[0]].amount += order_dollars[si];
	  }
	}
	/* main stats update */
	std::string category_index = "";
	for (auto crumb = source_crumbs.begin(); crumb != source_crumbs.end(); crumb++) {
	  if (category_index != "") category_index += "|";
	  category_index += *crumb;
	  
	  auto it = safe_find(global_crumb_stats, category_index);
	  it->second.users ++;
	  
	  if (is_converter) {
	    it->second.converters ++;
	    safe_inc(it->second.hash, tetris_string);
	    bool exact_converter = false;
	    if (order_categories.find(category_index) != order_categories.end()) {
	      it->second.exact_converters ++;
	      safe_inc(it->second.exact_hash, tetris_string);
	      exact_converter = true;
	    }
	    if (cart_events > 1) {
	      it->second.multicart_converters++;
	      safe_inc(it->second.multicart_hash, tetris_string);
	      if (exact_converter) {
		it->second.exact_multicart_converters++;
		safe_inc(it->second.exact_multicart_hash, tetris_string);
	      }
	    }
	  }
	}
	
      }
    }
  } // json history

  
  std::ofstream file("flat_crumb_stats.csv");
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

  for (auto it = global_crumb_stats.begin(); it != global_crumb_stats.end(); it++) {
    file << it->first << "\t" << it->second.users << "\t" << it->second.converters << "\t";
    std::multimap<long, std::string> inverter;
    std::map<std::string, long> spectrum;

    for (auto it2 = it->second.exact_multicart_hash.begin(); it2 != it->second.exact_multicart_hash.end(); it2++) {
      inverter.insert(std::pair<long, std::string>(it2->second, it2->first));
      auto seq_parts = split_string(it2->first, "|");
      for (auto itp = seq_parts.begin(); itp != seq_parts.end(); itp++) {
	safe_inc(spectrum, *itp);
      }
    }

    std::string spectrum_out = "{";
    for (auto its = spectrum.begin(); its != spectrum.end(); its++) {
      if (spectrum_out != "{") spectrum_out += ",";
      spectrum_out += "\"" + its->first + "\":" + std::to_string(its->second);
    }
    spectrum_out += "}";
    int limit = 20;
    long coverage = 0;
    std::string json_out = "[";
    
    for (auto it2 = inverter.rbegin(); it2 != inverter.rend(); it2++) {
      if (coverage != 0) json_out += ",";
      json_out += "\"" + it2->second + "\",\"" + std::to_string(it2->first) + "\"";
      coverage += it2->first;
      limit --;
      if (limit == 0) break;
    }
    json_out += "]";
    file << coverage << "\t" << json_out << "\t" << it->second.order_total << "\t" << spectrum_out << "\t"
	 << it->second.exact_order_total << "\t" << it->second.exact_converters << "\n";

    
  }
  
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}
