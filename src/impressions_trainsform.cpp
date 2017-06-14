#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "util.hpp"
#include <stdio.h>
#include <iostream>
#include <fstream>

std::unordered_map<unsigned long, unsigned int> vid_map;

std::vector<int> get_pixels(std::string json) {
  std::vector<int> result;
  rapidjson::Document d;
  d.Parse(json.c_str());
  std::cerr << json << " ";
  if (d.HasParseError()) {
    std::cerr << "o";
  } else {
    std::cerr << ".";
  }
  std::cerr << "\n";
  return result;
}

int main(int argc, char ** argv) {
  char buffer[1024 * 1204];
  while (fgets(buffer, 1024*1024, stdin)) {
    auto parts = split_string(buffer, "\t");
    if (parts.size() != 3) {
      continue;
    }

    auto pixels = get_pixels(parts[1]);
    

    
    auto json = replace_all(parts[2], "\\'","'");
    json = replace_all(json, "\\\\","\\");
    rapidjson::Document d;
    d.Parse(json.c_str());
    if (d.HasParseError()) {
      std::cerr << "o";
    } else {
      auto vid = d["vid"].GetUint64();
      auto it = vid_map.find(vid);
      if (it == vid_map.end()) {
	vid_map[vid] = 1;
      } else {
	it->second++;
      }
    }
  }
  std::ofstream vid_map_file("vid_map.dat", std::ios::binary);
  std::map<unsigned int, unsigned int> imp_distribution;
  for (auto it = vid_map.begin(); it != vid_map.end(); it++) {
    vid_map_file.write((char *)&(it->first), sizeof(unsigned long));
    vid_map_file.write((char *)&(it->second), sizeof(unsigned int));
    auto it2 = imp_distribution.find(it->second);
    if (it2 == imp_distribution.end()) {
      imp_distribution[it->second] = 1;
    } else {
      it2->second++;
    }
  }
  for (auto it = imp_distribution.begin(); it != imp_distribution.end(); it++) {
    std::cout << it->first << "\t" << it->second << "\n";
  }
}
