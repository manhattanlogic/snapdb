#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "util.hpp"
#include <stdio.h>
#include <iostream>
#include <fstream>

std::unordered_map<unsigned long, unsigned int> vid_map;


int main(int argc, char ** argv) {
  char buffer[1024 * 1204];
  while (fgets(buffer, 1024*1024, stdin)) {
    auto parts = split_string(buffer, "\t");
    if (parts.size() != 3) {
      continue;
    }
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
  for (auto it = vid_map.begin(); it != vid_map.end(); it++) {
    vid_map_file.write((char *)&(it->first), sizeof(unsigned long));
    vid_map_file.write((char *)&(it->second), sizeof(unsigned int));
  }
}
