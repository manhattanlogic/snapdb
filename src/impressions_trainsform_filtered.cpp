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
  d.Parse(replace_all(json, "'", "").c_str());
  if (d.HasParseError()) {
    std::cerr << "o";
    return result;
  } else {
    for (int i = 0; i < d.Size(); i++) {
      result.push_back(d[i].GetInt());
    }
  }
  return result;
}

struct impression_data {
  std::string crv;
  std::string og;
};

int main(int argc, char ** argv) {
  auto l = get_filesize("long_vids.dat");
  if (l <= 0) {
    std::cerr << "no vid file provided\n";
    return(0);
  }
  unsigned long * vids = (unsigned long *)malloc(l);
  FILE * f = fopen("long_vids.dat","rb");
  fread(vids, l / sizeof(unsigned long), sizeof(unsigned long), f);
  fclose(f);

  std::unordered_map<unsigned long, std::map<unsigned long, impression_data> > vidmap;
  for (int i = 0; i < l / sizeof(long); i++) {
    std::map<unsigned long, impression_data> datum;
    vidmap[vids[i]] = datum;
  }

  std::cerr << vidmap.size() << " vids loaded\n";
  
  
  std::unordered_map<int, unsigned long> pixel_stats;
  char buffer[1024 * 1204];

  long count = 0;
  
  while (fgets(buffer, 1024*1024, stdin)) {
    auto parts = split_string(buffer, "\t");
    if (parts.size() != 3) {
      continue;
    }

    auto pixels = get_pixels(parts[1]);
    for (auto i = 0; i < pixels.size(); i++) {
      auto it = pixel_stats.find(pixels[i]);
      if (it == pixel_stats.end()) {
	pixel_stats[pixels[i]] = 1;
      } else {
	it->second++;
      }
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
      if (vidmap.find(vid) != vidmap.end()) {
	std::cout << buffer;
	count ++;
      }
    }
  }
}
