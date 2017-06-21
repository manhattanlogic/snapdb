#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "util.hpp"
#include <stdio.h>
#include <iostream>
#include <fstream>

std::unordered_map<unsigned long, std::vector<unsigned long> > vid_map;

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

int _main(int argc, char ** argv) {
  struct vid_record {
    unsigned long vid;
    unsigned long users;
    unsigned long crossusers;
  };
  std::cerr << "sizeof(vid_record):" << sizeof(vid_record) << "\n";
  auto l = get_filesize("vid_map_dual.dat");
  vid_record * data = (vid_record *)malloc(l);
  FILE * f = fopen("vid_map_dual.dat","rb");
  auto j = fread(data, l / sizeof(vid_record), sizeof(vid_record), f);
  fclose(f);
  std::cerr << j << " loaded\n";

  unsigned long users = 0;
  unsigned long crossusers = 0;
  
  unsigned long imps_users = 0;
  unsigned long imps_crossusers = 0;
  
  for (long i = 0; i < l / sizeof(vid_record); i++) {
    //std::cerr << data[i].vid << " " << data[i].users << " " << data[i].crossusers << "\n";
    if (data[i].users > 0) {
      users++;
    }
    if (data[i].crossusers > 0) {
      crossusers++;
    }
    imps_users += data[i].users;
    imps_crossusers += data[i].crossusers;
  }
  std::cerr << "users: " << users << "\ncrossusers: " << crossusers << "\n";
  std::cerr << "imps_users: " << imps_users << "\nimps_crossusers: " << imps_crossusers << "\n";
}


int __main(int argc, char ** argv) {
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

  long hit = 0;
  long miss = 0;
  
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
	std::vector<unsigned long> datum = {0,0};
	vid_map[vid] = datum;
	it = vid_map.find(vid);
      }

      it->second[0]++;
      if (vidmap.find(vid) != vidmap.end()) {
	it->second[1]++;
      }
   
    }
  }
  std::ofstream vid_map_file("vid_map_dual.dat", std::ios::binary);
  for (auto it = vid_map.begin(); it != vid_map.end(); it++) {
    vid_map_file.write((char *)&(it->first), sizeof(unsigned long));
    vid_map_file.write((char *)&(it->second[0]), sizeof(unsigned long));
    vid_map_file.write((char *)&(it->second[1]), sizeof(unsigned long));
  }
}


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
      if (d.HasMember("events") && d["events"].IsArray()) {
	std::string full_string;
	for (int i = 0; i < d["events"].Size(); i++) {
	  std::string e = "";
	  std::string os = "x";
	  std::string device = "x";
	  std::string channel = "x";
	  std::vector<std::string> tags;
	  if (d["events"][i].HasMember("ua") && d["events"][i]["ua"].IsObject()) {
	    os = d["events"][i]["ua"]["_os"].GetString();
	    device = d["events"][i]["ua"]["_device_type"].GetString();
	    if (d["events"][i].HasMember("subids") && d["events"][i]["subids"].IsObject()) {
	      channel = d["events"][i]["subids"]["_device_channel_type"].GetString();
	    }
	    e = "[" + os + " " + device + " : " + channel + "] ";
	  } else {
	    e = " - ";
	  }

	  std::string g = "";
	  if (d["events"][i].HasMember("plc") && d["events"][i]["plc"].IsArray()) {
	    for (int j = 0; j < d["events"][i]["plc"].Size(); j++) {
	      std::string tag = d["events"][i]["plc"][j][1].GetString();
	      tags.push_back(tag);
	      if (j > 0) g+= ",";
	      g += tag;
	    }
	  } else {
	    g = "-";
	  }
	  full_string += e + g + " \t ";
	}
	std::cerr << vid << "\t" << full_string << "\n";
      } else {
	std::cerr << vid  << "\t" << "----------------------------\n";
      }
    }
  }
}
