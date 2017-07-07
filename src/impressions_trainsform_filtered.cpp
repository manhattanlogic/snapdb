#include "rapidjson/include/rapidjson/document.h"
#include "rapidjson/include/rapidjson/prettywriter.h"
#include "rapidjson/include/rapidjson/writer.h"
#include "rapidjson/include/rapidjson/stringbuffer.h"
#include "util.hpp"
#include <stdio.h>
#include <iostream>
#include <fstream>
#include <unordered_set>
#include "date/tz.h"

date::sys_time<std::chrono::milliseconds>
parse8601(std::istream&& is)
{
  // call .time_since_epoch().count() on the result in order to get a UTC unix timestamp in ms 
  std::string save;
  is >> save;
  std::istringstream in{save};
  date::sys_time<std::chrono::milliseconds> tp;
  in >> date::parse("%FT%TZ", tp);
  if (in.fail())
    {
      in.clear();
      in.exceptions(std::ios::failbit);
      in.str(save);
      in >> date::parse("%FT%T%Ez", tp);
    }
  return tp;
}


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
    auto parts = basic_split_string(buffer, "\t");
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
  // this is the "impressions_compact.csv" file producer. Add Geo info here
  // cat /media/disk1/revjet_impressions/* | gunzip | ./impressions_trainsform_filtered > impressions_compact.csv
  char buffer[1024 * 1204];
  
  std::unordered_set<std::string> devices;

  struct line_struct {
    unsigned long vid;
    unsigned long ts;
    std::string os;
    std::string device;
    std::string channel;
    std::vector<std::string> tags;
    std::string country = "NONE";
    std::string state = "NONE";
    std::string city = "NONE";
    std::string metro = "NONE";
  };
  
  while (fgets(buffer, 1024*1024, stdin)) {
    auto parts = basic_split_string(buffer, "\t");
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
      line_struct line = {};
      
      auto vid = d["vid"].GetUint64();
      if (d.HasMember("events") && d["events"].IsArray()) {
	std::string full_string;
	for (int i = 0; i < d["events"].Size(); i++) {
	  std::string e = "";
	  std::string os = "x";
	  std::string device = "x";
	  std::string channel = "x";
	  std::vector<std::string> tags;
	  unsigned long ts;
	  if (d["events"][i].HasMember("ua") && d["events"][i]["ua"].IsObject()) {
	    struct tm tm = {};
	    auto str_ts = d["events"][i]["ts"].GetString();


	    ts = parse8601(std::istringstream{str_ts}).time_since_epoch().count() / 1000;
	    
	    // strptime(str_ts, "%Y-%m-%dT%H:%M:%S", &tm);
	    // ts = mktime(&tm);

	    os = d["events"][i]["ua"]["_os"].GetString();
	    device = d["events"][i]["ua"]["_device_type"].GetString();
	    devices.insert(device);
	    if (d["events"][i].HasMember("subids") && d["events"][i]["subids"].IsObject()) {
	      channel = d["events"][i]["subids"]["_device_channel_type"].GetString();
	    }
	    e = "[" + os + " " + device + " : " + channel + "] ";
	  } else {
	    e = " - ";
	  }


	  if (d["events"][i].HasMember("geo") && d["events"][i]["geo"].IsObject()) {
	    if (d["events"][i]["geo"].HasMember("country") && d["events"][i]["geo"]["country"].IsString()) {
	      line.country = d["events"][i]["geo"]["country"].GetString();
	    }
	    if (d["events"][i]["geo"].HasMember("state") && d["events"][i]["geo"]["state"].IsString()) {
	      line.state = d["events"][i]["geo"]["state"].GetString();
	    }
	    if (d["events"][i]["geo"].HasMember("city") && d["events"][i]["geo"]["city"].IsString()) {
	      line.city = d["events"][i]["geo"]["city"].GetString();
	    }
	    if (d["events"][i]["geo"].HasMember("metro") && d["events"][i]["geo"]["metro"].IsString()) {
	      line.metro = d["events"][i]["geo"]["metro"].GetString();
	    }
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
	
	  if (i == 0) {
	    line.vid = vid;
	    line.os = os;
	    line.device = device;
	    line.channel = channel;
	  } else if (i == 1) {
	    line.tags = tags;
	    line.ts = ts;
	  }
	}
	std::cout << line.vid << "\t" << line.ts << "\t" << line.os << "\t" << line.device << "\t" << line.channel << "\t";
	for (int j = 0; j < line.tags.size(); j++) {
	  if (j > 0) std::cout << ",";
	  std::cout << line.tags[j];
	}
	std::cout << "\t" << line.country << "\t" << line.state << "\t" << line.city << "\t" << line.metro;
	std::cout << "\n";
	// std::cerr << vid << "\t" << full_string << "\n";
      } else {
	// std::cerr << vid  << "\t" << "----------------------------\n";
      }
    }
  }
  return (0);
}


int main_(int argc, char** argv) {
  std::ifstream imp_data("impressions_compact.csv");
  struct stats_struct {
    std::unordered_set<unsigned long> users;
    unsigned long impressions;
  };
  std::string line;

  std::unordered_map<std::string, stats_struct> stats;
  
  while (std::getline(imp_data, line)) {
    auto parts = basic_split_string(line, "\t");
    if (parts.size() < 6) continue;
    auto tags = basic_split_string(parts[5], ",");
    if (tags.size() != 4) {
      std::cerr << parts[5] << "\n";
      continue;
    }
    std::string record_id = parts[2] + "\t" + parts[3] + "\t" + parts[4] + "\t" + tags[2] + "\t" + tags[3];
    auto it = stats.find(record_id);
    if (it == stats.end()) {
      stats_struct ss = {};
      stats[record_id] = ss;
      it = stats.find(record_id);
    }
    it->second.impressions ++;
    it->second.users.insert(std::stoul(parts[0]));
  }

  std::cerr << "------------------------\n";
  
  for (auto i = stats.begin(); i != stats.end(); i++) {
    std::cout << i->first << "\t" << i->second.users.size() << "\t" << i->second.impressions << "\n";
  }
  
  return (0);
}
