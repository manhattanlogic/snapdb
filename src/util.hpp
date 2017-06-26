#ifndef UTILX_HPP
#define UTILX_HPP
#include <unordered_map>
#include <map>
#include <string>
#include <vector>
#include <fstream>


std::ifstream::pos_type get_filesize(const char* filename)
{
    std::ifstream in(filename, std::ifstream::ate | std::ifstream::binary);
    return in.tellg(); 
}


std::string replace_all(
			const std::string & str ,   // where to work
			const std::string & find ,  // substitute 'find'
			const std::string & replace // by 'replace'
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

std::vector<std::string> basic_split_string(std::string line, const char * sep = " ") {
  std::vector <std::string> result;
  char * pch = strtok ((char *)line.c_str(), sep);
  while (pch != NULL) {
    result.push_back(pch);
    pch = strtok (NULL, sep);
  }
  return result;
}

template <class T>
void safe_inc(std::unordered_map<T, long> &map, T key) {
  auto it = map.find(key);
  if (it == map.end()) {
    map[key] = 1;
  } else {
    it->second++;
  }
}
template <class T>
void safe_inc(std::map<T, long> &map, T key) {
  auto it = map.find(key);
  if (it == map.end()) {
    map[key] = 1;
  } else {
    it->second++;
  }
}


template <class T, class V>
auto safe_find(std::unordered_map<T, V> &map, T key) {
  auto it = map.find(key);
  if (it == map.end()) {
    map[key] = (V) {};
    it = map.find(key);
  }
  return it;
}

template <class T, class V>
auto safe_find(std::map<T, V> &map, T key) {
  auto it = map.find(key);
  if (it == map.end()) {
    map[key] = (V) {};
    it = map.find(key);
  }
  return it;
}
#endif

