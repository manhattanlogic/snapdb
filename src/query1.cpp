#include <string>
#include <stdlib.h>
#include <stdio.h>
#include "snapdb.hpp"
#include <sstream>
#include <unordered_set>
#include <memory.h>
#include <fstream>
#include <iostream>

// history_filter contains filter for the display application

extern "C"
char * query() {
 
  std::stringstream result;

  std::ofstream vids("long_vids.dat",std::ios::binary);
  
  for (auto i = json_history.begin(); i != json_history.end(); i++) {
    vids.write((char *)&i->first, sizeof(unsigned long));
  }
  
  
  // end of custom code
  result << "ok\n";
  char * buffer = (char *)malloc(result.str().size() + 1);
  memcpy(buffer, result.str().c_str(), result.str().size());
  buffer[result.str().size()] = 0;
  return buffer;
}


/*
1. overstock data. rj data. by changing creative show possible influence on the funnel.
2. 
*/
