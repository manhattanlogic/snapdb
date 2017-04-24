# this is a test
PROG = snapdb

INCLUDES= -I/usr/local/include

CXX = g++ -std=c++14
CXXFLAGS = -O3  $(INCLUDES)

CC = cc
CFLAGS = -O3 $(INCLUDES)

LIBS = -L/usr/local/lib -lpthread -ldl 


OBJS = $(PROG).o

all: $(PROG) rproc.so query1.so

UNAME_S := $(shell uname -s)
ifeq ($(UNAME_S),Linux)
$(PROG): $(OBJS)
	$(CXX) $(CXXFLAGS) -Wl,--export-dynamic -o $@ $^ $(LIBS)

rproc.so: rproc.cpp
	$(CXX) -Wall -fPIC -c rproc.cpp -o rproc.o
	$(CXX) -shared -rdynamic -o rproc.so rproc.o

query1.so: query1.cpp
	$(CXX) -Wall -fPIC -c query1.cpp -o query1.o
	$(CXX) -shared -rdynamic -o query1.so query1.o

endif
ifeq ($(UNAME_S),Darwin)
$(PROG): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

rproc.so: rproc.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup rproc.cpp -o $@ 
query1.so: query1.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup query1.cpp -o $@ 
endif

clean:
	rm -rf $(PROG) $(PROG).dSYM  $(OBJS) *.o *~ rproc.dylib rproc.so
