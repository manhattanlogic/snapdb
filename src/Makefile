# this is a test
PROG = snapdb
#PROG2 = impressions_trainsform
PROG2 = impressions_trainsform_filtered
INCLUDES= -I/usr/local/include



DEBUG = -O3

CXX = g++ $(DEBUG) -std=c++14
CXXFLAGS = $(DEBUG)  $(INCLUDES)

CC = cc
CFLAGS = $(DEBUG) $(INCLUDES)

LIBS = -L/usr/local/lib -lpthread -ldl 


OBJS = $(PROG).o 
OBJS2 = $(PROG2).o
OBJS3 = $(PROG3).o

all: $(PROG3) $(PROG2) $(PROG) rproc.so query1.so phase3.so dump_skus.so compute_user_vectors.so cluster_statistics.so compute_crumbs_user_vectors.so flat_crumbs.so impression_intersect.so

$(PROG2): $(OBJS2)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS) -lcctz

$(PROG3): $(OBJS3)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS) 

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
phase3.so: phase3.cpp
	$(CXX) -Wall -fPIC -c phase3.cpp -o phase3.o
	$(CXX) -shared -rdynamic -o phase3.so phase3.o
dump_skus.so: dump_skus.cpp
	$(CXX) -Wall -fPIC -c dump_skus.cpp -o dump_skus.o
	$(CXX) -shared -rdynamic -o dump_skus.so dump_skus.o
compute_user_vectors.so: compute_user_vectors.cpp
	$(CXX) -Wall -fPIC -c compute_user_vectors.cpp -o compute_user_vectors.o
	$(CXX) -shared -rdynamic -o compute_user_vectors.so compute_user_vectors.o
cluster_statistics.so: cluster_statistics.cpp
	$(CXX) -Wall -fPIC -c cluster_statistics.cpp -o cluster_statistics.o
	$(CXX) -shared -rdynamic -o cluster_statistics.so cluster_statistics.o
compute_crumbs_user_vectors.so: compute_crumbs_user_vectors.cpp
	$(CXX) -Wall -fPIC -c compute_crumbs_user_vectors.cpp -o compute_crumbs_user_vectors.o
	$(CXX) -shared -rdynamic -o compute_crumbs_user_vectors.so compute_crumbs_user_vectors.o
flat_crumbs.so: flat_crumbs.cpp
	$(CXX) -Wall -fPIC -c flat_crumbs.cpp -o flat_crumbs.o
	$(CXX) -shared -rdynamic -o flat_crumbs.so flat_crumbs.o
impression_intersect.so: impression_intersect.cpp
	$(CXX) -Wall -fPIC -c impression_intersect.cpp -o impression_intersect.o
	$(CXX) -shared -rdynamic -o impression_intersect.so impression_intersect.o
endif
ifeq ($(UNAME_S),Darwin)
$(PROG): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)
rproc.so: rproc.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup rproc.cpp -o $@ 
query1.so: query1.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup query1.cpp -o $@
phase3.so: phase3.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup phase3.cpp -o $@
dump_skus.so: dump_skus.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup dump_skus.cpp -o $@
compute_user_vectors.so: compute_user_vectors.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup compute_user_vectors.cpp -o $@
cluster_statistics.so: cluster_statistics.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup cluster_statistics.cpp -o $@
compute_crumbs_user_vectors.so: compute_crumbs_user_vectors.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup compute_crumbs_user_vectors.cpp -o $@
flat_crumbs.so: flat_crumbs.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup flat_crumbs.cpp -o $@
impression_intersect.so: impression_intersect.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup impression_intersect.cpp -o $@
endif

clean:
	rm -rf $(PROG) $(PROG).dSYM  $(OBJS) *.o *.dylib *.so
