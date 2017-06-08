# this is a test
PROG = snapdb
PROG2 = impressions_trainsform
INCLUDES= -I/usr/local/include

CXX = g++ -std=c++14
CXXFLAGS = -O3  $(INCLUDES)

CC = cc
CFLAGS = -O3 $(INCLUDES)

LIBS = -L/usr/local/lib -lpthread -ldl 


OBJS = $(PROG).o 
OBJS2 = $(PROG2).o

all: $(PROG2) $(PROG) rproc.so query1.so dump_skus.so compute_user_vectors.so cluster_statistics.so compute_crumbs_user_vectors.so flat_crumbs.so

$(PROG2): $(OBJS2)
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
endif
ifeq ($(UNAME_S),Darwin)
$(PROG): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)
rproc.so: rproc.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup rproc.cpp -o $@ 
query1.so: query1.cpp
	$(CXX) -dynamiclib -undefined dynamic_lookup query1.cpp -o $@
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
endif

clean:
	rm -rf $(PROG) $(PROG).dSYM  $(OBJS) *.o *.dylib *.so
