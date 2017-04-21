PROG = hs

INCLUDES= -I/usr/local/include

CXX = c++ -std=c++14
CXXFLAGS = -O3  $(INCLUDES)

CC = cc
CFLAGS = -O3 $(INCLUDES)

LIBS = -L/usr/local/libn -lpthread


OBJS = test.o

$(PROG): $(OBJS)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LIBS)

clean:
	rm -rf $(PROG) $(PROG).dSYM  $(OBJS) *.o *~
