##-*- Mode: GNUmakefile -*-

CXX ?= g++

WFLAGS ?= -Wall
OFLAGS ?= -O3 -march=native -mtune=native
#OFLAGS ?= -O2 -ggdb
#OFLAGS ?= -ggdb -O0
SFLAGS ?= -std=c++11

CXXFLAGS += $(SFLAGS) $(WFLAGS) $(OFLAGS) 

TARGETS ?= txt2tdm
CLEANFILES += $(TARGETS)

##======================================================================
## top-level
all: $(TARGETS)

##======================================================================
%.o: %.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ -c $<

##======================================================================
## linker
txt2tdm: txt2tdm.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

##======================================================================
clean:
	rm -f $(TARGETS:=.c) $(TARGETS:=.o)


