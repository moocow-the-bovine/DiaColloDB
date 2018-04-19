##-*- Mode: GNUmakefile -*-

CXX ?= g++

CPPFLAGS += -D_FILE_OFFSET_BITS=64
WFLAGS ?= -Wall
OFLAGS ?= -O3 -march=native -mtune=native -fopenmp
#OFLAGS ?= -O2 -ggdb
#OFLAGS ?= -ggdb -O0
SFLAGS ?= -std=c++11

CXXFLAGS += $(SFLAGS) $(WFLAGS) $(OFLAGS) 

TARGETS ?= txt2tdm
#txt2tdm-bin
CLEANFILES += $(TARGETS)

##======================================================================
## top-level
all: $(TARGETS)

##======================================================================
## deps
txt2tdm.o: txt2tdm.cc tdmModel.h

##======================================================================
%.o: %.cc
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ -c $<

##======================================================================
## linker
txt2tdm: txt2tdm.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

txt2tdm-bin: txt2tdm-bin.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

##======================================================================
clean:
	rm -f $(TARGETS:=.c) $(TARGETS:=.o)

