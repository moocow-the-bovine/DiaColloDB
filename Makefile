##-*- Mode: GNUmakefile -*-

CXX ?= g++

CPPFLAGS += -D_FILE_OFFSET_BITS=64
WFLAGS ?= -Wall
OFLAGS ?= -O3 -march=native -mtune=native -fopenmp
#OFLAGS ?= -O2 -ggdb
#OFLAGS ?= -ggdb -O0 -fopenmp
SFLAGS ?= -std=c++11

CXXFLAGS += $(SFLAGS) $(WFLAGS) $(OFLAGS) 

TARGETS ?= txt2tdm tdm-convert tdm-header
#tdm-bin2mm tdm-bin2ccs
#txt2tdm-bin
CLEANFILES += $(TARGETS)

##======================================================================
## top-level
all: $(TARGETS)

##======================================================================
## deps
common_deps = tdmModel.h tdmIO.h
#tdmConvert.h

##======================================================================

tdm-convert.o: tdmConvert.h
tdm-header.o: tdmConvert.h

%.o: %.cc $(common_deps)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ -c $<

##======================================================================
## linker
txt2tdm: txt2tdm.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-convert: tdm-convert.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-header: tdm-header.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

##======================================================================
clean:
	rm -f $(TARGETS:=.c) $(TARGETS:=.o)


