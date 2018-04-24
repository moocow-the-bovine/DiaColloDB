##-*- Mode: GNUmakefile -*-

CXX ?= g++

CPPFLAGS += -D_FILE_OFFSET_BITS=64
WFLAGS ?= -Wall
OFLAGS ?= -O3 -march=native -mtune=native -fopenmp
#OFLAGS ?= -O2 -ggdb
#OFLAGS ?= -ggdb -O0 -fopenmp
SFLAGS ?= -std=c++11

CXXFLAGS += $(SFLAGS) $(WFLAGS) $(OFLAGS) 

TARGETS ?= tdm-compile tdm-header tdm-convert tdm-filter
#TARGETS += dict2bin dict2txt dict-find
#tdm-bin2mm tdm-bin2ccs
#txt2tdm-bin
CLEANFILES += $(TARGETS)

##======================================================================
## top-level
all: $(TARGETS)

##======================================================================
## deps
#common_deps = tdmModel.h tdmIO.h
common_deps = tdmCommon.h tdmDict.h tdmModel.h tdmIO.h
#tdmConvert.h

##======================================================================

tdm-compile.o: tdmCompile.h
tdm-convert.o: tdmConvert.h
tdm-filter.o:  tdmFilter.h tdmCompile.h

%.o: %.cc $(common_deps)
	$(CXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ -c $<

##======================================================================
## linker
tdm-compile: tdm-compile.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-convert: tdm-convert.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-header: tdm-header.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-filter: tdm-filter.o
	$(CXX) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

##======================================================================
clean:
	rm -f $(TARGETS) $(TARGETS:=.o)


