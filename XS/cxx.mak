##-*- Mode: GNUmakefile -*-

TARGETS ?= \
	cofgen

CC       = g++
LD       = $(CC)
OFLAGS ?= -O2
CFLAGS ?= -Wall -D_FILE_OFFSET_BITS=64 $(OFLAGS) -fopenmp 
CXXFLAGS ?= $(CFLAGS)
LDFLAGS ?= $(CFLAGS)
LIBS ?=

all: $(TARGETS)

##-- dependencies
dcdb-cofgen.o: dcdb-cofgen.cc utils.h

##-- patterns
.SUFFIXES: .cc .o
.cc.o:
	g++ $(CXXFLAGS) -c $< -o $@

##-- final targets
dcdb-cofgen: dcdb-cofgen.o
	$(LD) $(LDFLAGS) -o $@ $^ $(LIBS)

##-- clean
.PHONY: clean
clean:
	rm -f *.o $(TARGETS)

