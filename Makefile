##-*- Mode: GNUmakefile -*-

TDMCXX ?= g++
TDMLINKER ?= $(TDMCXX)
CXX ?= $(TDMCXX)

CPPFLAGS += -D_FILE_OFFSET_BITS=64
WFLAGS ?= -Wall
#OFLAGS ?= -O3 -march=native -mtune=native -fopenmp
OFLAGS ?= -ggdb -O0 -fopenmp
SFLAGS ?= -std=c++11
CXXFLAGS += $(SFLAGS) $(WFLAGS) $(OFLAGS) 

TARGETS ?= tdm-compile tdm-header tdm-convert tdm-filter tdm-tfidf tdm-svd
#TARGETS += dict2bin dict2txt dict-find
#tdm-bin2mm tdm-bin2ccs
#txt2tdm-bin
CLEANFILES += $(TARGETS)

##-- petsc/slepc stuff
SLEPC_DIR ?= /usr/lib/slepc
PETSC_DIR ?= /usr/lib/petsc
SLEPC_CONF = $(firstword $(wildcard ${SLEPC_DIR}/conf/slepc_common ${SLEPC_DIR}/lib/slepc/conf/slepc_common))

##======================================================================
## top-level
all: $(TARGETS)

##-- petsc includes (after top-level rule)
include ${SLEPC_CONF}
PETSC_CXXCOMPILE_LOCAL = $(filter-out -g -O2,${PETSC_CXXCOMPILE})
PETSC_CLINKER_LOCAL    = $(filter-out -g -O2,${CLINKER})

##======================================================================
## deps
#common_deps = tdmModel.h tdmIO.h
common_deps = tdmCommon.h tdmDict.h tdmModel.h tdmIO.h
#tdmConvert.h

##======================================================================
## config
config:
	@echo "PETSC_DIR=$(PETSC_DIR)"
	@echo "SLEPC_DIR=$(SLEPC_DIR)"
	@echo "PCC_FLAGS=$(PCC_FLAGS)"
	@echo "PCC_LINKER_FLAGS=$(PCC_LINKER_FLAGS)"
	@echo "CPPFLAGS=$(CPPFLAGS)"
	@echo "CFLAGS=$(CFLAGS)"
	@echo "CXXFLAGS=$(CXXFLAGS)"
	@echo "PETSC_CXXCOMMPILE_LOCAL=$(PETSC_CXXCOMPILE_LOCAL)"
	@echo "PETSC_CLINKER_LOCAL=$(PETSC_CLINKER_LOCAL)"

##======================================================================

tdm-compile.o: tdmCompile.h
tdm-convert.o: tdmConvert.h
tdm-filter.o:  tdmFilter.h tdmCompile.h
tdm-tfidf.o: tdmTfIdf.h
tdm-svd.o: tdm-svd.cc tdmSvd.h $(common_deps)
	$(PETSC_CXXCOMPILE_LOCAL) -o $@ $<

%.o: %.cc $(common_deps)
	$(TDMCXX) $(CPPFLAGS) $(CXXFLAGS) -o $@ -c $<

##======================================================================
## linker
tdm-compile: tdm-compile.o
	$(TDMLINKER) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-convert: tdm-convert.o
	$(TDMLINKER) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-header: tdm-header.o
	$(TDMLINKER) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-filter: tdm-filter.o
	$(TDMLINKER) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-tfidf: tdm-tfidf.o
	$(TDMLINKER) $(CXXFLAGS) $(LDFLAGS) -o $@ $^

tdm-svd: tdm-svd.o
	$(PETSC_CLINKER_LOCAL) $(LDFLAGS) -o $@ $^ $(SLEPC_LIB)

##======================================================================
clean:
	rm -f $(TARGETS) $(TARGETS:=.o)


