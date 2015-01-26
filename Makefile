##-- test colloc-index stuff

SHELL = /bin/bash -o pipefail
.DELETE_ON_ERROR:
.SECONDARY:

con ?= kern/index/kern01/kern01.con
dst ?= $(notdir $(con:.con=))

RML ?= $(HOME)/work/bbaw/ddc/ddc
export RML

csort ?= env -i LC_ALL=C sort
cuniq ?= env -i LC_ALL=C uniq

##--------------------------------------------------------------
## dump (ddc-json)
#dump: dump-ddc-json
dump-ddc-json: $(dst).dump-ddc-json.files
$(dst).dump-ddc-json.files: $(con)
	rm -rf $(@:.files=.d)
	mkdir -p $(@:.files=.d)
	$(RML)/bin/ddc_dump -f $(con) -o $(@:.files=.d)
	find $(@:.files=.d) -maxdepth 1 -name '*.json' | $(csort) -t/ -nk2 >$@

#afiles ?= $(notdir $(patsubst %.json,%,$(shell cat $(dst).dump-ddc.files)))

##--------------------------------------------------------------
## dump (ddc-tabs)
dump: dump-ddc-tabs
dump-ddc-tabs: $(dst).dump-ddc-tabs.files
$(dst).dump-ddc-tabs.files: $(con)
	rm -rf $(@:.files=.d)
	mkdir -p $(@:.files=.d)
	$(RML)/bin/ddc_dump -t -f $(con) -o $(@:.files=.d)
	find $(@:.files=.d) -maxdepth 1 -name '*.tabs' | $(csort) -t/ -nk2 >$@

files ?= $(notdir $(patsubst %.tabs,%,$(shell cat $(dst).dump-ddc-tabs.files)))

##--------------------------------------------------------------
config:
	@echo "files=$(wordlist 1,10,$(files)) ..."

##--------------------------------------------------------------
## dump (json->tj)
#dump: dump-tj
dump-tj: $(dst).dump-tj.files
$(dst).dump-tj.files: $(dst).dump-ddc-json.files
	rm -rf $(@:.files=.d)
	mkdir -p $(@:.files=.d)
	$(MAKE) dump-tj-files
	find $(@:.files=.d) -maxdepth 1 -name '*.tj' | $(csort) -t/ -nk2 >$@

dump-tj-files: $(addprefix $(dst).dump-tj.d/,$(files:=.tj))
$(dst).dump-tj.d/%.tj: $(dst).dump-ddc-json.d/%.json
	ddc-dump2tj.perl $< -o=$@

##--------------------------------------------------------------
## dump (tabs->text)
#dump: dump-t
dump-t: $(dst).dump-t.files
$(dst).dump-t.files: $(dst).dump-ddc-tabs.files
	rm -rf $(@:.files=.d)
	mkdir -p $(@:.files=.d)
	$(MAKE) dump-t-files
	find $(@:.files=.d) -maxdepth 1 -name '*.t' | $(csort) -t/ -nk2 >$@

dump-t-files: $(addprefix $(dst).dump-t.d/,$(files:=.t))
$(dst).dump-t.d/%.t: $(dst).dump-ddc-tabs.d/%.tabs
	tt-cut.awk '$$1' $< >$@

##--------------------------------------------------------------
## dump (tabs->wld)
dump-dwl: $(dst).dump-dwl.files
$(dst).dump-dwl.files: $(dst).dump-ddc-tabs.files
	rm -rf $(@:.files=.d)
	mkdir -p $(@:.files=.d)
	$(MAKE) dump-dwl-files
	find $(@:.files=.d) -maxdepth 1 -name '*.t' | $(csort) -t/ -nk2 >$@

dump-dwl-files: $(addprefix $(dst).dump-dwl.d/,$(files:=.dwl))
$(dst).dump-dwl.d/%.dwl: $(dst).dump-ddc-tabs.d/%.tabs
	./tabs2dwl.perl $< > $@


##--------------------------------------------------------------
## 1-grams, enum (text)

1g: t-1g
t-1g: $(dst).t.1g
$(dst).t.1g: $(dst).dump-t.files
	tt-1grams.perl -v=2 -list $< -freq$(csort) -o=$@

enum: t-enum
t-enum: $(dst).t.s2i.db $(dst).t.i2s.db
%.s2i.db: %.1g
	tt-cut.awk '$$2,NR' $< | tt-dict2db.perl -o=$@

%.i2s.db: %.1g
	tt-cut.awk 'NR,$$2' $< | tt-dict2db.perl -o=$@

##--------------------------------------------------------------
## dump (text-integers)
dump-ti: $(dst).dump-ti.files
$(dst).dump-ti.files: $(dst).dump-t.files $(dst).t.s2i.db
	rm -rf $(@:.files=.d)
	mkdir -p $(@:.files=.d)
	$(MAKE) dump-ti-files
	find $(@:.files=.d) -maxdepth 1 -name '*.ti' | $(csort) -t/ -nk2 >$@

dump-ti-files: $(addprefix $(dst).dump-ti.d/,$(files:=.ti))
$(dst).dump-ti.d/%.ti: $(dst).t.s2i.db $(dst).dump-t.d/%.t
	tt-dbapply.perl $^ | tt-cut.awk '$$2' >$@

##--------------------------------------------------------------
## n-grams (text)

nglen ?= 5
ng-t: ngrams-t
t-ng: ngrams-t
t-ng$(nglen): ngrams-t
ngrams-t: $(dst).t.ng$(nglen)
$(dst).t.ng$(nglen): $(dst).dump-t.files
	tt-ngrams.perl -eos='__$$' -n=$(nglen) -list $< | $(csort) | $(cuniq) -c | perl -pe 's{^\s*(\d+) (.*)}{$$1\t$$2};' >$@


##--------------------------------------------------------------
## n-grams (text-integers)

ngisort := $(shell perl -e'print join(" ",map {"-nk$$_"} (2..($(nglen)+1)))')

ng-ti: ngrams-ti
ti-ng: ngrams-ti
ti-ng$(nglen): ngrams-ti
ngrams-ti: $(dst).ti.ng$(nglen)
$(dst).ti.ng$(nglen): $(dst).dump-ti.files
	tt-ngrams.perl -eos=0 -n=$(nglen) -list $< | $(csort) $(ngisort) | $(cuniq) -c | perl -pe 's{^\s*(\d+) (.*)}{$$1\t$$2};' >$@

##--------------------------------------------------------------
## n-gram db (db-integers)

ngdb-ti: ngramdb-ti
ti-ngdb: ngramdb-ti
ngramdb-ti: $(dst).ti.ng$(nglen).db
%.ti.ng$(nglen).db: %.ti.ng$(nglen)
	./ti-ngrams2db.perl -pk='N$(nglen)' -pv='N' -o=$@ $<

##--------------------------------------------------------------
## n-gram bin (vec()-style, for DIY binary search on filehandle)

ngbin-ti: ngrambin-ti
ti-ngbin: ngrambin-ti
ngrambin-ti: $(dst).ti.ng$(nglen).bin
%.ti.ng$(nglen).bin: %.ti.ng$(nglen)
	./ti-ngrams2bin.perl -pk='N$(nglen)' -pv='N' -o=$@ $<
