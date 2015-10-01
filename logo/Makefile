##-*- Mode: GNUmakefile -*-

svg ?= $(wildcard diacollo-logo*.svg)

TARGETS ?= pdf png
#TARGETS += eps

all: $(TARGETS)

pdf: $(svg:.svg=.pdf)
%.pdf: %.svg
	inkscape -C -A $@ $<

eps: $(svg:.svg=.eps)
%.eps: %.svg
	inkscape -C -E $@ $<

png: $(svg:.svg=.png)
%.png: %.svg
	inkscape -C -b white -d 72 -e $@ $<

clean:
	rm -rf *.pdf *.eps *.png

