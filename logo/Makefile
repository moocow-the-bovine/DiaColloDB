##-*- Mode: GNUmakefile -*-

svg ?= $(wildcard diacollo-logo*.svg)

TARGETS ?= png-big png-med png-small ico pdf
#TARGETS += pdf
#TARGETS += eps


all: $(TARGETS)

pdf: $(svg:.svg=.pdf)
%.pdf: %.svg
	inkscape -D -A $@ $<

eps: $(svg:.svg=.eps)
%.eps: %.svg
	inkscape -D -E $@ $<

png: png-big
png-big: $(svg:.svg=-big.png)
%-big.png: %.svg
	inkscape -D -d 100 -e $@ $<

png-med: $(svg:.svg=-med.png)
%-med.png: %.svg
	inkscape -D -h 57 -e $@ $<

png-small: $(svg:.svg=-small.png)
%-small.png: %.svg
	inkscape -D -h 16 -e $@ $<

ico: $(svg:.svg=-small.ico)
%-small.ico: %-small.png
	convert -resize x16 -gravity center -crop 16x16+0+0 $< -background transparent -flatten -colors 256 $@

clean:
	rm -rf *.pdf *.eps *.png *.ico

