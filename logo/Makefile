##-*- Mode: GNUmakefile -*-

svg ?= $(wildcard diacollo-logo*.svg)

TARGETS ?= png-big png-logo png-icon ico pdf
#TARGETS += pdf
#TARGETS += eps


all: $(TARGETS)

pdf: $(svg:.svg=.pdf)
%.pdf: %.svg
	inkscape -C -A $@ $<

eps: $(svg:.svg=.eps)
%.eps: %.svg
	inkscape -C -E $@ $<

png: png-big
png-big: $(svg:.svg=-big.png)
%-big.png: %.svg
	inkscape -C -d 72 -e $@ $<

png-logo: $(svg:.svg=-logo.png)
%-logo.png: %.svg
	inkscape -D -h 57 -e $@ $<

png-icon: $(svg:.svg=-icon.png)
%-icon.png: %.svg
	inkscape -D -h 16 -e $@ $<

ico: $(svg:.svg=-icon.ico)
%-icon.ico: %-icon.png
	convert -resize x16 -gravity center -crop 16x16+0+0 $< -background transparent -flatten -colors 256 $@

clean:
	rm -rf *.pdf *.eps *.png

