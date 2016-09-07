##-*- Mode: GNUmakefile -*-

diffs  ?= diff adiff min max sum avg havg gavg lavg
scores ?= f fm lf lfm milf mi1 mi3 ld ll
src    ?= $(patsubst %,diff-%,$(diffs)) $(patsubst %,score-%,$(scores))
#src    ?= $(patsubst %,score-%,$(scores))

plotdeps ?= diffs.gp scores.gp common.gp

TARGETS ?= png

all: $(TARGETS)

##--------------------------------------------------------------
gp: $(src:=.gp)
diff-%.gp:
	:>$@
	echo "load \"diffs.gp\";" >> $@
	echo "set title \"$*(a,b)\";" >> $@
	test \! -e "diff-$*.gpo" || cat $(wildcard diff-$*.gpo) >>$@
	echo "splot $*(x,y) notitle;" >> $@

score-%.gp:
	:>$@
	echo "load \"scores.gp\";" >> $@
	echo "set title \"$*(f2,f12) [N=100k,f1=100]\";" >> $@
	test \! -e "score-$*.gpo" || cat $(wildcard score-$*.gpo) >>$@
	echo "splot $*(x,y) notitle;" >> $@

##--------------------------------------------------------------
mono-png: $(src:=.mono.png)
png-mono: $(src:=.mono.png)
%.mono.png: %.mono.eps
	pstopnm -stdout -pgm -portrait -xsize 640 $< | pnmcrop -white | pnmtopng -compression 7 > $@

png: $(src:=.png)
%.png: %.gp $(plotdeps)
	echo "set term png crop; set output \"$@\"; load \"$<\";" | gnuplot

##--------------------------------------------------------------
svg: $(src:=.svg)
%.svg: %.gp $(plotdeps)
	echo "set term svg background rgb \"#FFFFFF\"; set output \"$@\"; load \"$<\";" | gnuplot

##--------------------------------------------------------------
pdf-cairo: $(src:=.cairo.pdf)
cairo-pdf: $(src:=.cairo.pdf)
%.cairo.pdf: %.gp $(plotdeps)
	echo "set term pdfcairo; set output \"$@\"; load \"$<\";" | gnuplot

pdf: $(src:=.pdf)
%.pdf: %.eps
	epstopdf --outfile=$@ $<

##--------------------------------------------------------------
eps: $(src:=.eps)
%.eps: %.gp $(plotdeps)
	echo "set term postscript eps color; set output \"$@\"; load \"$<\";" | gnuplot

mono-eps: $(src:=.mono.eps)
eps-mono: $(src:=.mono.eps)
%.mono.eps: %.gp $(plotdeps)
	echo "set term postscript eps mono; set output \"$@\"; load \"$<\";" | gnuplot

##--------------------------------------------------------------
clean:
	rm -f *.svg *.png *.jpg *.eps *.pdf $(src:=.gp)
