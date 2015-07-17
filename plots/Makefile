##-*- Mode: GNUmakefile -*-

diffs ?= diff adiff min max sum avg havg gavg lavg

TARGETS ?= svg png

all: $(TARGETS)

##--------------------------------------------------------------
gp: $(diffs:=.gp)
%.gp:
	:>$@
	echo "load \"diffops.gp\";" >> $@
	echo "set title \"$*(a,b)\";" >> $@
	test \! -e "$*.gpo" || cat $(wildcard $*.gpo) >>$@
	echo "splot $*(x,y) notitle;" >> $@

##--------------------------------------------------------------
png: $(diffs:=.png)
%.png: %.gp diffops.gp
	echo "set term png; set output \"$@\"; load \"$<\";" | gnuplot

##--------------------------------------------------------------
svg: $(diffs:=.svg)
%.svg: %.gp diffops.gp
	echo "set term svg background rgb \"#FFFFFF\"; set output \"$@\"; load \"$<\";" | gnuplot

##--------------------------------------------------------------
pdf-cairo: $(diffs:=.cairo.pdf)
cairo-pdf: $(diffs:=.cairo.pdf)
%.cairo.pdf: %.gp diffops.gp
	echo "set term pdfcairo; set output \"$@\"; load \"$<\";" | gnuplot

pdf: $(diffs:=.pdf)
%.pdf: %.eps
	epstopdf --outfile=$@ $<

##--------------------------------------------------------------
eps: $(diffs:=.eps)
%.eps: %.gp diffops.gp
	echo "set term postscript eps color; set output \"$@\"; load \"$<\";" | gnuplot

mono-eps: $(diffs:=.mono.eps)
eps-mono: $(diffs:=.mono.eps)
%.mono.eps: %.gp diffops.gp
	echo "set term postscript eps mono; set output \"$@\"; load \"$<\";" | gnuplot

##--------------------------------------------------------------
clean:
	rm -f *.svg *.png *.jpg *.eps *.pdf $(diffs:=.gp)