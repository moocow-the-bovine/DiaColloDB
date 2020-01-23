#/*-*- Mode: C++ -*-*/

MODULE = DiaColloDB::XS		PACKAGE = DiaColloDB::XS::Cofreqs

##-- enable perl prototypes
PROTOTYPES: ENABLE

##--------------------------------------------------------------
## cofgen.h
int
generatePairs(char *ifile, char *ofile, size_t dmax)
 PREINIT:
  const char *prog = "DiaColloDB::XS::Cofreqs::generatePairs()";
 CODE:
  RETVAL = CofGenerator<>(prog).main(ifile,ofile,dmax);
 OUTPUT:
  RETVAL

