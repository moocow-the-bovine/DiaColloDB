#/*-*- Mode: C++ -*-*/

MODULE = DiaColloDB::XS		PACKAGE = DiaColloDB::XS::CofUtils

##-- enable perl prototypes
PROTOTYPES: ENABLE

##--------------------------------------------------------------
## cofgen.h
int
generatePairsXS(char *ifile, char *ofile, size_t dmax)
 PREINIT:
  const char *prog = "DiaCollODB::XS::CofUtils::generatePairsXS()";
 CODE:
  RETVAL = CofGenerator<>(prog).main(ifile,ofile,dmax);
 OUTPUT:
  RETVAL

