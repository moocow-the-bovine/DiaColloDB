#/*-*- Mode: C++ -*-*/

MODULE = DiaColloDB::XS		PACKAGE = DiaColloDB::XS::CofUtils

##-- enable perl prototypes
PROTOTYPES: ENABLE

##--------------------------------------------------------------
## cof-gen.h
int
generatePairsTmpXS(char *ifile, char *ofile, size_t dmax)
 PREINIT:
  const char *prog = "DiaCollODB::XS::CofUtils::generatePairsTmpXS()";
 CODE:
  RETVAL = CofGenerator<>(prog).main(ifile,ofile,dmax);
 OUTPUT:
  RETVAL

##--------------------------------------------------------------
## cof-compile.h
int
loadTextFileXS32(char *ifile, char *ofile, size_t fmin)
 PREINIT:
  const char *prog = "DiaCollODB::XS::CofUtils::loadTextFileXS32()";
 CODE:
  RETVAL = CofCompiler32::main(prog, ifile, ofile, fmin);
 OUTPUT:
  RETVAL

int
loadTextFileXS64(char *ifile, char *ofile, size_t fmin)
 PREINIT:
  const char *prog = "DiaCollODB::XS::CofUtils::loadTextFileXS64()";
 CODE:
  RETVAL = CofCompiler64::main(prog, ifile, ofile, fmin);
 OUTPUT:
  RETVAL

