use ExtUtils::MakeMaker;
use Config;

##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------
WriteMakefile
  (
   NAME		=> 'DiaColloDB::XS',
   AUTHOR       => 'Bryan Jurish <moocow@cpan.org>',
   ABSTRACT	=> 'XS utilities for DiaColloDB (Linux/GCC only)',
   VERSION_FROM => 'XS.pm',

   ##-- compiler stuff
   CC           => 'g++',
   LD           => 'g++',
   #LIBS         => "$MY_LDFLAGS $MY_LIBS",      ##-- additional libraries
   #DEFINE       => '-DHAVE_CONFIG_H',     ##-- additional defines
   #INC          => $MY_IFLAGS,      ##-- additional includes
   CCFLAGS      => '-Wall -D_FILE_OFFSET_BITS=64 -fopenmp',
   OPTIMIZE     => '-O2',  ##-- optimization flags
   LDFLAGS       => "$Config{ldflags} -fopenmp \$(OPTIMIZE)",
   LDDLFLAGS     => "$Config{lddlflags} -fopenmp",

   ##-- output binaries
   OBJECT => [qw(XS.o)],
   #EXE_FILES => [qw(dcdb-cofgen)],
   clean => {
             FILES => '*.o dcdb-cofgen',
            },

   ##-- DEBUG
   #MAKEAPERL=>1,
   ##LINKTYPE=>'static', ##-- requires libperl-dev on debian
   #MAP_TARGET=>'ucperl',
   #CCFLAGS=>'-g',
   ##-- /DEBUG

   ##-- XS stuff (that probably doesn't need to be changed)
   XSOPT        => '-C++',
   #TYPEMAPS     => ['perlobject.map', 'typemap'],

   ##-- prerequisites
   #MIN_PERL_VERSION => '5.8.0', ##-- perl 5.6 chokes with "Can't declare another package's variables at t/01_ini.t line 22."
   PREREQ_PM    => {
		    #'Module' => $version,
		    #'Test::More' => 0,
		   },
  );


#-----------------------------------------------------------------------
# Extensions
#-----------------------------------------------------------------------

##---------------------------------------------------------------
sub MY::post_initialize {
  package MY;
  my $inherited = shift->SUPER::post_initialize(@_);
  $inherited .= <<EOF;
EOF
  return $inherited;
}

##---------------------------------------------------------------
sub MY::constants {
  package MY;
  my $inherited = shift->SUPER::constants(@_);
  return $inherited;
}

##---------------------------------------------------------------
sub MY::depend {
  package MY;
  my $inherited = shift->SUPER::depend(@_);
  $inherited .= <<EOF;

EOF
  return $inherited;
}

##---------------------------------------------------------------
sub MY::c_o {
  package MY;
  my $inherited = shift->SUPER::c_o(@_);
  $inherited .= <<EOF;

EOF
  return $inherited;
}

##---------------------------------------------------------------
sub MY::special_targets {
  package MY;
  my $inherited = shift->SUPER::special_targets(@_);
  $inherited .= <<EOF;

###-- special targets: local binaries
.PHONY: dcdb-cofgen
dcdb-cofgen:
\t\$(MAKE) -rf cxx.mak dcdb-cofgen

EOF
  return $inherited;
}

##---------------------------------------------------------------
sub MY::postamble {
  package MY;
  my $inherited = shift->SUPER::postamble(@_);
  return $inherited;
}
