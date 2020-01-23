## -*- Mode: CPerl -*-
## File: DiaColloDB::XS::CofUtils.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Descript: DiaColloDB: C++ utilities for Cofreqs relation compilation

package DiaColloDB::XS::CofUtils;
use DiaColloDB::XS;
use DiaColloDB::Utils qw(:run :env :math :temp);
use strict;

##======================================================================
## Globals
our @ISA = qw();

BEGIN {
  print STDERR "loading ", __PACKAGE__, "\n"; ##-- DEBUG
}

##======================================================================
## Cofreqs wrappers

## $cof_or_undef = $cof->generatePairs( $tokfile )
## $cof_or_undef = $cof->generatePairs( $tokfile, $outfile )
##  + implements DiaColloDB::Relation::Cofreqs::generatePairs()
##  + input: $tokfile : as passed to Cofreqs::create() (= "$dbdir/tokens.dat")
##  + output: $outfile : co-occurrence frequencies (= "$cof->{base}.dat"), as passed to stage2
sub generatePairs {
  my ($cof,$tokfile,$outfile) = @_;
  my $dmax = $cof->{dmax} // 1;
  $cof->vlog('trace', "create(): stage1/xs: generate pairs (dmax=$dmax)");

  $outfile = "$cof->{base}.dat" if (!$outfile);
  my $tmpfile = tmpfile("$outfile.tmp", UNLINK=>(!$cof->{keeptmp}))
    or $cof->logconfess("failed to create temp-file '$outfile.tmp': $!");

  env_push('LC_ALL'=>'C');
  generatePairsXS($tokfile, $tmpfile, ($cof->{dmax}//1))==0
    or $cof->logconfess("failed to generate co-occurrence frequencies for '$tokfile' to '$tmpfile': $!");
  runcmd("sort -nk1 -nk2 -nk3 $tmpfile | uniq -c - $outfile")==0
    or $cof->logconfess("failed to collate co-occurrence frequencies from '$tmpfile' to '$outfile': $!");
  env_pop();

  return $cof;
}

*DiaColloDB::Relation::Cofreqs::generatePairs = \&generatePairs;

1; ##-- be happy

__END__
