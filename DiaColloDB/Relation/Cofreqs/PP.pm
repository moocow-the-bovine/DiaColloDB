## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Cofreqs::PP
## Author: Bryan Jurish <moocow@cpan.org>
## Descript: DiaColloDB: pure-perl fallbacks for Cofreqs relation compilation

package DiaColloDB::Relation::Cofreqs::PP;
use DiaColloDB::Utils qw(:run :env :math :temp);
use strict;

##======================================================================
## Globals
our @ISA = qw();

##======================================================================
## Cofreqs wrappers

## $cof_or_undef = $cof->generatePairs( $tokfile )
## $cof_or_undef = $cof->generatePairs( $tokfile, $outfile )
##  + implements DiaColloDB::Relation::Cofreqs::generatePairs()
sub generatePairs {
  my ($cof,$tokfile,$outfile) = @_;
  my $dmax = $cof->{dmax} // 1;
  $cof->vlog('trace', "create(): stage1/pp: generate pairs (dmax=$dmax)");

  $outfile = "$cof->{base}.dat" if (!$outfile);

  ##-- token reader fh
  CORE::open(my $tokfh, "<$tokfile")
    or $cof->logconfess("create(): open failed for token-file '$tokfile': $!");
  binmode($tokfh,':raw');

  ##-- temporary output file
  my $tmpfile = tmpfile("$outfile.tmp", UNLINK=>(!$cof->{keeptmp}))
    or $cof->logconfess("failed to create temp-file '$outfile.tmp': $!");
  open(my $tmpfh, ">$tmpfile")
    or $cof->logconfess("failed to open temp-file '$outfile.tmp': $!");
  binmode($tmpfh,':raw');

  ##-- stage1: generate pairs
  my (@sent,$i,$j,$wi,$wj);
  while (!eof($tokfh)) {
    @sent = qw();
    while (defined($_=<$tokfh>)) {
      chomp;
      last if (/^$/ );
      push(@sent,$_);
    }
    next if (!@sent);

    ##-- get pairs
    foreach $i (0..$#sent) {
      $wi = $sent[$i];
      print $tmpfh
	(map {"$wi\t$sent[$_]\n"}
	 grep {$_>=0 && $_<=$#sent && $_ != $i}
	 (($i-$dmax)..($i+$dmax))
	);
    }
  }
  close($tmpfh)
    or $cof->logconfess("close failed for temp-file '$tmpfile': $!");

  ##-- sort & count
  env_push(LC_ALL=>'C');
  runcmd("sort -nk1 -nk2 -nk3 $tmpfile | uniq -c - $outfile")==0
    or $cof->logconfess("create(): open failed for pipe to sort|uniq: $!");
  env_pop();

  ##-- cleanup
  CORE::unlink($tmpfile) if (!$cof->{keeptmp});

  return $cof;
}

*DiaColloDB::Relation::Cofreqs::generatePairs = \&generatePairs;

1; ##-- be happy

__END__
