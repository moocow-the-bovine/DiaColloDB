#!/usr/bin/perl -w

use lib qw(.);
use CollocDB;
use CollocDB::Utils qw(:sort :run :env);
use Getopt::Long;
use strict;

##======================================================================
## Command-line
our ($help);
our $outbase = undef;
our $n = 1;
our $keeptmp = 0;

GetOptions(##-- general
	   'help|h' => \$help,

	   ##-- I/O
	   'length|width|n=i' => \$n,
	  );

if ($help)  {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] [TOKFILE]

 Options:
   -help
   -n N

EOF
  exit 1;
}

CollocDB->ensureLog();

##======================================================================
## MAIN

##-- reader
my $tokfile = shift(@ARGV) // '-';
open(my $tokfh, "<$tokfile")
  or die("$0: open failed for '$tokfile': $!");
binmode($tokfh,':raw');

my (@sent,$i,$j,$wi,$wj);
while (!eof($tokfh)) {
  @sent = qw();
  while (defined($_=<$tokfh>)) {
    chomp;
    last if (/^$/ );
    push(@sent,$_);
  }
  next if (!@sent);

  ##-- get tuples
  foreach $i (0..$#sent) {
    $wi = $sent[$i];
    print #$sortfh
      (map {"$wi\t$sent[$_]\n"}
       grep {$_>=0 && $_<=$#sent && $_ != $i}
       (($i-$n)..($i+$n))
      );
  }
}
