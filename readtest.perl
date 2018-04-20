#!/usr/bin/perl -w

use PDL;
use PDL::IO::FastRaw;
use PDL::CCS::Nd;
use PDL::CCS::IO::FastRaw;
use strict;
BEGIN {
  $, = ' ';
}

if (0) {
  my $infile = shift || 'test.float';
  my $p = readfraw($infile);
  defined($p) or die("$0: readfraw failed for $infile: $!");
  print "$p\n";
}
else {
  my $infile = shift || 'pnn.out.tdm_bin.ccs';
  my $ccs = ccs_mapfraw($infile,{ReadOnly=>1,Creat=>0});
  defined($ccs) or die("$0: ccs_mapfraw failed for $infile: $!");
  print "$ccs\n";
}
