#!/usr/bin/perl -w

use lib qw(.);
use CollocDB::EnumFile;

if (@ARGV < 2) {
  print STDERR "Usage: $0 ENUMBASE [INFILE(s)...]\n";
  exit 1;
}

##-- setup logger
CollocDB::Logger->ensureLog();

##-- open enum
my $ebase = shift;
my $ef = CollocDB::EnumFile->new(base=>$ebase, flags=>'r')
  or die("$0: failed to create enum '$ebase': $!");

open(my $sfh, "<$ebase.s")
  or die("$0: open failed for $ebase.s: $!");  ##-- pack('(n/A)*', @$i2s)
open(my $ixfh, "<$ebase.ix")
  or die("$0: open failed for $ebase.ix: $!"); ##-- [$i] => pack('N',$offset_in_sfh_of_string_with_id_i)
open(my $sxfh, "<$ebase.sx")
  or die("$0: open failed for $ebase.sx: $!"); ##-- [$j] => pack('NN',$offset_in_sfh_of_string_with_sortindex_j_and_id_i, $i)

##-- map inputs
my ($i,$rest,$s);
while (defined($_=<>)) {
  chomp;
  next if (/^$/);
  ($s,$rest) = split(/\t/,$_,2);
  $i = $ef->s2i($s) // -1;
  print join("\t", $s, ($rest ? $rest : qw()), $i), "\n";
}

##-- finish up
$ef->close()
  or die("$0: failed to close enum '$ebase': $!");
