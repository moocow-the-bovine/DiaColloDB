#!/usr/bin/perl -w

use lib qw(.);
use CollocDB;

##==============================================================================
## test: enum

sub test_enum {
  my $base = shift || 'etest';
  my $enum = CollocDB::Enum->new();
  $enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->addSymbols(qw(a b c foo bar baz));
  $enum->close();
  exit 0;
}
test_enum(@ARGV);


##==============================================================================
## MAIN

foreach $i (1..3) {
  print "---dummy[$i]---\n";
}
exit 0;

