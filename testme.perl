#!/usr/bin/perl -w

use lib qw(.);
use CollocDB;

##==============================================================================
## test: enum

sub test_enum_create {
  my $base = shift || 'etest';
  my $enum = CollocDB::Enum->new();
  $enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->addSymbols(qw(a b c));
  $enum->close();
}
#test_enum_create(@ARGV);

sub test_enum_append {
  my $base = shift || 'etest';
  my @syms = @_ ? @_ : qw(x y z);
  my $enum = CollocDB::Enum->new();
  $enum->open($base,"ra") or die("enum->open failed: $!");
  $enum->addSymbols(@syms);
  $enum->close();
}
#test_enum_append(@ARGV);


sub test_enum_fromtext {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = CollocDB::Enum->new();
  $enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->loadTextFile($labs) or die("loadTextFile() failed for '$labs': $!");
  $enum->close();
}
#test_enum_fromtext(@ARGV);

sub test_enum_memload {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = CollocDB::Enum->new();
  #$enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->loadTextFile($labs) or die("loadTextFile() failed for '$labs': $!");
  $enum->saveDbFile($base) or die ("enum->saveDbFile() failed for '$base': $!");
}
test_enum_memload(@ARGV);


##==============================================================================
## MAIN

foreach $i (1..3) {
  print "---dummy[$i]---\n";
}
exit 0;

