#!/usr/bin/perl -w

use lib qw(.);
use CollocDB::Enum;

if (@ARGV < 2) {
  print STDERR "Usage: $0 ENUMBASE [INFILE(s)...]\n";
  exit 1;
}

##-- setup logger
CollocDB::Logger->ensureLog();

##-- open enum
my $ebase = shift;
my $ef = CollocDB::Enum->new(base=>$ebase, flags=>'r')
  or die("$0: failed to create enum '$ebase': $!");

##-- map inputs
my ($i,$rest,$s);
my $i2s = $ef->{i2s}{data};
while (defined($_=<>)) {
  chomp;
  next if (/^$/);
  ($i,$rest) = split(/\t/,$_,2);
  $s = $i2s->{$i};
  print join("\t", $i, ($rest ? $rest : qw()), $s), "\n";
}

##-- finish up
$ef->close()
  or die("$0: failed to close enum '$ebase': $!");
