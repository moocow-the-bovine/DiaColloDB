#!/usr/bin/perl -w

use lib qw(.);
use CollocDB::EnumFile;

if (@ARGV < 2) {
  print STDERR "Usage: $0 INFILE OUTBASE\n";
  exit 1;
}
my ($infile,$ebase) = @ARGV;

##-- setup logger
CollocDB::Logger->ensureLog();


my $ef = CollocDB::EnumFile->new(base=>$ebase, flags=>'rw')
  or die("$0: failed to create enum '$base': $!");
$ef->loadTextFile($infile)
  or die("$0: failed to load '$infile': $!");
$ef->flush()
  or die("$0: failed to flush enum '$base': $!");

$ef->close()
  or die("$0: failed to close enum '$base': $!");

