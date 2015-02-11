#!/usr/bin/perl -w

use lib qw(.);
use CollocDB::EnumFile;
use CollocDB::EnumFile::MMap;
use Getopt::Long qw(:config no_ignore_case);

my ($help);
my $efclass = 'CollocDB::EnumFile';
GetOptions(
	   'help|h' => \$help,
	   'mmap|map|m!'  => sub { $efclass='CollocDB::EnumFile'.($_[1] ? "::MMap" : ''); },
	  );

if ($help || @ARGV < 2) {
  print STDERR <<EOF;
Usage: $0 [OPTIONS] INFILE OUTBASE

Options:
   -help       # this help message
   -[no]mmap   # do/don't mmap files (default=don't)
EOF
  exit ($help ? 0 : 1);
}
my ($infile,$ebase) = @ARGV;

##-- setup logger
CollocDB::Logger->ensureLog();

my $ef = $efclass->new(base=>$ebase, flags=>'rw')
  or die("$0: failed to create $efclass object for basename '$base': $!");
$ef->loadTextFile($infile)
  or die("$0: failed to load '$infile': $!");
$ef->flush()
  or die("$0: failed to flush enum '$base': $!");

$ef->close()
  or die("$0: failed to close enum '$base': $!");

