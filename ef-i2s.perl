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

if (@ARGV < 2) {
  print STDERR <<EOF;
Usage: $0 ENUMBASE [INFILE(s)...]

Options:
   -help       # this help message
   -[no]mmap   # do/don't mmap files (default=don't)
EOF
  exit ($help ? 0 : 1);
}

##-- setup logger
CollocDB::Logger->ensureLog();

##-- open enum
my $ebase = shift;
my $ef = $efclass->new(base=>$ebase, flags=>'r')
  or die("$0: failed to create $efclass object for basename '$ebase': $!");

##-- map inputs
my ($i,$rest,$s);
while (defined($_=<>)) {
  chomp;
  next if (/^$/);
  ($i,$rest) = split(/\t/,$_,2);
  $s = $ef->i2s($i) // '';
  print join("\t", $i, ($rest ? $rest : qw()), $s), "\n";
}

##-- finish up
$ef->close()
  or die("$0: failed to close enum '$ebase': $!");
