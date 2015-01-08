#!/usr/bin/perl -w

use DB_File;
use Fcntl;
use Getopt::Long qw(:config no_ignore_case);

our ($help);
our $separator = "\n";
GetOptions(
	   'help|h' => \$help,
	   'separator|sep|s=s' => \$separator,
	   'newlines|n!' => sub { $separator=$_[1] ? "\n" : "\0" },
	   'binary|bin|b!' => sub { $separator=$_[1] ? "\0" : "\n" },
	  );

if (!@ARGV) {
  print STDERR <<EOF;
Usage: $0 CORPUS._INDEX [ID_FILE(s)...]
EOF
  exit 1;
}
my $dbfile = shift;

##-- open type-index file
my (@data);
my $dbflags  = O_RDONLY;
my $dbmode   = 0640;
my $dbinfo = DB_File::RECNOINFO->new();
$dbinfo->{bval} = $separator;
my $dbf = tie(@data, 'DB_File', $dbfile, $dbflags, $dbmode, $dbinfo)
  or die("$0: failed to tie() '$dbfile': $!");

##-- apply
my ($i,$a_in,$a_dict);
my $include_empty = 1;
while (defined($_=<>)) {
  chomp;
  next if (/^%%/ || /^$/);
  chomp;
  ($i,$a_in) = split(/\t/,$_,2);
  $dbf->get($i, $a_dict);
  $_         = join("\t", $i, (defined($a_in) ? $a_in : qw()), (defined($a_dict) && ($include_empty || $a_dict ne '') ? $a_dict : qw()))."\n";
} continue {
  print $_ or last;
}

##-- close safely
undef $dbf;
untie @data;
