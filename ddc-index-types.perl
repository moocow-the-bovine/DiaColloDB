#!/usr/bin/perl -w

use DB_File;
use Fcntl;

if (!@ARGV) {
  print STDERR <<EOF;
Usage: $0 CORPUS._INDEX
EOF
  exit 1;
}
my $dbfile = shift;

##-- open type-index file
my (@data);
my $dbflags  = O_RDONLY;
my $dbmode   = 0640;
my $dbinfo = DB_File::RECNOINFO->new();
$dbinfo->{bval} = "\0";
my $dbf = tie(@data, 'DB_File', $dbfile, $dbflags, $dbmode, $dbinfo)
  or die("$0: failed to tie() '$dbfile': $!");

##-- dump data
my ($key,$val,$st);
for ($st=$dbf->seq($key,$val,R_FIRST); $st==0; $st=$dbf->seq($key,$val,R_NEXT)) {
  print $val, "\n";
}

##-- close safely
undef $dbf;
untie @data;

