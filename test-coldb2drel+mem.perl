#!/usr/bin/perl -w

use lib qw(.);
use CollocDB;
use CollocDB::Utils qw(:sort :run :env);
use Getopt::Long;
use DB_File;
use Fcntl;
use strict;

##======================================================================
## Command-line
our ($help);
our $outbase = undef;
our $n = 1;
our $use_bdb = 0;

GetOptions(##-- general
	   'help|h' => \$help,

	   ##-- I/O
	   'length|width|n=i' => \$n,
	   'bdb|db!' => \$use_bdb,
	  );

if (!@ARGV || $help)  {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DBDIR

 Options:
   -help
   -n N

EOF
  exit 1;
}

our $dbdir = shift(@ARGV);
CollocDB->ensureLog();

##======================================================================
## MAIN

##-- output base
my $base = "$dbdir/cofdm${n}";

##-- reader
my $tokfile = "$dbdir/tokens.dat";
open(my $tokfh, "<$tokfile")
  or die("$0: open failed for '$tokfile': $!");
binmode($tokfh,':raw');

my $pack_i = 'N'; ##-- must be network order, otherwise sort()-trick won't work!
my $pack_f = 'N';
my $pack_dist = 'c';
my $pack_key  = $pack_i.$pack_i.$pack_dist;

my (@sent,$w,$i,$wi);
my %cf = qw();
my $dbfile = "$base.dbtmp";
if ($use_bdb) {
  my $dbflags  = O_RDWR|O_CREAT|O_TRUNC;
  my $dbmode   = 0640;
  my $dbinfo   = DB_File::BTREEINFO->new();
  tie(%cf, 'DB_File', $dbfile, $dbflags, $dbmode, $dbinfo);
}

while (1) {
  if (!defined($w=<$tokfh>) || $w=~/^$/) {
    ##-- eos
    foreach $i (0..$#sent) {
      $wi = $sent[$i];
      foreach (grep {$_>=0 && $_<=$#sent && $_ != $i} (($i-$n)..($i+$n))) {
	++$cf{pack($pack_key, $wi, $sent[$_], ($_-$i))};
      }
    }
    last if (!defined($w));
    @sent = qw();
    next;
  }
  chomp($w);
  push(@sent,$w);
}

##-- dump
my $pack_r1 = "${pack_i}${pack_f}";              ##-- $r1 : [$i1] => [$pos2,$f1]
my $pack_r2 = "${pack_i}${pack_dist}${pack_f}";  ##-- $r2 : [$i2,$d12,$f12] in range $pos2[$i1]..($pos2[$i1+1]-1)
my $r1 = CollocDB::PackedFile->new(file=>"$base.dba1", packas=>$pack_r1, flags=>'rw')
  or die("$0: failed to create $base.dba1: $!");
my $r2 = CollocDB::PackedFile->new(file=>"$base.dba2", packas=>$pack_r2, flags=>'rw')
  or die("$0: failed to create $base.dba2: $!");
$r1->truncate();
$r2->truncate();
my ($pos1,$pos2) = (0,0);
my ($i1_cur,$f1_cur,$pos2_cur) = (-1,0);
my ($f12,$i1,$i2,$d12);
my ($key);
foreach $key (sort keys %cf) {
  ($i1,$i2,$d12) = unpack($pack_key, $key);
  $f12 = $cf{$key};
  if ($i1 != $i1_cur) {
    if ($i1_cur != -1) {
      ##-- dump record for $i1_cur
      if ($i1_cur != $pos1++) {
	$r1->seek($i1_cur);
	$pos1 = $i1_cur+1;
      }
      $r1->write(pack($pack_r1, $pos2_cur,$f1_cur));
    }
    $i1_cur   = $i1;
    $f1_cur   = 0;
    $pos2_cur = $pos2;
  }

  ##-- track marginal f($i1)
  $f1_cur += $f12;

  ##-- dump record to $r2
  $r2->write(pack($pack_r2, $i2,$d12,$f12));
  ++$pos2;
}

##-- dump final record for $i1_cur
if ($i1_cur != -1) {
  $r1->seek($i1_cur) if ($i1_cur != $pos1++);
  $r1->write(pack($pack_r1, $pos2_cur,$f1_cur));
}

##-- untie and unlink
if ($use_bdb) {
  untie(%cf);
  unlink($dbfile) if (-e $dbfile);
}

print STDERR "$0 memory usage:\n";
system("ps -o 'pid,%mem,rss,vsz' -p $$");
