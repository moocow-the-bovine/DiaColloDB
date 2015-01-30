#!/usr/bin/perl -w

use lib qw(.);
use CollocDB;
use CollocDB::Utils qw(:sort :run);
use IPC::Run qw(run new_chunker);
use Getopt::Long;
use strict;

##======================================================================
## Command-line
our ($help);
our $outbase = undef;
our $n = 1;

GetOptions(##-- general
	   'help|h' => \$help,

	   ##-- I/O
	   'length|width|n=i' => \$n,
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

sub test0 {
  my $returned=0;
  crun(['cat'],
       '<', sub { return undef if ($returned); $returned=1; return join('',map {"$_\n"} (1..10)); },
      );
  exit 0;
}
#test0();

##-- reader
my $tokfile = "$dbdir/tokens.dat";
open(my $tokfh, "<$tokfile")
  or die("$0: open failed for '$tokfile': $!");
binmode($tokfh,':raw');
my (@sent,$i,$j,$wi,$wj,@tuples);
sub cofread {
  ##-- read the next sentence
  return undef if (CORE::eof($tokfh));
  @sent = qw();
  while (defined($_=<$tokfh>)) {
    chomp;
    last if (/^$/ );
    push(@sent,$_);
  }
  return '' if (!@sent);

  ##-- get tuples
  @tuples = qw();
  foreach $i (0..$#sent) {
    $wi = $sent[$i];
    push(@tuples,
	 map {"$wi\t$sent[$_]\n"}
	 grep {$_>=0 && $_<=$#sent && $_ != $i}
	 (($i-$n)..($i+$n))
	);
  }
  return join('',@tuples);
}

##-- writer
my $pack_f = 'N';
my $pack_i = 'N';
my $pack_r1 = "${pack_i}${pack_f}";  ##-- $r1 : [$i1] => [$pos2,$f1]
my $pack_r2 = "${pack_i}${pack_f}";  ##-- $r2 : [$i2,$f12] in range $pos2[$i1]..($pos2[$i1+1]-1)
my $base = "$dbdir/cof${n}";
my $r1 = CollocDB::PackedFile->new(file=>"$base.dba1", packas=>$pack_r1, flags=>'rw') ##-- [$i1] => [pos2($i1),f($i1)]
  or die("$0: failed to create $base.dba1: $!");
my $r2 = CollocDB::PackedFile->new(file=>"$base.dba2", packas=>$pack_r2, flags=>'rw') ##-- [$i2,$f12] in range pos2($i1)..(pos2($i1+1)-1)
  or die("$0: failed to create $base.dba2: $!");
$r1->truncate();
$r2->truncate();
my ($pos1,$pos2) = (0,0);
my ($i1_cur,$f1_cur,$pos2_cur) = (-1,0);
my ($f12,$i1,$i2);
sub cofwrite {
  ($f12,$i1,$i2) = split(' ',$_[0],3);
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
  $r2->write(pack($pack_r2, $i2,$f12));
  ++$pos2;
}
sub coffinish {
  ##-- dump final record for $i1_cur
  if ($i1_cur != -1) {
    $r1->seek($i1_cur) if ($i1_cur != $pos1++);
    $r1->write(pack($pack_r1, $pos2_cur,$f1_cur));
  }
}

##-- guts
crun([qw(sort -n -k1 -k2)],
     '<', \&cofread,
     '|', [qw(uniq -c)],
     '>', new_chunker("\n"), \&cofwrite,
    );
coffinish();

