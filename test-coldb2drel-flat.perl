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
my (@sent,$i,$wi,@tuples);
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
	 map {"$wi\t$sent[$_]\t".($_-$i)."\n"}
	 grep {$_>=0 && $_<=$#sent}
	 (($i-$n)..($i+$n))
	);
  }
  return join('',@tuples);
}

##-- writer
my $pack_f = 'N';
my $pack_i = 'N';
my $pack_dist = 'c';
my $pack_cof  = "${pack_i}${pack_i}${pack_dist}${pack_f}";  ##-- $cof : [$i1,$i2,$dist] => $f12 , where [$i1,$i1,0]==$f1
my $base = "$dbdir/cofdf${n}";
my $cof = CollocDB::PackedFile->new(file=>"$base.dba", packas=>$pack_cof, flags=>'rw')
  or die("$0: failed to create $base.dba: $!");
$cof->truncate();
my ($f12,$i1,$i2,$d12);
sub cofwrite {
  ($f12,$i1,$i2,$d12) = split(' ',$_[0],4);
  $cof->write(pack($pack_cof, $i1,$i2,$d12,$f12));
}

##-- guts
crun([qw(sort -n -k1 -k2 -k3 -k4)],
     '<', \&cofread,
     '|', [qw(uniq -c)],
     '>', new_chunker("\n"), \&cofwrite,
    );
