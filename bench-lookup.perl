#!/usr/bin/perl -w

use Benchmark qw(cmpthese timethese);
use open qw(:std :utf8);
use strict;

##======================================================================
## Globals
#our $ntypes = 2000000;
our $ntypes = 100000;
our $nvals  =      4;
our $nkeys  =    100;
srand(0);

BEGIN { select(STDERR); $|=1; select(STDOUT); }

##======================================================================
## Subs

## \@types = loadTypes($typfile)
## IGNORED
sub loadTypes {
  my $tfile = shift;
  open(my $fh, "<$tfile")
    or die("$0: loadTypes(): open failed for type-file '$tfile': $!");
  binmode($fh,':utf8');
  my (%types);
  my ($w,$rest);
  while (defined($_=<$fh>)) {
    chomp;
    next if (/^$/ || /^%%/);
    ($w,$rest) = split(/\t/,$_,2);
    next if (exists($types{$w}));
    $types{$w} = undef;
  }
  close($fh) or die("$0: close failed for type-file '$tfile': $!");
  return [sort keys %types];
}


##======================================================================
## MAIN

##-- create dummy data
print STDERR "$0: testing $nkeys key-lookups over $ntypes dummy values of length (0:$nvals)\n";

print STDERR "$0: creating values... ";
my %w2v_s  = qw(); ##-- ("$wi" => join(' ', %v2f), ... )
my %w2v_p  = qw(); ##-- (pack('L',$wi) => pack('L*', %v2f), ... )
my %w2v_sp = qw(); ##-- ("$wi" => pack('L*', %v2f), ... )

my @wi = (0..($ntypes-1));
my ($wi,$nv,%v2f);
foreach $wi (@wi) {
  $nv  = int(rand($nvals));
  %v2f = map {(rand($ntypes)=>0)} (1..$nv);
  $w2v_s{$wi} = join(' ', %v2f);
  $w2v_p{pack('L',$wi)} = pack("(LL)[$nv]",%v2f);
  $w2v_sp{$wi} = pack("(LL)[$nv]",%v2f);
}
%v2f = qw();
print STDERR " created.\n";

##-- get keys
my @keys = map {int(rand($ntypes)).""} (1..$nkeys);

##-- bench
cmpthese(-3, {
	      'lookup_s'=>sub {%v2f=split(' ',$w2v_s{$_}) foreach (@keys);},
	      'lookup_p'=>sub {%v2f=unpack('L*', $w2v_p{pack('L',$_)}) foreach (@keys);},
	      'lookup_sp'=>sub {%v2f=unpack('L*', $w2v_sp{$_}) foreach (@keys);},
	     });

