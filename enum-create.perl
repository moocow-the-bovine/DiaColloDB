#!/usr/bin/perl -w

use bytes;

if (@ARGV < 2) {
  print STDERR "Usage: $0 INFILE OUTBASE\n";
  exit 1;
}
my ($infile,$ebase) = @ARGV;

##-- load input file
my $i2s = [];
open(my $infh,"<$infile")
  or die("$0: open failed for input file '$infile': $!");
my ($i,$s);
while (defined($_=<$infh>)) {
  chomp;
  next if (/^$/);
  ($i,$s) = split(' ',$_,2);
  $i2s->[$i] = $s;
}
close($infh);

##-- write output file
open(my $sfh, ">$ebase.s")
  or die("$0: open failed for $ebase.s: $!");  ##-- pack('(n/A)*', @$i2s)
open(my $ixfh, ">$ebase.ix")
  or die("$0: open failed for $ebase.ix: $!"); ##-- [$i] => pack('N',$offset_in_sfh_of_string_with_id_i)
open(my $sxfh, ">$ebase.sx")
  or die("$0: open failed for $ebase.sx: $!"); ##-- [$j] => pack('NN',$offset_in_sfh_of_string_with_sortindex_j_and_id_i, $i)

my $off   = 0;
my $i2off = []; ##-- >[$i] => $offset
my $pack_off = 'N';
my $pack_id  = 'N';
my $pack_len = 'n';
my $olen     = length(pack($pack_off,0));
my $llen     = length(pack($pack_len,0));
my $ilen     = length(pack($pack_id,0));
foreach (@$i2s) {
  $_ //= '';
  $sfh->print(pack("${pack_len}/A", $_));
  $ixfh->print(pack($pack_off,$off));
  push(@$i2off, $off);
  $off += $llen + length($_);
}
close($sfh);
close($ixfh);

##-- now dump sorted keys for binsearch
foreach $i (sort {$i2s->[$a] cmp $i2s->[$b]} (0..$#$i2s)) {
  $sxfh->print(pack($pack_off.$pack_id, $i2off->[$i], $i));
}
close($sxfh);


