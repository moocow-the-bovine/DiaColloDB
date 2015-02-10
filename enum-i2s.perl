#!/usr/bin/perl -w

use bytes;
use Fcntl qw(SEEK_SET);

if (@ARGV < 2) {
  print STDERR "Usage: $0 ENUMBASE [INFILE(s)...]\n";
  exit 1;
}
my $ebase = shift;

##-- open enum files
open(my $sfh, "<$ebase.s")
  or die("$0: open failed for $ebase.s: $!");  ##-- pack('(n/A)*', @$i2s)
open(my $ixfh, "<$ebase.ix")
  or die("$0: open failed for $ebase.ix: $!"); ##-- [$i] => pack('N',$offset_in_sfh_of_string_with_id_i)
open(my $sxfh, "<$ebase.sx")
  or die("$0: open failed for $ebase.sx: $!"); ##-- [$j] => pack('NN',$offset_in_sfh_of_string_with_sortindex_j_and_id_i, $i)

##-- map inputs
my ($i,$rest,$buf,$off,$slen);
my $pack_off = 'N';
my $pack_len = 'n';
my $olen     = length(pack($pack_off,0));
my $llen     = length(pack($pack_len,0));
while (defined($_=<>)) {
  chomp;
  next if (/^$/);
  ($i,$rest) = split(/\t/,$_,2);

  CORE::seek($ixfh, $i*$olen, SEEK_SET)
      or die("$0: seek() failed on $ebase.ix for i=$i\n");
  CORE::read($ixfh,$buf,$olen)==$olen
      or die("$0: read() failed on $ebase.ix for i=$i\n");
  $off = unpack('N',$buf);

  CORE::seek($sfh, $off, SEEK_SET)
      or die("$0: seek() failed on $ebase.s for offset=$off\n");
  CORE::read($sfh, $buf,$llen)==$llen
      or die("$0: read() failed on $ebase.s for string length at offset=$off\n");
  $slen = unpack('n',$buf);
  CORE::read($sfh, $s, $slen)==$slen
      or die("$0: read() failed on $ebase.s for string of length $slen at offset=$off\n");

  print join("\t", $i, ($rest ? $rest : qw()), $s), "\n";
}

##-- finish up
close($sfh);
close($ixfh);
close($sxfh);

