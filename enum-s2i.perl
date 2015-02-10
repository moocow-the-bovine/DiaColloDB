#!/usr/bin/perl -w

use bytes;
use Fcntl qw(SEEK_SET);

if (@ARGV < 2) {
  print STDERR "Usage: $0 ENUMBASE [INFILE(s)...]\n";
  exit 1;
}
my $ebase = shift;

##======================================================================
## Globals

my $pack_off = 'N';
my $pack_id  = 'N';
my $pack_len = 'n';
my $pack_sx  = $pack_off.$pack_id;
my $olen     = length(pack($pack_off,0));
my $ilen     = length(pack($pack_id,0));
my $llen     = length(pack($pack_len,0));
my $sxlen    = $olen+$ilen;

##======================================================================
## Files

##-- open enum files
open(my $sfh, "<$ebase.s")
  or die("$0: open failed for $ebase.s: $!");  ##-- pack('(n/A)*', @$i2s)
open(my $ixfh, "<$ebase.ix")
  or die("$0: open failed for $ebase.ix: $!"); ##-- [$i] => pack('N',$offset_in_sfh_of_string_with_id_i)
open(my $sxfh, "<$ebase.sx")
  or die("$0: open failed for $ebase.sx: $!"); ##-- [$j] => pack('NN',$offset_in_sfh_of_string_with_sortindex_j_and_id_i, $i)

##======================================================================
## subs

## $i_or_undef = sxfind($key, $ilo,$ihi)
sub sxfind {
  my ($key,$ilo,$ihi) = @_;
  $ilo //= 0;
  $ihi //= (-s $sxfh) / $sxlen;
  my ($imid,$buf,$soff,$si);
  while ($ilo < $ihi) {
    $imid = ($ihi+$ilo) >> 1;

    ##-- get sx-record @ $imid
    CORE::seek($sxfh, $imid*$sxlen, SEEK_SET)
	or die("$0: seek() failed on $ebase.sx for item $imid");
    CORE::read($sxfh, $buf, $olen)==$olen
	or die("$0: read() failed on $ebase.sx for item $imid");
    $soff = unpack($pack_off, $buf);

    ##-- get string for sx-record
    CORE::seek($sfh, $soff, SEEK_SET)
	or die("$0: seek() failed on $ebase.s for offset $soff");
    CORE::read($sfh, $buf, $llen)==$llen
	or die("$0: read() failed on $ebase.s for string length at offset $soff\n");
    $slen = unpack($pack_len, $buf);
    CORE::read($sfh, $buf, $slen)==$slen
	or die("$0: read() failed on $ebase.s for string of length $slen at offset $soff\n");

    if ($buf lt $key) {
      $ilo = $imid + 1;
    } else {
      $ihi = $imid;
    }
  }

  ##-- output
  if ($ilo==$ihi) {
    ##-- get sx-record @ $ilo
    CORE::seek($sxfh, $ilo*$sxlen, SEEK_SET)
	or die("$0: seek() failed on $ebase.sx for item $ilo");
    CORE::read($sxfh, $buf, $sxlen)==$sxlen
	or die("$0: read() failed on $ebase.sx for item $ilo");
    ($soff,$si) = unpack($pack_sx, $buf);

    ##-- get string for sx-record
    CORE::seek($sfh, $soff, SEEK_SET)
	or die("$0: seek() failed on $ebase.s for offset $soff");
    CORE::read($sfh, $buf, $llen)==$llen
	or die("$0: read() failed on $ebase.s for string length at offset $soff\n");
    $slen = unpack($pack_len, $buf);
    CORE::read($sfh, $buf, $slen)==$slen
	or die("$0: read() failed on $ebase.s for string of length $slen at offset $soff\n");

    return $si if ($buf eq $key);
  }
  return undef;
}

##======================================================================
## MAIN

##-- map inputs
my ($s,$rest,$i);
while (defined($_=<>)) {
  chomp;
  next if (/^$/);
  ($s,$rest) = split(/\t/,$_,2);
  $i = sxfind($s) // -1;

  print join("\t", $s, ($rest ? $rest : qw()), $i), "\n";
}

##-- finish up
close($sfh);
close($ixfh);
close($sxfh);

