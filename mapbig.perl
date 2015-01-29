#!/usr/bin/perl -w

use File::Map;

sub mapbig {
  my $bigfile = shift || 'big.bin';
  my ($buf);
  system("ps -o 'pid,%mem,rss,vsz' -p $$");

  File::Map::map_file($buf, $bigfile, '<');
  system("ps -o 'pid,%mem,rss,vsz' -p $$");

  $sum = ($buf =~ /[^\0]/ ? -1 : 0);
  print "sum=$sum\n";
  system("ps -o 'pid,%mem,rss,vsz' -p $$");
}
mapbig(@ARGV); exit 0;
