#!/usr/bin/perl -w

use lib qw(.);
use File::Map;
use PDL;
use PDL::Types;
use PDL::IO::FastRaw;
use PDL::Ngrams;
use DB_File;
use Fcntl qw(:DEFAULT SEEK_SET SEEK_END);

use CollocDB;
use CollocDB::PackedFile;

use Config;
use Benchmark qw(cmpthese timethese);

BEGIN { $,=' '; }

##--------------------------------------------------------------
sub test_types {
  use bytes;
  print map {
    my $packas=$PDL::Types::pack[$_->{numval}];
    sprintf("%-8s %2s %d\n", $_->{ioname}, $packas, length(pack($packas,0)));
  } @PDL::Types::typehash{PDL::Types::typesrtkeys};
  exit 0;
}
#test_types();

##--------------------------------------------------------------
sub test0 {
  my @ptypes = PDL::Types->ppdefs(); ##-- CONTINUE HERE: find pdl integer type with size==4
  my $file  = 'mapme.bin';
  my $p = mapfraw($file, {ReadOnly=>1, Creat=>0, Dims=>[((-s $file)/4)], Datatype=>long});
  print $p->slice("0:10"), "\n";
}
#test0();

##--------------------------------------------------------------
sub test_b2l {
  ##-- convert char(4,N) to long(N)
  my $pb = sequence(byte, 4,2);
  my $pl = zeroes(long, $pb->dim(1));
  ${$pl->get_dataref} = ${$pb->get_dataref};
  $pl->upd_data;
  return ($pb,$pl);
}
#test_b2l();

##--------------------------------------------------------------
sub test_mmap {
  my ($pb,$pl)=test_b2l();
  ##-- mmap a file
  my $pmfile = 'map.bin';
  #open(my $fh,">:raw", $pmfile) or die("$0: failed to truncate $pmfile: $!"); undef $fh;
  my $pm     = mapfraw($pmfile, {ReadOnly=>0, Creat=>1, Dims=>[$pl->dims], Datatype=>$pl->type});
  $pm .= $pl;
}
#test_mmap();

##--------------------------------------------------------------
sub storage2pdl_mapfraw_l {
  my $file = shift;
  my $size = (-s $file);
  my $p    = mapfraw($file, {ReadOnly=>1, Creat=>0, Dims=>[$size/4], Datatype=>PDL::long});
  return $p;
}

##--------------------------------------------------------------
sub storage2pdl_mapfraw_c {
  my $file = shift;
  my $size = (-s $file);
  my $p    = mapfraw($file, {ReadOnly=>1, Creat=>0, Dims=>[4,($size/4)], Datatype=>PDL::byte});
  return $p;
}

##--------------------------------------------------------------
sub storage2pdl_mapfraw_cl {
  my $file = shift;
  my $pc   = storage2pdl_mapfraw_c($file);
  my $pl   = zeroes(long, $pc->dim(1));
  if ($Config{byteorder} =~ /^1234/) {
    $pl |= $pc->slice("(0),");
    $pl |= ($pc->slice("(1),")->long << 8);
    $pl |= ($pc->slice("(2),")->long << 16);
    $pl |= ($pc->slice("(3),")->long << 24);
  }
  elsif ($Config{byteorder} =~ /4321$/) {
    $pl |= ($pc->slice("(0),")->long << 24);
    $pl |= ($pc->slice("(1),")->long << 16);
    $pl |= ($pc->slice("(2),")->long << 8);
    $pl |= $pc->slice("(3),");
  }
  else {
    die("$0: can't handle machine byte-order '$Config{byteorder}'");
  }
  return $pl;
}

##--------------------------------------------------------------
sub storage2pdl_mapfraw_S {
  my $file = shift;
  my $size = (-s $file);
  my $p    = mapfraw($file, {ReadOnly=>1, Creat=>0, Dims=>[2,($size/4)], Datatype=>PDL::ushort});
  return $p;
}

##--------------------------------------------------------------
sub storage2pdl_mapfraw_Sl {
  my $file = shift;
  my $pc   = storage2pdl_mapfraw_S($file);
  my $pl   = zeroes(long, $pc->dim(1));
  if ($Config{byteorder} =~ /^1234/) {
    $pl |= $pc->slice("(0),");
    $pl |= ($pc->slice("(1),")->long << 16);
  }
  elsif ($Config{byteorder} =~ /4321$/) {
    $pl |= ($pc->slice("(0),")->long << 16);
    $pl |= $pc->slice("(1),");
  }
  else {
    die("$0: can't handle machine byte-order '$Config{byteorder}'");
  }
  return $pl;
}

##--------------------------------------------------------------
sub storage2pdl_map_unpack {
  my $file = shift || 'mapme.bin';
  my ($buf);
  File::Map::map_file($buf, $file, '<')
      ;#or die("$0: map_file() failed for '$file': $!");
  my $p = pdl(long, [unpack('L*', $buf)]);
  return $p;
}


##--------------------------------------------------------------
sub test_storage2pdl {
  my $file = shift || 'mapme.bin';
  my $pl = storage2pdl_mapfraw_l($file);
  my $pc = storage2pdl_mapfraw_c($file);
  my $pcl = storage2pdl_mapfraw_cl($file);
  my $pS = storage2pdl_mapfraw_S($file);
  my $pSl = storage2pdl_mapfraw_Sl($file);
  my $plu = storage2pdl_map_unpack($file);
  exit 0;
}
#test_storage2pdl(@ARGV);

##--------------------------------------------------------------
sub bench_storage2pdl {
  my $file = shift || 'mapme.bin';
  print STDERR "$0: benchmarking storage2pdl_*() routines for file $file...\n";
  timethese(-1,
	   {
	    "mapfraw_l"=>sub { storage2pdl_mapfraw_l($file)->minmax },
	    #"mapfraw_c"=>sub { storage2pdl_mapfraw_c($file)->minmax },
	    #"mapfraw_cl"=>sub { storage2pdl_mapfraw_cl($file)->minmax },
	    #"mapfraw_S"=>sub { storage2pdl_mapfraw_S($file)->minmax },
	    "mapfraw_Sl"=>sub { storage2pdl_mapfraw_Sl($file)->minmax },
	    "map+unpack"=>sub { storage2pdl_map_unpack($file)->minmax },
	   }
	  );
  exit 0;
}
#bench_storage2pdl(@ARGV);

##--------------------------------------------------------------
sub test_ngrams {
  my $file = shift || 'mapme.bin';

  print STDERR "$0: mapping input pdl...\n";
  my $pl = storage2pdl_mapfraw_l($file);

  foreach my $n (1..3) {
    print STDERR "$0: computing $n-grams...\n";
    my ($f,$fw) = ng_cofreq( $pl->slice("*${n},") );
    $f->writefraw("$file.f$n");
    $fw->writefraw("$file.f${n}w");
  }

  print STDERR "$0: done\n";
  return;
}
#test_ngrams(@ARGV);

##--------------------------------------------------------------
sub test_map_unigrams {
  my $file = shift || 'mapme.bin';
  print STDERR "map:in\n";
  my $size = (-s $file);
  my $p    = mapfraw($file, {ReadOnly=>1, Creat=>0, Dims=>[$size/4], Datatype=>PDL::long});

  print STDERR "ng_cofreq\n";
  my ($f,$fw) = ng_cofreq($p->slice("*1,"));

  print STDERR "map:out\n";
  my $ofile = "$file.ug";
  my $opdl  = mapfraw($ofile, {Creat=>1, Dims=>[$fw->max+1], Datatype=>PDL::long});
  $opdl->index($fw->flat) .= $f;

  print STDERR "done.\n";
}
#test_map_unigrams(@ARGV);

##==============================================================
## test: packed array lookup

sub palookup_open_db {
  my $dbfile = shift || 'kern01.wl.d/xf.dba';
  my $data = [];
  my $dbflags  = O_RDONLY;
  my $dbmode   = 0640;
  my $dbinfo   = DB_File::RECNOINFO->new();
  $dbinfo->{reclen} = 4;
  $dbinfo->{flags} |= R_FIXEDLEN;
  my $db = tie(@$data, 'DB_File', $dbfile, $dbflags, $dbmode, $dbinfo)
    or die("$0: failed to tie db-file '$dbfile': $!");
  $db->filter_fetch_value(sub { $_=unpack('N',$_); });
  return $data;
}

sub palookup_items {
  my $itemfile = shift || 'kern01.xi-10';
  open(my $fh,"<$itemfile") or die("$0: open failed for $itemfile: $!");
  my $items = [map {chomp;$_+0} <$fh>];
  close $fh;
  return $items;
}

sub test_palookup_db {
  my ($dbfile,$itemfile) = @_;
  my $data  = palookup_open_db($dbfile);
  my $items = palookup_items($itemfile);
  my ($val);
  foreach (@$items) {
    $val = $data->[$_];
    #print $_, "\t", $val, "\n";
  }
  print STDERR "test_palookup_db\n";
  system("ps -o 'pid,%mem,rss,vsz' -p $$");
  #
  # full lookup (2534912 items):
  #   PID %MEM   RSS    VSZ
  # 30462  7.3 299484 316532
  #
  # real	0m23.451s : 108k op/sec
}
#test_palookup_db(@ARGV); exit 0;

sub palookup_open_mmap {
  my $dbfile = shift || 'kern01.wl.d/xf.dba';
  my ($buf);
  File::Map::map_file($buf, $dbfile, '<')
      ;#or die("$0: map_file() failed for $dbfile: $!");
  return \$buf;
}
sub test_palookup_mmap {
  my ($dbfile,$itemfile) = @_;
  my $data  = palookup_open_mmap($dbfile);
  my $items = palookup_items($itemfile);
  my ($val);
  foreach (@$items) {
    $val = vec($$data, $_, 32);
    #print $_, "\t", $val, "\n";
  }
  print STDERR "test_palookup_mmap\n";
  system("ps -o 'pid,%mem,rss,vsz' -p $$");
  #
  # full lookup (2534912 items):
  #   PID %MEM   RSS    VSZ
  # 30417  7.6 308448 326052
  #
  # real	0m2.110s : 1.2M op/sec
}
#test_palookup_mmap(@ARGV); exit 0;

##--------------------------------------------------------------
package Tie::File::Packed;
use Tie::Array;
use IO::File;
use Carp qw(confess);
use Fcntl;
use strict;
our @ISA = qw(Tie::Array);

## PACKAGE->TIEARRAY($file,$mode,%opts)
##  %opts:
##    pack=>$pack_sub,     ##-- operates on $_
##    unpack=>$unpack_sub, ##-- operates on $_
##    reclen=>$reclen,
##    mode=>$openmode,
##    size=>$size,         ##-- number of records
sub TIEARRAY {
  my ($that,$file,$mode,%opts) = @_;
  $mode //= '<';
  my $tied = bless({
		    'file'=>$file,
		    'mode'=>$mode,
		    'size'=>undef,
		    'pack'=>undef,
		    'unpack'=>undef,
		    'reclen'=>undef,
		    %opts,
		   }, (ref($that)||$that));
  confess(__PACKAGE__, "::TIEARRAY(): reclen not specified!") if (!$tied->{reclen});

  $tied->{fh} = ref($file) ? $tied->{file} : IO::File->new("$tied->{mode}$file");
  confess(__PACKAGE__, "::TIEARRAY(): open failed for file '$file': $!") if (!$tied->{fh});

  $tied->{size} = (-s $tied->{fh}) / $tied->{reclen};
  return $tied;
}

## $val = $tied->FETCH($index)
sub FETCH {
  seek($_[0]{fh}, $_[1]*$_[0]{reclen}, SEEK_SET)
    or return undef;
  local ($_);
  read($_[0]{fh}, $_, $_[0]{reclen})==$_[0]{reclen}
    or confess(__PACKAGE__, "::FETCH(): failed to fetch item at index $_[1]: $!");
  $_[0]{unpack}->() if (defined($_[0]{unpack}));
  return $_;
}

## undef = $tied->STORE($index,$value)
sub STORE {
  seek($_[0]{fh}, $_[1]*$_[0]{reclen}, SEEK_SET)
    or confess(__PACKAGE__, "::STORE(): cannot seek() to index position $_[1]: $!");
  local $_ = $_[1];
  $_[0]->{pack}->() if (defined($_[0]{pack}));
  $_[0]{fh}->print($_);
  return $_[1];
}

## $count = $tied->FETCHSIZE()
sub FETCHSIZE {
  return $_[0]{size};
}

## $tied->STORESIZE($count)
sub STORESIZE {
  if ($_[1] > $_[0]{size}) {
    ##-- grow
    seek($_[0]{fh}, $_[1]*$_[0]{reclen}-1, SEEK_SET)
      or confess(__PACKAGE__, "::STORESIZE() failed to grow file to $_[1] elements: $!");
    $_[0]{fh}->print("\0");
  }
  else {
    ##-- shrink
    truncate($_[0]{fh}, $_[1]*$_[0]{reclen})
      or confess(__PACKAGE__, "::STORESIZE() failed to shrink file to $_[1] elements: $!");
  }
  return $_[0]{size}=$_[1];
}

## undef = $tied->EXTEND($count)
sub EXTEND {
  $_[0]->STORESIZE($_[1]);
}

## $bool = $tied->EXISTS($index)
sub EXISTS {
  return ($_[1] < $_[0]{size});
}

## undef = $tied->DELETE($index)
sub DELETE {
  $_[0]->STORE($_[1], pack("C$_[0]{reclen}"));
}

## undef = $tied->CLEAR()
##  + optional
sub CLEAR {
  $_[0]->STORESIZE(0);
}

1;
package main;

##--------------------------------------------------------------
sub palookup_open_fp {
  my $dbfile = shift || 'kern01.wl.d/xf.dba';
  my $data   = [];
  tie(@$data, 'Tie::File::Packed', $dbfile, '<', reclen=>4, unpack=>sub {$_=unpack('N',$_)})
    or die("$0: tie() failed for $dbfile: $!");
  return $data;
}
sub test_palookup_fp {
  my ($dbfile,$itemfile) = @_;
  my $data  = palookup_open_fp($dbfile);
  my $items = palookup_items($itemfile);
  my ($val);
  foreach (@$items) {
    $val = $data->[$_];
    print $_, "\t", $val, "\n";
  }
  print STDERR "test_palookup_fp\n";
  system("ps -o 'pid,%mem,rss,vsz' -p $$");
  ##
  # full lookup (2534912 items):
  #   PID %MEM   RSS    VSZ
  # 30337  7.3 298512 316176
  #
  # real	0m15.589s : 163 op/sec
}
#test_palookup_fp(@ARGV); exit 0;

##--------------------------------------------------------------
sub palookup_open_pf {
  my $dbfile = shift || 'kern01.wl.d/xf.dba';
  my $data   = [];
  tie(@$data, 'CollocDB::PackedFile', $dbfile, '<', reclen=>4, packas=>'N')
    or die("$0: tie() failed for $dbfile: $!");
  return $data;
}
sub test_palookup_pf {
  my ($dbfile,$itemfile) = @_;
  my $data  = palookup_open_pf($dbfile);
  my $items = palookup_items($itemfile);
  my ($val);
  foreach (@$items) {
    $val = $data->[$_];
    #print $_, "\t", $val, "\n";
  }
  print STDERR "test_palookup_pf\n";
  system("ps -o 'pid,%mem,rss,vsz' -p $$");
  ##
  # full lookup (2534912 items):
  #   PID %MEM   RSS    VSZ
  # 30337  7.3 298512 316176
  #
  # real	0m15.589s : 163 op/sec
}
#test_palookup_pf(@ARGV); exit 0;


##--------------------------------------------------------------
sub bench_palookup {
  my ($dbfile,$itemfile) = @_;
  #my $data_db = palookup_open_db($dbfile);
  my $data_mm = palookup_open_mmap($dbfile);
  my $data_fp = palookup_open_fp($dbfile);
  my $data_pf = palookup_open_pf($dbfile);
  my $items = palookup_items($itemfile);
  my ($val);
  cmpthese(-1,
	   {
	    #'db'   => sub { $val=$data_db->[$_] foreach (@$items); },
	    'mmap' => sub { $val=vec($$data_mm,$_,32) foreach (@$items); },
	    'fp'   => sub { $val=$data_fp->[$_] foreach (@$items); },
	    'pf'   => sub { $val=$data_pf->[$_] foreach (@$items); },
	   });
}
bench_palookup(@ARGV); exit 0;

##==============================================================
## MAIN

print STDERR "MAIN: ok\n";
exit 0;
