#!/usr/bin/perl -w

use File::Map;
use PDL;
use PDL::Types;
use PDL::IO::FastRaw;
use PDL::Ngrams;

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
test_map_unigrams(@ARGV);

##==============================================================
## MAIN

print STDERR "ok\n";
exit 0;
