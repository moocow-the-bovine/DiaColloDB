#!/usr/bin/perl -w

use lib qw(.);
use CollocDB;
use CollocDB::Utils qw(:sort);
use PDL;
use File::Path qw(make_path remove_tree);
use File::Find;
use File::Basename qw(basename);

##==============================================================================
## test: enum

sub test_enum_create {
  my $base = shift || 'etest';
  my $enum = CollocDB::Enum->new();
  $enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->addSymbols(qw(a b c));
  $enum->close();
}
#test_enum_create(@ARGV);

sub test_enum_append {
  my $base = shift || 'etest';
  my @syms = @_ ? @_ : qw(x y z);
  my $enum = CollocDB::Enum->new();
  $enum->open($base,"ra") or die("enum->open failed: $!");
  $enum->addSymbols(@syms);
  $enum->close();
}
#test_enum_append(@ARGV);


sub test_enum_text2db {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = CollocDB::Enum->new();
  $enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->loadTextFile($labs) or die("loadTextFile() failed for '$labs': $!");
  $enum->close();
}
#test_enum_text2db(@ARGV);

sub test_enum_text2mem {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = CollocDB::Enum->new();
  #$enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->loadTextFile($labs) or die("loadTextFile() failed for '$labs': $!");
}
#test_enum_text2mem(@ARGV);

sub test_enum_text2mem2db {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = CollocDB::Enum->new();
  $enum->loadTextFile($labs) or die("loadTextFile() failed for '$labs': $!");
  $enum->saveDbFile($base) or die ("enum->saveDbFile() failed for '$base': $!");
}
#test_enum_text2mem2db(@ARGV);

##==============================================================================
## test: tuple-enum + expansion-map

sub test_createdb_xtuples {
  my ($inlist,$outdir) = @_;
  !-d $outdir
    or remove_tree($outdir)
      or die("$0: could not remove old $outdir: $!");
  make_path($outdir)
    or die("$0: could not create output directory $outdir: $!");

  ##-- tuple-enum
  my $xenum = CollocDB::Enum->new();
  my $xs2i  = $xenum->{s2i}{data};
  my $xi2s  = $xenum->{i2s}{data};
  my $nx    = 0;
  my $eos   = '__$';

  ##-- corpus storage
  my $tokfile =  "$outdir/tokens.bin";
  CORE::open(my $tokfh, ">$tokfile")
    or die("$0: open failed for $tokfile: $!");
  my $tokpack = substr($PDL::Types::pack[$PDL::Types::typehash{PDL_L}{numval}],0,1);

  ##-- read input files and map to integers
  CORE::open(my $listfh, "<$inlist")
    or die("$0: open failed for input file-list $inlist: $!");
  my ($f,$date,$w,$l,$x,$xi);
  while (defined($f=<$listfh>)) {
    chomp($f);
    next if ($f=~/^\s*$/ || $f=~/^%%/);
    print STDERR "$0: processing $f ...\n";
    CORE::open(my $infh, "<$f") or die("$0: open failed for '$f': $!");
    $date = 0;
    while (defined($_=<$infh>)) {
      chomp;
      if (/^%%(?:\$DDC:meta\.date_|\$?date)=([0-9]+)/) {
	$date = $1;
      }
      next if (/^%%/);
      if (/^$/) {
	$x = "$date\t$eos\t$eos";
      } else {
	($w,$l) = (split(/\t/,$_))[0,2];
	$x = "$date\t$w\t$l";
      }
      ##-- ensure tuple
      $xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x}));

      ##-- save to token-fh for later analysis
      $tokfh->print(pack($tokpack,$xi));
    }
    CORE::close($infh);
  }

  ##-- close token storage
  $tokfh->close()
    or die("$0: failed to close token storage: $!");

  ##-- save tuple-enum
  print STDERR "$0: creating tuple enum DB $outdir/xenum.* ...\n";
  @$xi2s{values %$xs2i} = keys %$xs2i;
  $xenum->{size} = $nx;
  $xenum->saveDbFile("$outdir/xenum")
    or die("$0: failed to save $outdir/xenum: $!");

  ##-- create w2x, l2x
  print STDERR "$0: computing expansion maps w2x, l2x...\n";
  my %w2xi = qw();
  my %l2xi = qw();
  while (($x,$xi)=each %$xs2i) {
    ($w,$l) = (split(/\t/,$x,3))[1,2];
    $w2xi{$w} .= pack('N',$xi);
    $l2xi{$l} .= pack('N',$xi);
  }

  print STDERR "$0: creating $outdir/w2x.db\n";
  my $w2xdb = CollocDB::DBFile->new(file=>"$outdir/w2x.db")
    or die("$0: failed to create $outdir/w2x.db: $!");
  my $w2xdata = $w2xdb->{data};
  $w2xdata->{$_} = $w2xi{$_} foreach (sort keys %w2xi);

  print STDERR "$0: creating $outdir/l2x.db\n";
  my $l2xdb = CollocDB::DBFile->new(file=>"$outdir/l2x.db")
    or die("$0: failed to create $outdir/l2x.db: $!");
  my $l2xdata = $l2xdb->{data};
  $l2xdata->{$_} = $l2xi{$_} foreach (sort keys %l2xi);

  ##-- all done
  print STDERR "$0: finished (xtuples)\n";
  exit 0;
}
#test_createdb_xtuples(@ARGV);

sub test_createdb_ituples {
  my ($inlist,$outdir) = @_;
  !-d $outdir
    or remove_tree($outdir)
      or die("$0: could not remove old $outdir: $!");
  make_path($outdir)
    or die("$0: could not create output directory $outdir: $!");

  ##-- enums
  my $xenum = CollocDB::Enum->new();
  my $xs2i  = $xenum->{s2i}{data};
  my $nx    = 0;
  #
  my $wenum = CollocDB::Enum->new();
  my $ws2i  = $wenum->{s2i}{data};
  my $nw    = 0;
  #
  my $lenum = CollocDB::Enum->new();
  my $ls2i  = $lenum->{s2i}{data};
  my $nl    = 0;
  #
  my $eos   = '__$';

  ##-- corpus storage
  my $tokfile =  "$outdir/tokens.bin";
  CORE::open(my $tokfh, ">$tokfile")
    or die("$0: open failed for $tokfile: $!");
  my $tokpack = substr($PDL::Types::pack[$PDL::Types::typehash{PDL_L}{numval}],0,1);

  ##-- read input files and map to integers
  CORE::open(my $listfh, "<$inlist")
    or die("$0: open failed for input file-list $inlist: $!");
  my ($f,$date,$w,$l,$x,$xi,$wi,$li);
  while (defined($f=<$listfh>)) {
    chomp($f);
    next if ($f=~/^\s*$/ || $f=~/^%%/);
    print STDERR "$0: processing $f ...\n";
    CORE::open(my $infh, "<$f") or die("$0: open failed for '$f': $!");
    $date = 0;
    while (defined($_=<$infh>)) {
      chomp;
      if (/^%%(?:\$DDC:meta\.date_|\$?date)=([0-9]+)/) {
	$date = $1;
      }
      next if (/^%%/);
      if (/^$/) {
	($w,$l) = ($eos,$eos);
      } else {
	($w,$l) = (split(/\t/,$_))[0,2];
      }

      ##-- ensure symbols
      $wi = $ws2i->{$w} = ++$nw if (!defined($wi=$ws2i->{$w}));
      $li = $ls2i->{$l} = ++$nl if (!defined($li=$ls2i->{$l}));
      $xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x=pack('nNN',$date,$wi,$li)}));

      ##-- save to token-fh
      $tokfh->print(pack($tokpack,$xi));
    }
    CORE::close($infh);
  }

  ##-- close token storage
  $tokfh->close()
    or die("$0: failed to close token storage: $!");

  ##-- save: xenum
  print STDERR "$0: creating tuple enum DB $outdir/xenum.* ...\n";
  @{$xenum->{i2s}{data}}{values %$xs2i} = keys %$xs2i;
  $xenum->{size} = $nx;
  $xenum->saveDbFile("$outdir/xenum")
    or die("$0: failed to save $outdir/xenum: $!");

  ##-- save: wenum
  print STDERR "$0: creating tuple enum DB $outdir/wenum.* ...\n";
  @{$wenum->{i2s}{data}}{values %$ws2i} = keys %$ws2i;
  $wenum->{size} = $nw;
  $wenum->saveDbFile("$outdir/wenum")
    or die("$0: failed to save $outdir/wenum: $!");

  ##-- save: lenum
  print STDERR "$0: creating tuple enum DB $outdir/lenum.* ...\n";
  @{$lenum->{i2s}{data}}{values %$ls2i} = keys %$ls2i;
  $lenum->{size} = $nl;
  $lenum->saveDbFile("$outdir/lenum")
    or die("$0: failed to save $outdir/lenum: $!");

  ##-- create w2x, l2x
  print STDERR "$0: computing expansion maps w2x, l2x...\n";
  my %w2xi = qw();
  my %l2xi = qw();
  while (($x,$xi)=each %$xs2i) {
    ($wi,$li)   = unpack('@2NN',$x);
    $w2xi{$wi} .= pack('N',$xi);
    $l2xi{$li} .= pack('N',$xi);
  }

  print STDERR "$0: creating $outdir/w2x.db\n";
  my $w2xdb = CollocDB::DBFile->new(file=>"$outdir/w2x.db", pack_key=>'N')
    or die("$0: failed to create $outdir/w2x.db: $!");
  my $w2xdata = $w2xdb->{data};
  $w2xdata->{$_} = $w2xi{$_} foreach (sort {$a<=>$b} keys %w2xi);

  print STDERR "$0: creating $outdir/l2x.db\n";
  my $l2xdb = CollocDB::DBFile->new(file=>"$outdir/l2x.db", pack_key=>'N')
    or die("$0: failed to create $outdir/l2x.db: $!");
  my $l2xdata = $l2xdb->{data};
  $l2xdata->{$_} = $l2xi{$_} foreach (sort {$a<=>$b} keys %l2xi);

  ##-- all done
  print STDERR "$0: finished (ituples)\n";
  exit 0;
}
#test_createdb_ituples(@ARGV);

##==============================================================================
## test: csort

sub test_csort_sub {
  print map {"sorted: $_"} @_;
}

sub test_csort {
  my $infile = shift || 'sortme.txt';
  csort([$infile],\&test_csort_sub);
}
#test_csort(@ARGV);

##==============================================================================
## test: PackedFile

sub test_pf_create {
  my $pfile = shift || 'pf.bin';
  my $pf = CollocDB::PackedFile->new(reclen=>4,packas=>'N')
    or die("$0: failed to create CollocDB::PackedFile object: $!");
  $pf->open($pfile,'rw')
    or die("$0: failed to open '$pfile': $!");
  $pf->push($_) foreach (1..10);
  ##-- dump
  $pf->saveTextFile(\*STDOUT);
  $pf->close();
}
#test_pf_create(@ARGV); exit 0;

sub test_pf_load {
  my $pfile = shift || 'pf.bin';
  my $tfile = shift || 'pf.dat';
  my $pf = CollocDB::PackedFile->new(reclen=>4,packas=>'N')
    or die("$0: failed to create CollocDB::PackedFile object: $!");
  $pf->open($pfile,'rw')
    or die("$0: failed to open '$pfile': $!");
  $pf->loadTextFile($tfile, gaps=>1);
  $pf->saveTextFile(\*STDOUT);
  $pf->close();
}
#test_pf_load(@ARGV); exit 0;

##==============================================================================
## MAIN

foreach $i (1..3) {
  print "---dummy[$i]---\n";
}
exit 0;

