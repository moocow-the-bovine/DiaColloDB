#!/usr/bin/perl -w

use lib qw(. dclib ./blib/lib ./blib/arch);
use DiaColloDB;
use DiaColloDB::Utils qw(:sort :regex);
use PDL;
use File::Path qw(make_path remove_tree);
use File::Find;
use File::Basename qw(basename);
use Time::HiRes qw(gettimeofday tv_interval);
use JSON;
use Data::Dumper;
use Benchmark qw(timethese cmpthese);
use Fcntl qw(:seek);
use Test::More;
use utf8;

#use DiaColloDB::Relation::TDF; ##-- DEBUG
BEGIN {
  select STDERR; $|=1; select STDOUT; $|=1;
  $, = ' ';
}

##==============================================================================
## test: enum

sub test_enum_create {
  my $base = shift || 'etest';
  my $enum = DiaColloDB::Enum->new();
  $enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->addSymbols(qw(a b c));
  $enum->close();
}
#test_enum_create(@ARGV);

sub test_enum_append {
  my $base = shift || 'etest';
  my @syms = @_ ? @_ : qw(x y z);
  my $enum = DiaColloDB::Enum->new();
  $enum->open($base,"ra") or die("enum->open failed: $!");
  $enum->addSymbols(@syms);
  $enum->close();
}
#test_enum_append(@ARGV);


sub test_enum_text2db {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = DiaColloDB::Enum->new();
  $enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->loadTextFile($labs) or die("loadTextFile() failed for '$labs': $!");
  $enum->close();
}
#test_enum_text2db(@ARGV);

sub test_enum_text2mem {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = DiaColloDB::Enum->new();
  #$enum->open($base,"rw") or die("enum->open failed: $!");
  $enum->loadTextFile($labs) or die("loadTextFile() failed for '$labs': $!");
}
#test_enum_text2mem(@ARGV);

sub test_enum_text2mem2db {
  my $base = shift || 'etest';
  my $labs = shift || "$base.lab";
  my $enum = DiaColloDB::Enum->new();
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
  my $xenum = DiaColloDB::Enum->new();
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
  my $w2xdb = DiaColloDB::DBFile->new(file=>"$outdir/w2x.db")
    or die("$0: failed to create $outdir/w2x.db: $!");
  my $w2xdata = $w2xdb->{data};
  $w2xdata->{$_} = $w2xi{$_} foreach (sort keys %w2xi);

  print STDERR "$0: creating $outdir/l2x.db\n";
  my $l2xdb = DiaColloDB::DBFile->new(file=>"$outdir/l2x.db")
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
  my $xenum = DiaColloDB::Enum->new();
  my $xs2i  = $xenum->{s2i}{data};
  my $nx    = 0;
  #
  my $wenum = DiaColloDB::Enum->new();
  my $ws2i  = $wenum->{s2i}{data};
  my $nw    = 0;
  #
  my $lenum = DiaColloDB::Enum->new();
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
  my $w2xdb = DiaColloDB::DBFile->new(file=>"$outdir/w2x.db", pack_key=>'N')
    or die("$0: failed to create $outdir/w2x.db: $!");
  my $w2xdata = $w2xdb->{data};
  $w2xdata->{$_} = $w2xi{$_} foreach (sort {$a<=>$b} keys %w2xi);

  print STDERR "$0: creating $outdir/l2x.db\n";
  my $l2xdb = DiaColloDB::DBFile->new(file=>"$outdir/l2x.db", pack_key=>'N')
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
  my $pf = DiaColloDB::PackedFile->new(reclen=>4,packas=>'N')
    or die("$0: failed to create DiaColloDB::PackedFile object: $!");
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
  my $pf = DiaColloDB::PackedFile->new(reclen=>4,packas=>'N')
    or die("$0: failed to create DiaColloDB::PackedFile object: $!");
  $pf->open($pfile,'rw')
    or die("$0: failed to open '$pfile': $!");
  $pf->loadTextFile($tfile, gaps=>1);
  $pf->saveTextFile(\*STDOUT);
  $pf->close();
}
#test_pf_load(@ARGV); exit 0;

##==============================================================================
## bench: file size


sub bench_filesize {
  my $file = shift || "kern01.wl.d/xf.dba";
  my $pf   = DiaColloDB::PackedFile->new(file=>$file,flags=>'r',packas=>'N');
  $pf->{size} = $pf->size();
  my ($size);
  cmpthese(1000000,
	   {
	    #'-s:FILE' => sub { $size = (-s $file); },
	    #'-s:FH'   => sub { $size = (-s $pf->{fh}); },
	    '{size}'  => sub { $size = $pf->{size}; },
	   });
  #               Rate -s:FILE   -s:FH  {size}
  # -s:FILE  2085235/s      --    -47%    -84%  : real=1.040/M ~ 961k op/sec [incl overhead]
  # -s:FH    3957601/s     90%      --    -69%  : real=0.808/M ~ 124k op/sec [incl overhead]
  # {size}  12952997/s    521%    227%      --  : real=0.627/M ~ 159k op/sec [incl overhead]
}
#bench_filesize(@ARGV); exit 0;

##==============================================================================
## bench: substr+unpack

## $s = subunpack1(\$buf, $offset)
sub subunpack1 {
  return unpack('n/A', substr(${$_[0]}, $_[1]));
}

## $s = subunpack2(\$buf, $offset)
sub subunpack2 {
  my $len = unpack('n', substr(${$_[0]}, $_[1], 2));
  return substr(${$_[0]}, $_[1]+2, $len);
}

## $sub = subunpack2c()
## $s = $sub->(\$buf,$offset);
sub subunpack2c {
  my ($len);
  return sub {
    $len = unpack('n', substr(${$_[0]}, $_[1], 2));
    return substr(${$_[0]}, $_[1]+2, $len);
  };
}

sub bench_subunpack {
  my $sfile = shift || 'lef.s';

  print STDERR "$0: loading $sfile ...\n";
  open(my $sfh,"<$sfile") or die("$0: open failed for $sfile: $!");
  read($sfh, (my $sbuf), (-s $sfh));
  close($sfh);

  use bytes;
  print STDERR "$0: computing offsets ...\n";
  my @i2s = unpack("(n/A)*", $sbuf);
  my @offs = qw();
  my $off = 0;
  foreach (@i2s) {
    push(@offs, $off);
    $off += length($_) + 2;
  }

  print STDERR "$0: benchmarking ...\n";
  my $size   = scalar(@i2s);
  my $nbench = 100;
  my @boffs  = map {int(rand($size))} (1..$nbench);
  my ($s);
  my $subunpack2c = subunpack2c();
  cmpthese(-1,
	   {
	    subunpack1  => sub { $s=subunpack1(\$sbuf,$_) foreach (@boffs); },
	    subunpack2  => sub { $s=subunpack2(\$sbuf,$_) foreach (@boffs); },
	    subunpack2c => sub { $s=$subunpack2c->(\$sbuf,$_) foreach (@boffs); },
	    #               Rate  subunpack1 subunpack2c  subunpack2
	    # subunpack1  11.0/s          --       -100%       -100%
	    # subunpack2c 2669/s      24162%          --         -0%
	    # subunpack2  2669/s      24162%          0%          --
	   });
}
#bench_subunpack(@ARGV);

##==============================================================================
## test: enum loaded

sub test_enum_loaded {
  DiaColloDB::Logger->ensureLog();
  my $ebase = shift || "corpus1.d/lenum";
  my $ef    = DiaColloDB::EnumFile->new(base=>$ebase)
    or die("$0: could not create EnumFile for $ebase: $!");

  $ef->info("toArray()");
  my $ea1   = $ef->toArray();

  $ef->info("load()");
  $ef->load()
    or die("$0: could not load enum: $!");
  $ef->info("loaded: ", ($ef->loaded ? "ok" : "NOT ok"));
  $ef->info("dirty: ", ($ef->dirty ? "NOT ok" : "ok"));

  my $ea2 = $ef->toArray();
  my $ea3 = $ef->toArray();
  $ef->info("cached array: ", ($ea2 eq $ea3 ? 'ok' : 'NOT ok'));

  $ef->info("addSymbols()");
  $ef->addSymbols(qw(foo bar));
  $ef->info("loaded: ", ($ef->loaded ? "ok": "NOT ok"));
  $ef->info("dirty: ", ($ef->dirty ? "ok": "NOT ok"));
  my $ea4 = $ef->toArray();
  $ef->info("dirty array: ", ($ea4 eq $ea3 ? 'ok' : 'NOT ok'));

  exit 0;
}
#test_enum_loaded(@ARGV);

##==============================================================================
## test: enum expand

sub test_enum_expand {
  DiaColloDB::Logger->ensureLog();
  my $re    = shift || '/[[:upper:]]nderung/u';
  my $ebase = shift || "corpus1.d/lenum";
  my ($eclass);
  #$eclass = 'DiaColloDB::EnumFile';
  $eclass = 'DiaColloDB::EnumFile::MMap';
  #$eclass = 'DiaColloDB::EnumFile::FixedLen';
  #$eclass = 'DiaColloDB::EnumFile::FixedLen::MMap';
  my $ef    = $eclass->new(base=>$ebase)
    or die("$0: could not create $eclass object for $ebase: $!");

  my $is = $ef->re2i($re, '@4n');
  print map {
    ($ef->{pack_s} ? join("\t", unpack($ef->{pack_s},$ef->i2s($_))) : $ef->i2s($_))."\n"
  } @$is;

  exit 0;
}
#test_enum_expand(@ARGV);

##==============================================================================
## test: profile io

use Storable;
sub test_profile_io0 {
  my $dbdir = shift || 'kern01.d';
  my $lemma = shift || 'Mann';

  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("$0: failed to open DB-directory $dbdir: $!");
  my $mp = $coldb->profile2(lemma=>$lemma, slice=>10, kbest=>50, score=>'ld');

  Storable::nstore($mp,"mp.bin");
  exit 0;
}
#test_profile_io0(@ARGV);

sub mp2s_storable {
  my $mp = shift;
  my $buf;
  open(my $fh, ">", \$buf) or die("mp2s_storable(): open failed for string buffer: $!");
  Storable::nstore_fd($mp, $fh);
  close($fh);
  return $buf;
}

sub mp2s_text {
  my $mp = shift;
  my $buf;
  open(my $fh, ">", \$buf) or die("mp2s_text(): open failed for string buffer: $!");
  $mp->saveTextFile($fh);
  close($fh);
  return $buf;
}

BEGIN {
  our $jxs;
}
sub jxs {
  $jxs //= JSON->new->utf8(1)->allow_nonref(1)->allow_blessed(1)->convert_blessed(1)->pretty(0)->canonical(0);
  return $jxs;
}

sub mp2s_json_raw {
  return jxs()->encode($_[0]);
}

sub p2array {
  my $prf = shift;
  my @tab = qw();
  my ($f1,$f2,$f12) = @$prf{qw(f1 f2 f12)};
  my $fscore = $prf->{$prf->{score}//'f12'};
  foreach (keys %$fscore) {
    push(@tab, [$f2->{$_},$f12->{$_},$fscore->{$_},$_]);
  }
  return \@tab;
}

sub mp2s_json_flat {
  my $mp  = shift;
  my ($p);
  return jxs->encode({map {$p=$mp->{ps}{$_}; ($_=>[@$p{qw(N f1 score)},p2array($p)])} keys %{$mp->{ps}}});
}

sub mp2s_json_raw2 {
  return to_json($_[0], {utf8=>1,allow_nonref=>1,allow_blessed=>1,convert_blessed=>1,pretty=>0,canonical=>0});
}
sub mp2s_json_flat2 {
  my $mp = shift;
  my ($p);
  return to_json({map {$p=$mp->{ps}{$_}; ($_=>[@$p{qw(N f1 score)},p2array($p)])} keys %{$mp->{ps}}},
		 {utf8=>1,allow_nonref=>1,allow_blessed=>1,convert_blessed=>1,pretty=>0,canonical=>0});
}


sub mp2s_pack {
  my $mp  = shift;
  my $buf = '';
  my ($key,$p,$key2,$f2,$f12,$scoref,$scoreh);
  while (($key,$p)=each(%{$mp->{ps}})) {
    $scoref = $p->{score} // '';
    $scoreh = $p->{$scoref};
    $buf   .= pack('(n/A)NN(n/A)', $key, @$p{qw(N f1)}, $scoref);
    while (($key2,$f12)=each(%{$p->{f12}})) {
      $buf .= pack('(n/A)NNf', $key2, $p->{f2}{$key2}, $f12, ($scoreh ? $scoreh->{$key2} : 0));
    }
  }
  return $buf;
}

sub mp2s_packraw {
  my $mp  = shift;
  my $ps  = $mp->{ps};
  my ($p,$f2,$f12);
  return join('',
	     map {
	       $p=$ps->{$_};
	       ($f2,$f12) = @$p{qw(f2 f12)};
	       pack('(n/A)NNN((n/A)NN)*',
		    $_, @$p{qw(N f1)}, scalar(keys %$f12),
		    map {
		      ($_, $f2->{$_}, $f12->{$_})
		    } keys %$f12)
	     }
	      keys %{$mp->{ps}}
	     );
}


sub bench_profile_io {
  my $mpbin = shift || 'mp.bin';

  my $mp = Storable::retrieve($mpbin)
    or die("$0: Storable::retrieve() failed for file '$mpbin': $!");

  use bytes;
  my %cf = (
	    storable  => {code=>\&mp2s_storable},
	    text      => {code=>\&mp2s_text},
	    json_raw  => {code=>\&mp2s_json_raw},
	    json_flat => {code=>\&mp2s_json_flat},
	    json_raw2 => {code=>\&mp2s_json_raw2},
	    json_flat2 => {code=>\&mp2s_json_flat2},
	    pack      => {code=>\&mp2s_pack},
	    packraw   => {code=>\&mp2s_packraw},
	   );
  $cf{$_}{label} = $_ foreach (keys %cf);
  foreach (values %cf) {
    $_->{str} = $_->{code}->($mp);
    $_->{len} = length($_->{str});
    open(my $fh, ">$mpbin.$_->{label}") or die("$0: open failed for $mpbin.$_->{label}: $!");
    print $fh $_->{str};
    close $fh;
  }
  #@data = unpack('((n/A)NN(n/A)((n/A)NNf)*)*', $cf{pack}{str})
  my $nrecs    = @{[ ($cf{text}{str} =~ /\n/sg) ]};
  my $fmt = "# %-12s\t%4d bytes / $nrecs records = %5.1f bytes/record\n";
  print STDERR sprintf($fmt, $_, $cf{$_}{len}, $cf{$_}{len}/$nrecs) foreach (sort keys %cf);
  # json_flat   	6359 bytes / 150 records =  42.4 bytes/record
  # json_flat2  	6359 bytes / 150 records =  42.4 bytes/record
  # json_raw    	8683 bytes / 150 records =  57.9 bytes/record
  # json_raw2   	8835 bytes / 150 records =  58.9 bytes/record
  # pack        	2941 bytes / 150 records =  19.6 bytes/record
  # packraw     	2341 bytes / 150 records =  15.6 bytes/record
  # storable    	8733 bytes / 150 records =  58.2 bytes/record
  # text        	6539 bytes / 150 records =  43.6 bytes/record

  my %cmpus = map {my $c=$_; ($c->{label}=>sub {$c->{code}->($mp)})} values %cf;
  cmpthese(-3, \%cmpus);
  #               Rate text pack json_flat2 json_flat packraw storable json_raw2 json_raw
  # text        3147/s   -- -37%       -38%      -40%    -54%     -58%      -82%     -84%
  # pack        4990/s  59%   --        -2%       -6%    -27%     -34%      -71%     -75%
  # json_flat2  5071/s  61%   2%         --       -4%    -26%     -33%      -71%     -74%
  # json_flat   5286/s  68%   6%         4%        --    -23%     -30%      -70%     -73%
  # packraw     6827/s 117%  37%        35%       29%      --      -9%      -61%     -65%
  # storable    7542/s 140%  51%        49%       43%     10%       --      -57%     -62%
  # json_raw2  17357/s 452% 248%       242%      228%    154%     130%        --     -12%
  # json_raw   19783/s 529% 296%       290%      274%    190%     162%       14%       --

  exit 0;
}
#bench_profile_io(@ARGV);

##==============================================================================
## bench: profile trimming

sub bench_profile_trim {
  my $dbdir = shift || 'kern01.d';
  my $lemma = shift || 'Mann';
  my $score = shift || 'ld';

  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("$0: failed to open DB-directory $dbdir: $!");
  my $mp0 = $coldb->profile2(lemma=>$lemma, slice=>0, kbest=>0, score=>$score, strings=>0);
  my $p0  = $mp0->{data}{(sort {$a<=>$b} keys %{$mp0->{data}})[0]};

  my $p1=$p0->clone(1)->trim(kbest=>10)->saveTextFile('-');
  print "--\n";
  my $p2=$p0->clone(1)->trim(keep=>$p0->which(kbest=>10))->saveTextFile(\*STDOUT);

  cmpthese(-3, {
		'clone'=>sub{my $p=$p0->clone(1);},
		'trim'=>sub {my $p=$p0->clone(1); $p->trim(kbest=>10);},
		'which+trim:keep'=>sub{my $p=$p0->clone(1); $p->trim(keep=>$p->which(kbest=>10));},
		'which+trim:drop'=>sub{my $p=$p0->clone(1); $p->trim(drop=>$p->which(kbest=>10,return=>'bad'));},
	       });
  #                   Rate which+trim:keep which+trim:drop         trim        clone
  # which+trim:keep 35.8/s              --             -8%         -19%         -84%
  # which+trim:drop 39.0/s              9%              --         -12%         -83%
  # trim            44.2/s             23%             13%           --         -81%
  # clone            227/s            534%            482%         413%           --

  exit 0;
}
#bench_profile_trim(@ARGV);

##==============================================================================
## test: profile algebra

sub save_profile_binop {
  my ($op,$mp1,$mp2,$mp3) = @_;
  foreach my $key (sort {$a<=>$b} keys %{$mp3->{data}}) {
    my ($p1,$p2,$p3) = ($mp1->{data}{$key},$mp2->{data}{$key},$mp3->{data}{$key});
    print join("\t",
	       "N:$p1->{N}${op}$p2->{N}=$p3->{N}",
	       "f1:$p1->{f1}${op}$p2->{f1}=$p3->{f1}")."\n";
    my $score = $p3->{score} // 'f12';
    foreach my $i2 (sort {$p3->{$score}{$b}<=>$p3->{$score}{$a}} keys %{$p3->{$score}}) {
      print join("\t",
		 '',$i2,
		 map {"$_:".($p1->{$_}{$i2}//0).$op.($p2->{$_}{$i2}//0).'='.$p3->{$_}{$i2}}
		 (qw(f2 f12),grep {defined($p3->{$_})} $p3->scoreKeys),
		)."\n";
    }
  }
}

sub test_profile_add {
  my $dbdir = shift || 'kern01.d';
  my $lemma1 = shift || 'Mann';
  my $lemma2 = shift || 'Frau';
  my $score = shift || 'ld';

  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("$0: failed to open DB-directory $dbdir: $!");
  my $mp1 = $coldb->profile2(lemma=>$lemma1, slice=>0, kbest=>10, score=>$score);
  my $mp2 = $coldb->profile2(lemma=>$lemma2, slice=>0, kbest=>10, score=>$score);

  my $mp_add = $mp1->add($mp2,N=>0)->compile($score);
  save_profile_binop('+',$mp1,$mp2,$mp_add);

  exit 0;
}
#test_profile_add(@ARGV);

sub test_profile_diff {
  my $dbdir = shift || 'kern01.d';
  my $lemma1 = shift || 'Mann';
  my $lemma2 = shift || 'Frau';
  my $score = shift || 'ld';

  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("$0: failed to open DB-directory $dbdir: $!");
  my $mp1 = $coldb->profile2(lemma=>$lemma1, slice=>0, kbest=>0, score=>$score, strings=>0);
  my $mp2 = $coldb->profile2(lemma=>$lemma2, slice=>0, kbest=>0, score=>$score, strings=>0);

  ##-- trim
  my %dkeys = map {($_=>undef)} keys(%{$mp1->{data}}), keys(%{$mp2->{data}});
  foreach my $d (keys %dkeys) {
    my ($p1,$p2) = ($mp1->{data}{$d},$mp2->{data}{$d});
    my %pkeys = map {($_=>undef)} (($p1 ? @{$p1->which(kbest=>10)} : qw()), ($p2 ? @{$p2->which(kbest=>10)} : qw()));
    $p1->trim(keep=>\%pkeys)->stringify($coldb->{lenum}) if ($p1);
    $p2->trim(keep=>\%pkeys)->stringify($coldb->{lenum}) if ($p2);
  }

  my $mp_diff = $mp1->diff($mp2,N=>0);
  save_profile_binop('-',$mp1,$mp2,$mp_diff);

  exit 0;
}
#test_profile_diff(@ARGV);

##==============================================================================
## bench: profile-multi

## (\@lemmata,\@dbs) = bench_profile_multi_load($lfile,$dbglob)
sub bench_profile_multi_load {
  my $lfile  = shift || 'lf-100.dat';
  my $dbglob = shift || 'kern01.d';
  my @dbdirs = glob($dbglob);

  ##-- open dbdirs
  DiaColloDB::Logger->ensureLog(
				level=>'INFO'
			       );
  my @coldbs = map {
    DiaColloDB->new(dbdir=>$_, logProfile=>'off')
      or die("$0: open failed for dbdir $_: $!");
  } @dbdirs;
  die("$0: no dbs!") if (!@coldbs);

  ##-- load lemma file
  open(my $lfh,"<$lfile") or die("$0: open failed for $lfile: $!");
  binmode($lfh,':utf8');
  my @lemmas = map {chomp; s/^[0-9]+\t//; $_} grep {defined($_) && $_ !~ /^\s*$/} <$lfh>;
  close($lfh);

  return (\@lemmas,\@coldbs);
}

sub bench_profile_multi {
  my $lfile  = shift || 'lf-100.dat';
  my $dbglob = shift || 'kern01.d';
  my ($lemmas,$coldbs) = bench_profile_multi_load($lfile,$dbglob);

  ##-- bench profile
  my %popts = (kbest=>10, slice=>0, score=>'ld');
  my ($l,$mp,$mpi);
  my $t0 = [gettimeofday];
  foreach $l (@$lemmas) {
    $mp = undef;
    foreach (@$coldbs) {
      $mpi = $_->profile2(lemma=>$l, %popts) or next;
      $mp  = defined($mp) ? $mp->_add($mpi) : $mpi;
    }
    $mp->compile($popts{score})->trim(%popts) if (defined($mp));
  }
  my $t1 = [gettimeofday];
  $_->close() foreach (@$coldbs);

  ##-- report
  my $elapsed = tv_interval($t0,$t1);
  my $nl      = @$lemmas;
  printf ("  # %-10s @ {%-12s}  : got %d profile(s) in %.4f seconds ~ %.2f secs/op\n", $lfile,$dbglob,$nl,$elapsed,$elapsed/$nl);
  # lf-100.dat @ {kern01.d} : got 10 profile(s) in 3.4193 seconds ~ 0.34 secs/op
  # lf-1k.dat  @ {kern01.d} : got 10 profile(s) in 5.4863 seconds ~ 0.55 secs/op
  # lf-10k.dat @ {kern01.d} : got 10 profile(s) in 5.1056 seconds ~ 0.51 secs/op
  # lftest.dat @ {kern01.d} : got 30 profile(s) in 9.0231 seconds ~ 0.30 secs/op
  ##
  # lf-100.dat @ {kern.d      } : got 10 profile(s) in 11.6816 seconds ~ 1.17 secs/op
  # lf-100.dat @ {kern0[1-4].d} : got 10 profile(s) in 15.3844 seconds ~ 1.54 secs/op (+31.6%)
  ##
  # lf-1k.dat  @ {kern.d      } : got 10 profile(s) in 16.6947 seconds ~ 1.67 secs/op
  # lf-1k.dat  @ {kern0[1-4].d} : got 10 profile(s) in 20.1486 seconds ~ 2.01 secs/op (+20.4%)
  ##
  # lf-10k.dat @ {kern.d      } : got 10 profile(s) in 18.7411 seconds ~ 1.87 secs/op
  # lf-10k.dat @ {kern0[1-4].d} : got 10 profile(s) in 21.8675 seconds ~ 2.19 secs/op (+17.1%)
  ##
  # lftest.dat @ {kern.d      } : got 30 profile(s) in 30.7756 seconds ~ 1.03 secs/op
  # lftest.dat @ {kern0[1-4].d} : got 30 profile(s) in 39.0718 seconds ~ 1.30 secs/op (+26.2%)
  ##
  ## (threaded)
  # lftest.dat @ {kern.d      }T : got 30 profile(s) in 32.0691 seconds ~ 1.07 secs/op
  # lftest.dat @ {kern0[1-4].d}T : got 30 profile(s) in 41.0114 seconds ~ 1.37 secs/op
  ##
  ##-- 2nd run
  # lftest.dat @ {kern.d      }  : got 30 profile(s) in 1.4780 seconds ~ 0.05 secs/op
  # lftest.dat @ {kern0[1-4].d}  : got 30 profile(s) in 1.5600 seconds ~ 0.05 secs/op (+ 5.5%)
  ##
  # lftest.dat @ {kern.d      }T : got 30 profile(s) in 2.6317 seconds ~ 0.09 secs/op
  # lftest.dat @ {kern0[1-4].d}T : got 30 profile(s) in 3.8586 seconds ~ 0.13 secs/op (+46.1%)
  exit 0;
}
#bench_profile_multi(@ARGV);

sub bench_profile_multi_threads {
  my $lfile  = shift || 'lf-100.dat';
  my $dbglob = shift || 'kern01.d';
  my ($lemmas,$coldbs) = bench_profile_multi_load($lfile,$dbglob);

  use threads;
  use threads::shared;

  ##-- bench profile
  my %popts = (kbest=>10, slice=>0, score=>'ld');
  my ($l,$mp,@threads,$mpi);
  my $t0 = [gettimeofday];
  foreach $l (@$lemmas) {
    $mp = undef;
    @threads = map {
      my $coldb = $_;
      threads->create(sub { $^W=0; $coldb->profile2(lemma=>$l,%popts); })
    } @$coldbs;
    foreach (@threads) {
      $mpi = $_->join() or next;
      $mp  = defined($mp) ? $mp->_add($mpi) : $mpi;
    }
    $mp->compile($popts{score})->trim(%popts) if (defined($mp));
  }
  my $t1 = [gettimeofday];
  $_->close() foreach (@$coldbs);

  ##-- report
  my $elapsed = tv_interval($t0,$t1);
  my $nl      = @$lemmas;
  printf ("  # %-10s @ {%-12s}T : got %d profile(s) in %.4f seconds ~ %.2f secs/op\n", $lfile,$dbglob,$nl,$elapsed,$elapsed/$nl);
}
#bench_profile_multi_threads(@ARGV);

##==============================================================================
## test: client

sub test_client_profile {
  my $lemma = shift || 'Frau';
  #my @urls = @_ ? @_ : glob('kern0[1-4].d');
  #my @urls = @_ ? @_ : ('http://localhost/~moocow/diacollo');
  #my @urls = @_ ? @_ : ('http://localhost/~moocow/diacollo', glob("kern0[2-4].d"));
  my @urls = @_ ? @_ : ('list://http://localhost/~moocow/diacollo');

  DiaColloDB::Logger->ensureLog();
  my $cli = DiaColloDB::Client->new((@urls==1 ? $urls[0] : \@urls), opts=>{user=>'taxi',password=>'tsgpw',logRequest=>'debug'})
    or die("$0: failed to create client for URLs ", join(' ',@urls), ": $!");
  my $mp = $cli->profile2(lemma=>$lemma, slice=>0, kbest=>10, score=>'ld')
    or die("$0: failed to retrieve profile for '$lemma': $cli->{error}");
  $cli->close();
  $mp->saveTextFile(\*STDOUT);

  exit 0;
}
#test_client_profile(@ARGV);

sub test_client_diff {
  my $alemma = shift || 'Mann';
  my $blemma = shift || 'Frau';

  #my @urls = @_ ? @_ : glob('kern0[1-4].d');
  my @urls = @_ ? @_ : ('http://localhost/~moocow/diacollo', glob("kern0[2-4].d"));

  DiaColloDB::Logger->ensureLog();
  my $cli = DiaColloDB::Client->new(\@urls, opts=>{user=>'taxi',password=>'tsgpw',logRequest=>'debug'})
    or die("$0: failed to create client for URLs ", join(' ',@urls), ": $!");
  my $mp = $cli->compare2(alemma=>$alemma, blemma=>$blemma, slice=>0, kbest=>10, score=>'ld')
    or die("$0: failed to retrieve diff for '$alemma'-'$blemma': $cli->{error}");
  $mp->saveTextFile(\*STDOUT);

  exit 0;
}
#test_client_diff(@ARGV);

##==============================================================================
## bench: set intersection

## (\@a,\@b) = makesets($N, $na,$nb, $nab)
sub makesets {
  my ($N,$na,$nb,$nboth) = @_;
  $nboth //= 0;
  $na -= $nboth;
  $nb -= $nboth;
  my %a  = map {(int(rand($N))=>undef)} (1..($na > 0 ? $na : 0));
  my %b  = map {(int(rand($N))=>undef)} (1..($nb > 0 ? $nb : 0));
  my %ab = map {(int(rand($N))=>undef)} (1..$nboth);
  @a{keys %ab} = undef;
  @b{keys %ab} = undef;
  return ([keys %a],[keys %b]);
}

sub intersect_l {
  my @l1 = sort {$a<=>$b} @{$_[0]};
  my @l2 = sort {$a<=>$b} @{$_[1]};
  my @l  = qw();
  my $i2 = 0;
 i1:
  foreach $e1 (@l1) {
  i2:
    for (; $i2 <= $#l2; ++$i2) {
      last if ($l2[$i2] >= $e1);
    }
    push(@l,$e1) if (($l2[$i2]//-1)==$e1);
  }
  return \@l;
}

sub intersect_h {
  return {map {($_=>undef)} grep {exists($_[1]{$_})} keys %{$_[0]}};
}

sub intersect_lh {
  my ($l1,$l2) = @_;
  ($l1,$l2) = ($l2,$l1) if ($#$l2 < $#$l1);
  my %h1 = (map {($_=>undef)} @$l1);
  return [grep {exists $h1{$_}} @$l2];
}

sub bench_intersect {
  my ($N,$na,$nb,$nboth) = @_;
  $N     ||= 6000000;
  $na    ||= 100;
  $nb    ||= 100;
  $nboth ||= 10;

  #my $l1 = [qw(84 11 64 95 94 14 48 52 30 62)];
  #my $l2 = [qw(84 11 21 70 14 18 46 89 55)];
  ###-- intersection: qw(11 14 84)

  my ($l1,$l2) = makesets($N,$na,$nb,$nboth);
  my $h1 = {map {($_=>undef)} @$l1};
  my $h2 = {map {($_=>undef)} @$l2};

  my $l12_l  = intersect_l($l1,$l2);
  my $l12_lh = intersect_lh($l1,$l2);
  my $l12_h = intersect_h($h1,$h2);

  print STDERR "$0: benchmarking N=$N, na=$na, nb=$nb, nboth=$nboth\n";
  cmpthese(-3,
	   {
	    'intersect_l' => sub { intersect_l($l1,$l2) },
	    'intersect_lh' => sub { intersect_lh($l1,$l2) },
	    'intersect_h' => sub { intersect_h($h1,$h2) },
	   });
  # ./testme.perl: benchmarking N=6000000, na=100, nb=100, nboth=10
  #                 Rate  intersect_l intersect_lh  intersect_h
  # intersect_l  10273/s           --         -57%         -78%
  # intersect_lh 24127/s         135%           --         -47%
  # intersect_h  45662/s         344%          89%           --
  ##
  # ./testme.perl: benchmarking N=6000000, na=100, nb=10000, nboth=10
  #                 Rate  intersect_l intersect_lh  intersect_h
  # intersect_l    182/s           --         -84%        -100%
  # intersect_lh  1151/s         533%           --         -97%
  # intersect_h  44797/s       24538%        3793%           --

  exit 0;
}
#bench_intersect(@ARGV);

##==============================================================================
## bench: binary encoding

use MIME::Base64 qw(encode_base64 decode_base64);
sub bench_binencode {
  my $bin = pack('N*', 0..1023);

  ##-- encode
  my $e_uu  = pack('u',$bin);
  my $e_64  = encode_base64($bin);

  ##-- decode
  my $d_uu = unpack('u',$e_uu);
  my $d_64 = decode_base64($e_64);

  ##-- check
  die("$0: uuencoding failed") if ($d_uu ne $bin);
  die("$0: base64 failed") if ($d_64 ne $bin);

  my ($tmp);
  cmpthese(-3,
	   {
	    'uuencode' => sub { $tmp=unpack('u',pack('u',$bin)); },
	    'base64'   => sub { $tmp=decode_base64(encode_base64($bin,'')); },
	   });
  #             Rate uuencode   base64
  # uuencode 28822/s       --     -54%
  # base64   63047/s     119%       --

  exit 0;
}
#bench_binencode();


##==============================================================================
## bench: integer sorting

sub bench_isort {
  my ($N,$len) = @_;
  $N //= 6000000;
  $n //= 1000;

  my @l = map {int(rand($N))." ".int(rand(42))} (1..$n);

  my ($tmp);
  no warnings 'numeric';
  cmpthese(-3,
	   {
	    '<=>'     => sub { $tmp=[sort {$a<=>$b} @l] },
	    'cmp'     => sub { $tmp=[sort {$a cmp $b} @l]; },
	    'len+cmp' => sub { $tmp=[sort {(length($a)<=>length($b)) || ($a cmp $b)} @l]; },
	   });
  ## plain integers:
  #           Rate len+cmp     cmp     <=>
  # len+cmp  726/s      --    -73%    -82%
  # cmp     2730/s    276%      --    -32%
  # <=>     4022/s    454%     47%      --

  ## "$i1 $i2" strings:
  # len+cmp  731/s      --    -69%    -77%
  # cmp     2327/s    218%      --    -26%
  # <=>     3130/s    328%     34%      --

  exit 0;
}
#bench_isort();

##==============================================================================
## debug: taz buggy enum

sub debug_enum {
  my $efile = shift || 'taz.d/l_enum';
  my $enum  = DiaColloDB::EnumFile::MMap->new(base=>$efile);

  ##-- variables
  use bytes;
  my ($sbufr,$sxbufr,$ixbufr) = @$enum{qw(sbufr sxbufr ixbufr)};
  my ($pack_l,$len_l,$pack_i,$len_i,$pack_o,$len_o,$len_sx) = @$enum{qw(pack_l len_l pack_i len_i pack_o len_o len_sx)};

  ##-- read sx records
  DiaColloDB->ensureLog();
  $enum->debug("reading sx records");
  my ($sx_off,$o,$i);
  my $sx_size = length($$sxbufr);
  my $pack_sx = $pack_o.$pack_i;
  my @i2sx    = qw(); ##-- $i2sx[$i] = [$off,$si]
  for ($sx_off=0; $sx_off < $sx_size; $sx_off += $len_sx) {
    ($o,$i)   = unpack($pack_sx,substr($$sxbufr,$sx_off,$len_sx));
    $i2sx[$i] = [$o,$sx_off/$len_sx];
  }
  $enum->debug(scalar(@i2sx), " sx-records loaded");

  ##-- check ix records
  $enum->debug("checking sx<->ix consistency");
  my $ix_size = length($$ixbufr);
  my ($ix_off,$i_off, $sx);
  for ($ix_off=0; $ix_off < $ix_size; $ix_off += $len_o) {
    $i     = $ix_off / $len_o;
    $i_off = unpack($pack_o, substr($$ixbufr, $ix_off, $len_o));

    $sx = $i2sx[$i];
    if ($sx->[0] != $i_off) {
      die("$0: sx<->ix offset mismatch for i=$i, sxi=$sx->[1]: sx-offset=$sx->[0] != ix-offset=$i_off");
    }
  }
  $enum->debug(($ix_size/$len_o)." ix-records checked out ok");

  ##-- check s records
  $enum->debug("checking s<->sx consistency");
  my $s_size  = length($$sbufr);
  my $n_s     = 0;
  my ($s_off,$s_len,$s_buf);
  for ($s_off=0, $i=0; $s_off < $s_size; ++$i) {
    $s_len = unpack($pack_l,substr($$sbufr,$s_off,$len_l));
    $s_buf = substr($$sbufr,$s_off+$len_l,$s_len);
    $s_buf = substr($s_buf,0,16)."..." if (length($s_buf) > 16);

    ##-- check for offset mismatch
    $sx = $i2sx[$i];
    if ($sx->[0] != $s_off) {
      die("$0: s<->sx offset mismatch for i=$i, sxi=$sx->[1], s='$s_buf': sx-offset=$sx->[0] != s-offset=$s_off");
    }

    ##-- update
    $s_off += $len_l + $s_len;
    ++$n_s;
  }
  $enum->debug("$n_s s-records checked out ok");

  $enum->debug("enum $enum->{base} appears consistent");
  exit 0;
}
#debug_enum(@ARGV);


sub debug_churn_enum_a {
  my $efile = shift || 'taz.d/l_enum';
  my $ofile = shift || 'tmp.aenum';
  my $enum  = DiaColloDB::EnumFile::MMap->new(base=>$efile);

  my $i2s   = $enum->toArray;
  my $e2    = $enum->new->fromArray($i2s);
  $e2->save($ofile);
  exit 0;
}
#debug_churn_enum_a(@ARGV);

sub debug_churn_enum_h {
  my $efile = shift || 'taz.d/l_enum';
  my $ofile = shift || 'tmp.henum';
  my $enum  = DiaColloDB::EnumFile::MMap->new(base=>$efile);

  $enum->load();
  my $s2i = $enum->{s2i};
  my $e2  = $enum->new->fromHash($s2i);
  $e2->save($ofile);
  exit 0;
}
#debug_churn_enum_h(@ARGV);

##==============================================================================
## test: tied enums

sub test_tied_enum {
  my $ebase = shift || "corpus1.d/p_enum";
  DiaColloDB->ensureLog();

  my ($i2s,$s2i) = DiaColloDB::EnumFile->tiepair(base=>$ebase, class=>'DiaColloDB::EnumFile::MMap');
  print "--new; i2s~", ${tied(@$i2s)}, " ; s2i~", ${tied(%$s2i)}, "}--\n";
  #print Data::Dumper->Dump([$i2s,$s2i], [qw(i2s s2i)]), "\n";

  if (0) {
    print "--untie(i2s)--\n";
    untie(@$i2s);
    print Data::Dumper->Dump([$i2s,$s2i], [qw(i2s s2i)]) ,"\n";
    ##
    print "--untie(s2i)--\n";
    untie(%$s2i);
    print Data::Dumper->Dump([$i2s,$s2i], [qw(i2s s2i)]), "\n";
  }

  ##-- test data manipulation
  my $e = ${tied(@$i2s)};
  my $s = 'foo';
  my $i = $e->size();
  print "--insert($i : $s)--\n";
  $s2i->{$s} = $i;
  $i2s->[$i] = $s;
  print "i2s($i)=$i2s->[$i]\n";
  print "s2i($s)=$s2i->{$s}\n";
  while (my ($key,$val) = each %$s2i) {
    print "$key=$val\n";
  }
  #print Data::Dumper->Dump([$i2s,$s2i], [qw(i2s s2i)]), "\n";

  ##-- get size
  print "--scalar(i2s)--\n";
  my $n = scalar @$i2s;
  print "scalar(i2s) = $n\n";

  ##-- dump
  print "--dump--\n";
  $e->saveTextFile('-');

  exit 0;
}
#test_tied_enum(@ARGV);

##==============================================================================
## test: identity enums

sub test_idenum {
  my $n    = shift // 10;
  my $base = shift // 'idenum';
  my $e = DiaColloDB::EnumFile::Identity->new();
  $e->setsize($n);

  $e->save($base) or die("$0: save failed for '$base': $!");
  my $e2 = DiaColloDB::EnumFile->new(base=>$base) or die("$0: open failed for '$base': $!");

  my ($i2s,$s2i) = $e2->tiepair();
  my $s0 = $i2s->[0];
  my $i0 = $s2i->{0};

  print "--dump--\n";
  $e->saveTextFile("-");
  exit 0;
}
#test_idenum(@ARGV);


##==============================================================================
## test: parseRequest via ddc

##--------------------------------------------------------------
sub test_ddcparse {
  my $dbdir = shift || 'kern.d';
  my $req   = shift || '$l, $p';
  my $defaultIndex = undef; #''; ##-- default index name; set to undef for groupby parsing

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  my $q     = $coldb->parseQuery($req, default=>$defaultIndex);

  ##-- dump query
  #print Data::Dumper->Dump([$q->toHash],[qw(qhash)]);
  print "qreq=$req\n";
  print "qstr=", $q->toString, "\n";

  exit 0;
}
#test_ddcparse(@ARGV);


##--------------------------------------------------------------
sub test_ddcrelq {
  my $dbdir = shift || 'kern.d';
  my %opts  = map {split(/=/,$_,2)} @_;

  $opts{query}   ||= 'Haus'; #'Haus, $p=NN #has[author,/kant/]';
  $opts{groupby} ||= '$l,$p=ADJA';
  $opts{slice}   ||= 0;
  $opts{date}    ||= '1900:1999';

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  my $rel   = DiaColloDB::DDC->fromDB($coldb, ddcServer=>'localhost:52000');

  my $qcount = $rel->countQuery($coldb, \%opts);

  ##-- dump query
  #print Data::Dumper->Dump([$q->toHash],[qw(qhash)]);
  print "limit=", ($opts{limit}//'(undef)'), "\n";
  print "qstr=", $qcount->toString, "\n";

  exit 0;
}
#test_ddcrelq(@ARGV);

##--------------------------------------------------------------
sub test_ddcprf {
  my $dbdir = shift || 'dta.d';
  my %opts  = map {split(/=/,$_,2)} @_;

  #$opts{query}   ||= 'Mann #sample[100] #has[textClass,Wiss*]'; #'Haus, $p=NN #has[author,/kant/]';
  #$opts{query}   ||= 'Mann'; #'Haus, $p=NN #has[author,/kant/]';
  $opts{query}   ||= '"$p=ADJA=2 Mann"'; #'Haus, $p=NN #has[author,/kant/]';
  #$opts{groupby} ||= 'l=jung|alt,p=ADJA,textClass';
  #$opts{groupby} ||= '[textClass]';
  $opts{groupby} ||= '[textClass~s/:+[^:]*$//]';
  $opts{slice}   ||= 0;
  #$opts{date}    ||= '1900:1999';
  $opts{score}   ||= 'f';
  $opts{kbest}   ||= 10;

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  #$coldb->{ddcServer} = 'localhost:52000'; ##-- local:kern.plato
  $coldb->{ddcServer} = 'kaskade.dwds.de:50250'; ##-- dta.beta
  $mp       = $coldb->profile('ddc',%opts) or die("$0: failed to acquire profile: $!");
  $mp->saveTextFile('-');

  exit 0;
}
#test_ddcprf(@ARGV);

##--------------------------------------------------------------
sub test_ddcdiff {
  my $dbdir = shift || 'dta.d';
  my %opts  = map {split(/=/,$_,2)} @_;

  $opts{aquery}   ||= "Fr\xfchling";
  $opts{bquery}   ||= "Herbst";
  $opts{groupby} ||= '';
  $opts{aslice}   ||= 10;
  $opts{bslice}   ||= 10;
  $opts{adate}   ||= ''; #'1600:1799';
  $opts{bdate}   ||= ''; #'1800:1899';
  $opts{score}   ||= 'ld';
  $opts{kbest}   ||= 10;

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  #$coldb->{ddcServer} = 'localhost:52000'; ##-- local:kern.plato
  $coldb->{ddcServer}  = 'kaskade.dwds.de:50250'; ##-- dta.beta
  $mp                  = $coldb->compare('ddc',%opts) or die("$0: failed to acquire profile: $!");
  $mp->saveTextFile('-');

  exit 0;
}
#test_ddcdiff(@ARGV);

##--------------------------------------------------------------
sub test_pnndiff {
  my $dbdir = shift || 'pnn.d';
  my %opts  = map {split(/=/,$_,2)} @_;
  %opts = (
	   aquery => "Bürokrat",
	   bquery => "Funktionär",
	   aslice => 1,
	   bslice => 1,
	   %opts,
	  );

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  $mp = $coldb->compare('cof',%opts) or die("$0: failed to acquire profile: $!");
  $mp->saveTextFile('-');

  exit 0;
}
#test_pnndiff(@ARGV);

##--------------------------------------------------------------
sub test_diffop {
  my $dbdir = shift || 'dta.d';
  my %opts  = map {split(/=/,$_,2)} @_;
  %opts = (
	   aquery => "Mann",
	   bquery => "Frau",
	   slice => 0,
	   date => '1700:1899',
	   score => 'ld',
	   kbest => 1,
	   diff   => 'lavg',
	   global => 1,
	   eps => 0.5,
	   %opts,
	  );

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  $mp = $coldb->compare('cof',%opts) or die("$0: failed to acquire profile: $!");
  $mp->saveTextFile('-');

  exit 0;
}
#test_diffop(@ARGV);

##==============================================================================
## test: vsem

sub matcat2d {
  my ($a,$b) = @_;
  my $c = zeroes($a->type, $a->dim(0)+$b->dim(0), $a->dim(1)+$b->dim(1));
  my ($tmp);
  ($tmp=$c->slice("0:".($a->dim(0)-1).",0:".($a->dim(1)-1))) .= $a;
  ($tmp=$c->slice(-$b->dim(0).":-1,".-$b->dim(1).":-1"))     .= $b;
  return $c;
}
BEGIN {
  *PDL::matcat2d = \&matcat2d;
}

sub isok {
  my ($label,$bool) = @_;
  print "$label: ".($bool ? "ok" : "NOT ok")."\n";
}
sub svdok {
  my ($label,$raw,$v,$s,$u,$eps) = @_;
  $eps //= 1e-5;
  isok("svd:$label", all(($v x stretcher($s) x $u)->abs->approx($raw->abs,$eps)));
}

##--------------------------------------------------------------
## test: vsem: union
sub test_svd_union {
  $, = ' ';
  my @adims = (5,3);
  my @bdims = (7,2);
  my $a =    sequence(@adims)->xchg(0,1);
  my $b = 10*sequence(@bdims)->xchg(0,1);
  my ($tmp);
  ($tmp=$a->where($a % 3)) .= 0;
  ($tmp=$b->where($b % 3)) .= 0;

  my $ab = matcat2d($a,$b);
  my ($va,$sa,$ua) = $a->svd;
  my ($vb,$sb,$ub) = $b->svd;
  my ($vab,$sab,$uab) = $ab->svd;
  svdok("a", $a,$va,$sa,$ua);
  svdok("b", $b,$vb,$sb,$ub);
  svdok("ab", $ab,$vab,$sab,$uab);

  ##-- ok, but this doesn't help us with vsem union()
  my $vabc = matcat2d($va,$vb);
  my $sabc = $sa->append($sb);
  my $uabc = matcat2d($ua,$ub);
  svdok("ab:cat", $ab,$vabc,$sabc,$uabc);

  ##-- stretcher values all == 1 (matrices already singular)
  my ($vabv,$vabs,$vabu) = $vabc->svd;
  my ($uabv,$uabs,$uabu) = $uabc->svd;

  exit 0;
}
#test_svd_union();


##--------------------------------------------------------------
## test: vsem: get sig sizes
sub test_vsem_sigsize {
  my $dbdir = shift || 'kern01.d';
  print STDERR "$0: sigsize: $dbdir\n";

  my @docs;
  tie(@docs, 'Tie::File::Indexed::JSON', "$dbdir/doctmp.a", mode=>'ra')
    or die("$0: tie filed for $dbdir/doctmp.a: $!");

  ##-- dump sig sizes
  my ($doc,$sig,$n);
  foreach $doc (@docs) {
    foreach $sig (@{$doc->{sigs}}) {
      $n = 0;
      $n += $_ foreach (values %$sig);
      print "$n\n";
    }
  }

  exit 0;
}
#test_vsem_sigsize(@ARGV);

##--------------------------------------------------------------
## test: vsem: trim sigs
sub test_vsem_trimsigs {
  my $tmpfile = shift || 'doctmp.a';
  my $nmin    = shift || 8;
  my $nmax    = shift || 1024;

  print STDERR "$0: trimsigs: $tmpfile (min=$nmin, max=$nmax)\n";

  my (@idocs,@odocs);
  tie(@idocs, 'Tie::File::Indexed::JSON', $tmpfile, mode=>'ra')
    or die("$0: tie filed for $tmpfile: $!");

  my $outfile = "$tmpfile.out";
  tie(@odocs, 'Tie::File::Indexed::JSON', $outfile, mode=>'rw')
    or die("$0: tie filed for $outfile: $!");

  ##-- trim sigs
  my ($doc,$sig,$n,@sigs);
  my ($ndocs,$nsig_in,$nsig_out,$ntok_in,$ntok_out) = (0,0,0,0,0);
  foreach $doc (@idocs) {
    @sigs = qw();
    foreach $sig (@{$doc->{sigs}}) {
      $n = 0;
      $n += $_ foreach (values %$sig);
      ++$nsig_in;
      $ntok_in += $n;
      if ($n >= $nmin && $n <= $nmax) {
	push(@sigs,$sig);
	++$nsig_out;
	$ntok_out += $n;
      }
    }
    if (@sigs) {
      $doc->{id} = $ndocs++;
      $doc->{sigs} = \@sigs;
      push(@odocs, $doc);
    }
  }
  my $ndoc_in  = @idocs;
  my $ndoc_out = @odocs;

  untie(@idocs);
  tied(@odocs)->rename($tmpfile);
  untie(@odocs);

  my $lfmt = "%3s";
  my $nfmt = "%".length($ntok_in)."d";
  my $pfmt = "%6.2f%%";
  my $trimline = sub {
    my ($label,$n_in,$n_out) = @_;
    return sprintf(" + $lfmt: $nfmt in, $nfmt out ($pfmt trimmed)\n", $label, $n_in, $n_out, 100*($n_in-$n_out)/$n_in);
  };
  print STDERR
    ("$0: file=$tmpfile ; outfile=$outfile\n",
     $trimline->('docs', $ndoc_in, $ndoc_out),
     $trimline->('sigs', $nsig_in, $nsig_out),
     $trimline->('toks', $ntok_in, $ntok_out),
    );
  exit 0;
}
#test_vsem_trimsigs(@ARGV);

##--------------------------------------------------------------
## test: vsem: tofloat
sub test_vsem_tofloat {
  my $dbdir = shift || 'kern01.d';

  my %dcio = (verboseIO=>1, saveSvdUS=>1, mmap=>0);
  my $vs = DiaColloDB::Relation::Vsem->new(base=>"$dbdir/vsem", flags=>'r', dcio=>\%dcio)
    or die("$0: failed to open $dbdir/vsem: $!");

  ##-- tweak map
  my $map = $vs->{dcmap};
  $map->{itype} = 'long';
  $map->{vtype} = 'float';

  ##-- convert types & re-save
  $map->compile_types() or die("$0: compile_types() failed");
  $map->saveDir("$dbdir/vsem.d/map.d-32", %dcio)
    or die("$0: failed to save $dbdir/vsem.d/map.d-32");

  ##-- all done
  exit 0;
}
#test_vsem_tofloat(@ARGV);

##--------------------------------------------------------------
## test: vsem: reindex
sub test_vsem_reindex {
  my $dbdir = shift || 'kern01.d';

  ##-- open (index_vsem:0)
  my $coldb = DiaColloDB->new() or die("$0: failed to create DiaColloDB object");
  $coldb->open($dbdir, index_vsem=>0) or die("$0: failed to open DiaColloDB directory $dbdir/:_ $!");

  ##-- (re-)index vsem (dying with log:
  # 2015-08-03 13:30:17 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile(): lemmatizer class: DocClassify::Lemmatizer::Raw
  # 2015-08-03 13:30:17 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compileTrim(): by #/terms per doc: maxTermsPerDoc=0
  # 2015-08-03 13:30:17 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compileTrim(): by global term freqency: minFreq=10
  # 2015-08-03 13:30:18 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compileTrim(): by document term frequency: minDocFreq=5
  # 2015-08-03 13:30:19 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compileCatEnum(): nullCat='(none)'
  # 2015-08-03 13:30:19 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compileTermEnum()
  # 2015-08-03 13:30:19 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_dcm(): matrix: dcm: (ND=408339 x NC=15915) [Doc x Cat -> Deg]
  # 2015-08-03 13:30:21 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_tdm0(): matrix: tdm0: (NT=72972 x ND=408339) [Term x Doc -> Freq]
  # 2015-08-03 13:30:21 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_tdm0(): matrix: tdm0: doc_wt [Doc -> Terms]
  # 2015-08-03 13:30:32 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_tdm0(): matrix: tdm0: Nnz
  # 2015-08-03 13:30:32 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_tdm0(): matrix: tdm0: PDL::CCS::Nd
  # 2015-08-03 13:31:03 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_tdm_log(): smooth(smoothf=1.001)
  # 2015-08-03 13:31:04 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_tw(): vector: tw: (NT=72972) [Term -> Weight]
  # 2015-08-03 13:31:04 dcdb-create.perl[19761] INFO: DocClassify.Mapper.LSI: compile_tw(): tw=max-entropy-quotient, weightByCat=0, wRaw=1, wCooked=1
  # Out of memory!
  # ... on plato, source=kern01.files
  # ... see vsnotes.txt; better but not yet really memory-friendly
  my %TDF_OPTS = %DiaColloDB::TDF_OPTS;
  $coldb->info("creating vector-space model $dbdir/vsem* [vbreak=$coldb->{vbreak}]");
  $coldb->{vsopts} //= {};
  $coldb->{vsopts}{$_} //= $TDF_OPTS{$_} foreach (keys %TDF_OPTS); ##-- vsem: default options

  ##-- tie doctmp array
  -e "$dbdir/doctmp.a"
    && ($coldb->{doctmpa} = [])
    && tie(@{$coldb->{doctmpa}}, 'Tie::File::Indexed::JSON', "$dbdir/doctmp.a", mode=>'ra', temp=>0)
    || $coldb->logconfess("could not tie temporary doc-data array to $dbdir/doctmp.a: $!");

  ##-- tweak options
  #$coldb->{vsopts}{weightByCat} = 1;
  #$coldb->{vsopts}{termWeight} = 'uniform';
  #@{$coldb->{vsopts}}{qw(twRaw twCooked)} = qw(1 0);
  $coldb->{vsopts}{saveMem} = 1;
  #$coldb->{vsopts}{svdr} = 32;

  ##-- re-index
  $coldb->{vsem} = DiaColloDB::Relation::Vsem->create($coldb, undef, base=>"$dbdir/vsem");

  exit 0;
}
#test_vsem_reindex(@ARGV);

sub test_vsem {
  my $dbdir = shift || 'dta_phil.d';
  my %opts  = map {split(/=/,$_,2)} @_;
  %opts = (
	   query => "Mädchen",
	   slice => 0,
	   kbest => 10,
	   slice => 100,
	   date => '1600:1699', ##-- BUGGY ?!
	   groupby => 'l',
	   %opts,
	  );

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  $mp = $coldb->profile('vsem',%opts) or die("$0: failed to acquire profile: $!");
  $mp->saveTextFile('-');
  exit 0;
}
#test_vsem(@ARGV);

sub test_vsem_diff {
  my $dbdir = shift || 'kern01.d';
  my %opts  = map {split(/=/,$_,2)} @_;
  %opts = (
	   aquery => "Katze",
	   bquery => "Maus",
	   slice => 0,
	   kbest => 10,
	   #date => '1600:1699', ##-- Can't call method "nelem" on an undefined value at DiaColloDB/Profile/PdlDiff.pm line 150.
	   groupby => 'l,p=NN',
	   diff => 'havg',
	   %opts,
	  );

  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  $mp = $coldb->compare('vsem',%opts) or die("$0: failed to acquire profile: $!");
  $mp->saveTextFile('-');
  exit 0;
}
#test_vsem_diff(@ARGV);

##--------------------------------------------------------------
## bench: attribute-token lists

sub read_atoks_txt {
  my $atokfile = shift;
  open(my $atokfh, "<:raw", $atokfile)
    or die("$0: open failed for $atokfile: $!");
  my (@vals);
  while (defined($_=<$atokfh>)) {
    chomp;
    next if (/^\s*$/);
    @vals = split(' ',$_);
  }
  close($atokfh);
  return;
}

sub read_atoks_bin {
  my $atokfile = shift;
  open(my $atokfh, "<:raw", $atokfile)
    or die("$0: open failed for $atokfile: $!");
  my ($buf,@vals);
  my $pack_w = 'N2';
  my $pack_l = 8;
  while (!$atokfh->eof) {
    CORE::read($atokfh, $buf, $pack_l)
	or die("$0: read failed for $atokfh: $!");
    @vals = unpack($pack_w,$buf);
  }
  return;
}

sub bench_atok_file {
  my $dbdir = shift // 'out.d';

  ##-- prepare: write binfile
  print STDERR "$0: bench_atok_file(): prepare\n";
  my $atokfile = "$dbdir/atokens.dat";
  open(my $atokfh, "<:raw", $atokfile)
    or die("$0: open failed for $atokfile: $!");
  my $pack_w = 'N2';
  open(my $binfh, ">:raw", "$atokfile.bin")
    or die("$0: open failed for $atokfile.bin: $!");
  my (@vals);
  while (defined($_=<$atokfh>)) {
    chomp;
    next if (/^\s*$/);
    @vals = split(' ',$_);
    pop(@vals);
    $binfh->print(pack($pack_w,@vals));
  }
  close($binfh);
  close($atokfh);

  ##-- bench
  cmpthese(1, {
	       'txt' => sub { read_atoks_txt($atokfile) },
	       'bin' => sub { read_atoks_bin("$atokfile.bin") },
	      });
}
#bench_atok_file(@ARGV);

##==============================================================================
## convert tdm tfidf to frequency matrix

sub tfidf_to_tdm {
  my $vsdir = shift;
  $vsdir =~ s{/$}{};
  $vsdir = "$vsdir/vsem.d" if (!-e "$vsdir/tdm.ix");
  DiaColloDB->ensureLog();

  ##-- open
  my $vs = DiaColloDB;
  $vs->info("open($vsdir)");
  my %ioopts = (ReadOnly=>0, mmap=>1);
  defined(my $tdm = DiaColloDB::Utils::readPdlFile("$vsdir/tdm", class=>'PDL::CCS::Nd', %ioopts, mmap=>0))
    or $vs->logconfess("open(): failed to load term-document matrix from $vsdir/tdm.*: $!");
  defined(my $tw  = DiaColloDB::Utils::readPdlFile("$vsdir/tw.pdl", %ioopts))
    or $vs->logconfess("open(): failed to load term-weights from $vsdir/tw.pdl: $!");

  ##-- common variables
  my $ix = $tdm->_whichND;
  my $nz = $tdm->_nzvals;

  if (all($nz==$nz->rint)) {
    die("$0: $vsdir/tdm.nz already contains only integer values; aborting");
  }

  $vs->info("converting tfidf to raw frequency matrix");
  $nz /= $tw->index($ix->slice("(0),"));
  $nz *= log(2);
  $nz->inplace->exp;
  $nz -= 1;
  $nz->inplace->rint;
  $nz = $nz->append(0);

  $vs->info("saving altered $vsdir/tdm.nz");
  DiaColloDB::Utils::writePdlFile($nz, "$vsdir/tdm.nz")
      or die("$0: failed to save $vsdir/tdm.nz: $!");

  $vs->info("converted $vsdir/tdm.nz");
  exit 0;
}
#tfidf_to_tdm(@ARGV);

##==============================================================================
## vsem: convert tdm to tcm

sub tdm_to_tcm {
  my $dbdir = shift // 'kern.d-p';
  DiaColloDB->ensureLog();

  DiaColloDB->info("tdm_to_tcm($dbdir)");
  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open DiaColloDB directory '$dbdir': $_");
  my $vs    = $coldb->{vsem};
  my $tdm   = $vs->{tdm};
  my $d2c   = $vs->{d2c};

  my $wnd0  = $tdm->_whichND->pdl;
  $wnd0->slice("(1),") .= $d2c->index($tdm->_whichND->slice("(1),"));
  $wnd0->_ccs_accum_sum_int($tdm->_nzvals, 0,0,
			    (my $wnd1=zeroes($wnd0->type, $tdm->ndims, $tdm->_nnz+1)),
			    (my $vals1=zeroes($tdm->type, $tdm->_nnz+1)),
			    (my $nout=pdl($wnd0->type, 0)));
  $nout  = $nout->sclr;
  $wnd1  = $wnd1->slice(",0:".($nout-1));
  $vals1 = $vals1->slice("0:$nout");
  $vals1->set($nout => 0);
  my $tcm = PDL::CCS::Nd->newFromWhich($wnd1, $vals1, dims=>[$vs->nTerms,$vs->nCats], sorted=>1, steal=>1);
  $tcm->writefraw("$vs->{base}.d/tcm")
    or die("$0: failed to write $vs->{base}/tcm*: $!");
  exit 0;
}
#tdm_to_tcm(@ARGV);

##==============================================================================
## vsem: convert tdm to tym

sub tdm_to_tym {
  my $dbdir = shift // 'kern.d-p';
  DiaColloDB->ensureLog();

  DiaColloDB->info("tdm_to_tym($dbdir)");
  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open DiaColloDB directory '$dbdir': $_");
  my $vs    = $coldb->{vsem};
  my $tdm   = $vs->{tdm};
  my $d2c   = $vs->{d2c};
  my $c2date = $vs->{c2date};

  DiaColloDB->info("converting");
  my $wnd0  = $tdm->_whichND->pdl;
  $wnd0->slice("(1),") .= $c2date->index( $d2c->index($tdm->_whichND->slice("(1),")) );
  $wnd0->_ccs_accum_sum_int($tdm->_nzvals, 0,0,
			    (my $wnd1=zeroes($wnd0->type, $tdm->ndims, $tdm->_nnz+1)),
			    (my $vals1=zeroes($tdm->type, $tdm->_nnz+1)),
			    (my $nout=pdl($wnd0->type, 0)));
  $nout  = $nout->sclr;
  $wnd1  = $wnd1->slice(",0:".($nout-1));
  $vals1 = $vals1->slice("0:$nout");
  $vals1->set($nout => 0);
  my $ymax = $wnd0->slice("(1),")->max;
  my $tym = PDL::CCS::Nd->newFromWhich($wnd1, $vals1, dims=>[$vs->nTerms,$ymax+1], sorted=>0, steal=>1);
  $tym->writefraw("$vs->{base}.d/tym")
    or die("$0: failed to write $vs->{base}/tym*: $!");
  exit 0;
}
#tdm_to_tym(@ARGV);

##==============================================================================
## vsem: convert tdm to cf

sub tdm_to_cf {
  my $dbdir = shift // 'kern.d-p';
  DiaColloDB->ensureLog();

  ##-- create dummy 'cf.pdl' if it doesn't exist
  my $vsdir = "$dbdir/vsem.d";
  my $cffile = "$vsdir/cf.pdl";
  if (!-e $cffile) {
    pdl(float,0)->writefraw($cffile)
      or die("$0: failed to write dummy $cffile: $!");
  }

  DiaColloDB->info("tdm_to_cf($dbdir)");
  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open DiaColloDB directory '$dbdir': $_");
  my $vs    = $coldb->{vsem};
  my $tdm   = $vs->{tdm};
  my $d2c   = $vs->{d2c};
  delete $vs->{cf};

  DiaColloDB->info("converting");
  $tdm->_nzvals->indadd( $d2c->index($tdm->_whichND->slice("(1),")), my $cf=zeroes($vs->vtype,$vs->nCats));
  $cf->writefraw($cffile)
    or die("$0: failed to write $cffile: $!");
  exit 0;
}
#tdm_to_cf(@ARGV);

##==============================================================================
## vsem: add "genre" field from "textClass"

sub vs_add_genre {
  my $dbdir = shift // 'kern.d-p';
  DiaColloDB->ensureLog();
  my $vsdir = "$dbdir/vsem.d";
  my $that   = 'DiaColloDB';

  ##-- open vsem header
  (my $hfile=$vsdir) =~ s{\.d(?:/?)$}{\.hdr};
  my $hdr = DiaColloDB::Utils::loadJsonFile($hfile)
    or $that->logconfess("failed to load header from $hfile: $!");
  die("$0: vsem index in $vsdir/ already contains a 'genre' field") if (grep {$_ eq 'genre'} @{$hdr->{meta}//[]});

  ##-- open "textClass" enum
  my $tcenum = $DiaColloDB::ECLASS->new(base=>"$vsdir/meta_e_textClass")
    or $that->logconfess("failed to open enum $vsdir/meta_e_textClass: $!");

  ##-- open 'mvals' and 'msorti' piddles
  defined(my $mvals0 = DiaColloDB::Utils::readPdlFile("$vsdir/mvals.pdl", mmap=>0))
    or $that->logconfess("failed to read $vsdir/mvals.pdl: $!");
  defined(my $msorti0 = DiaColloDB::Utils::readPdlFile("$vsdir/msorti.pdl", mmap=>0))
    or $that->logconfess("failed to read $vsdir/msorti.pdl: $!");

  $that->info("creating genre enum");
  my $g2i = {''=>0};
  my $ng  = 0;
  my ($tc,$g,$tci,$gi);
  my $tci2gi = zeroes($mvals0->type, $tcenum->size);
  my $ntc = $tcenum->size;
  for ($tci=0; $tci < $ntc; ++$tci) {
    $tc = $tcenum->i2s($tci);
    ($g=$tc) =~ s/\:.*$//;
    $gi = $g2i->{$g} = ++$ng if (!defined($gi=$g2i->{$g}));
    $tci2gi->set($tci => $gi);
  }
  my %efopts = map {($_=>$tcenum->{$_})} qw(pack_id pack_o pack_l utf8);
  my $genum = ref($tcenum)->new(%efopts)->fromHash($g2i)
    or $that->logconfess("failed to create genre enum: $!");
  $genum->save("$vsdir/meta_e_genre")
    or $that->logconfess("failed to save $vsdir/meta_e_genre: $!");

  ##-- update $mvals, $msorti
  $that->info("updating mvals, msorti");
  my $tcpos  = (grep {$hdr->{meta}[$_] eq 'textClass'} (0..$#{$hdr->{meta}}))[0];
  my $mvals1 = zeroes($mvals0->dim(0)+1, $mvals0->dim(1));
  $mvals1->slice("0:-2,") .= $mvals0;
  $mvals1->slice("(-1),") .= $tci2gi->index( $mvals0->slice("($tcpos),") );
  undef $mvals0;

  my $msorti1 = zeroes($msorti0->dim(0), $msorti0->dim(1)+1);
  $msorti1->slice(",0:-2") .= $msorti0;
  $msorti1->slice(",(-1)") .= $mvals1->slice("(-1),")->qsorti;

  ##-- update header
  $that->info("updating header");
  push(@{$hdr->{meta}}, 'genre');
  DiaColloDB::Utils::saveJsonFile($hdr, $hfile)
      or die("$0: failed to save header $hfile: $!");

  ##-- write output piddles
  $mvals1->writefraw("$vsdir/mvals.pdl")
    or die("$0: failed to write new $vsdir/mvals.pdl");
  $msorti1->writefraw("$vsdir/msorti.pdl")
    or die("$0: failed to write new $vsdir/msorti1.pdl");

  $that->info("done");
}
#vs_add_genre(@ARGV);

##==============================================================================
## hv test (for tdf/pdl aggregation)

sub xs_hvtest {
  my ($niters,$nitems) = @_;
  $niters ||= 1000;
  $nitems ||= 100;
  DiaColloDB::PDL::Utils::diacollo_hvtest($niters,$nitems);
  exit 0;
}
#xs_hvtest(@ARGV);

##==============================================================================
## PackedFile -> pdl

sub test_pfpdl {
  use PDL;
  use PDL::IO::FastRaw;
  my @args = @_;
  @args  = qw(42 24 7) if (!@args);
  my $pf = DiaColloDB::PackedFile->new(file=>'tmp.pf', flags=>'rw', packas=>'N');
  $pf->fromArray(\@args);
  my $pdl = $pf->toPdl();
  print "got pdl: $pdl\n";
  exit 0;
}
#test_pfpdl(@ARGV);

##==============================================================================
## test: list-clients urls

sub test_client_list {
  DiaColloDB->ensureLog(level=>'INFO');
  my $dburl = shift || 'list://kern01-1ka.d kern01-1kb.d ?fudge=0';
  my $cli = DiaColloDB::Client->new($dburl)
    or die("$0: failed to create client for DB-URL '$dburl'");

  my %q = (
	   query=>'Kaffee',
	   slice=>0,
	   kbest=>10,
	   strings=>1,
	   score=>'ld',
	   groupby=>'l,p=NN',
	  );
  my $mp = $cli->profile('cof',%q);
  $mp->saveTextFile('-');
  exit 0;
}
#test_client_list @ARGV;

##==============================================================================
## test: "stark" f2 mismatch 1900:1919 in {kern01-1k.d,union} vs a+b list

sub test_stark_union {
  DiaColloDB->ensureLog(level=>'INFO');
  my $dbdir = shift || 'kern01-1k.d';
  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("open failed for $dbdir: $!");

  my $lids = $coldb->enumIds($coldb->{lenum}, 'stark');
  my $xids = [map {@{$coldb->{l2x}->fetch($_)}} @$lids];
  my $cof  = $coldb->{cof};
  my ($r1,$r2) = @$cof{qw(r1 r2)};
  my $pack1i = $cof->{pack_i};
  my $pack1f = "@".DiaColloDB::Utils::packsize($cof->{pack_i}).$cof->{pack_f};
  my ($buf);
  foreach my $xid (@$xids) {
    my $xbuf = $coldb->{xenum}->i2s($xid);
    my @aids = unpack($coldb->{pack_x}, $xbuf);
    my $date = pop(@aids);
    my @vals = map {$coldb->{$coldb->{attrs}[$_].'enum'}->i2s($aids[$_])} (0..$#aids);
    next if (!grep {$_ eq 'ADJA'} @vals);
    my $beg2 = ($xid==0 ? 0 : unpack($pack1i,$r1->fetchraw($xid-1,\$buf)));
    my ($end2,$f1) = unpack($r1->{packas}, $r1->fetchraw($xid, \$buf));
    my $f1c = 0;
    for ($r2->seek($beg2), $pos2=$beg2; $pos2 < $end2; ++$pos2) {
      $r2->getraw(\$buf) or last;
      my ($i2,$f12) = unpack($r2->{packas},$buf);
      $f1c += $f12;
    }
    print join("\t", $f1, @vals,$date,"=$xid")."\n";
  }
  exit 0;
}
#test_stark_union(@ARGV);


##==============================================================================
## test: f2 expansion

##----------------------------------------------------------------------
sub test_expand_stats {
  DiaColloDB->ensureLog(level=>'INFO');
  my $dbdir = shift || 'kern01-1k.d';
  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("open failed for $dbdir: $!");
  use PDL;
  $|=1;

  foreach my $attr (@{$coldb->{attrs}}) {
    print "$attr: ";
    my $a2x = $coldb->{"${attr}2x"};
    my $a2b = $a2x->toArray;
    my $len_i = $a2x->{len_i};
    my $lens  = pdl(map {length($_)/$len_i} @$a2b);
    my $N = $lens->nelem;
    my ($mu,$prms,$med,$min,$max,$adev,$sigma) = $lens->statsover;
    print "N=$N ; mu=$mu ; sigma=$sigma ; min/max/median=$min / $max / $med\n";
  }

  ##-- kern@plato
  # l: N=631799 ; mu=9.10426575540639 ; sigma=24.0569035379271 ; min/max/median=0 / 547 / 2
  # p: N=12 ; mu=479338.833333333 ; sigma=757558.357989208 ; min/max/median=0 / 2849353 / 295066
  ##
  ##-- kern@kira
  # l: N=291287 ; mu=17.5979978509168 ; sigma=33.5284551256776 ; min/max/median=0 / 548 / 6
  # p: N=12 ; mu=427172.333333333 ; sigma=650768.244694855 ; min/max/median=0 / 2449699 / 285423.5
  ##
  ##-- zeit@kaskade
  # l: N=678642 ; mu=16.4264472284356 ; sigma=25.338426867474 ; min/max/median=0 / 400 / 7
  # p: N=12 ; mu=928973.083333333 ; sigma=1683321.33617384 ; min/max/median=0 / 6235283 / 371018
  ##
  ##-- zeitungen@kaskade
  # l: N=1788366 ; mu=14.3146783152889 ; sigma=21.3936860726719 ; min/max/median=0 / 414 / 7
  # p: N=12 ; mu=2133323.66666667 ; sigma=4183780.06375304 ; min/max/median=1 / 15114307 / 509367

  exit 0;
}
#test_expand_stats @ARGV;

##----------------------------------------------------------------------
use Algorithm::BinarySearch::Vec;
sub test_expand_intersect {
  my ($dbdir,$l,$p) = @_;
  $dbdir ||= 'kern.d';
  $l     ||= 'Mann';
  $p     ||= 'NN';

  DiaColloDB->ensureLog(level=>'INFO');
  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("open failed for $dbdir: $!");

  ##-- get enum-ids
  utf8::decode($l) if (!utf8::is_utf8($l));
  utf8::decode($p) if (!utf8::is_utf8($p));
  my $li = $coldb->{lenum}->s2i($l) or die("$0: unknown lemma '$l'");
  my $pi = $coldb->{penum}->s2i($p) or die("$0: unknown postag '$p'");

  ##-- get expansion-vectors
  my ($l2x,$p2x) = @$coldb{qw(l2x p2x)};
  my $lx = $l2x->fetchraw($li);
  my $px = $p2x->fetchraw($pi);
  my $len_i = $l2x->{len_i};
  my $nbits = $l2x->{len_i}*8;

  ##-- get multimap-variants
  my $mmbase = $p2x->{base};
  my $mmf    = $p2x;
  my $mmf2   = DiaColloDB::MultiMapFile2->new(base=>"${mmbase}2")
    or die("$0: failed to open MultiMapFile2 ${mmbase}2.*");
  my $mmf2m  = DiaColloDB::MultiMapFile2::MMap->new(base=>"${mmbase}2")
    or die("$0: failed to open MultiMapFile2::MMap ${mmbase}2.*");

  ##-- test intersection
  my $lpx = vintersect($lx,$px,$nbits);

  ##-- report sizes
  print "l=$l ; p=$p : Nl=".(length($lx)/$len_i)." ; Np=".(length($px)/$len_i)." ; N(l&p)=".(length($lpx)/$len_i)."\n";

  ##-- time: fetch
  timethese(-3,
	       {
		'fetch:l'     => sub { $lx = $l2x->fetchraw($li) },
		'fetch:p'     => sub { $px = $p2x->fetchraw($pi) },
	       },
	   );

  ##-- time: intersection (xs vs perl)
  timethese(-3,
	    {
	     'vintersect:xs' => sub { $lpx=Algorithm::BinarySearch::Vec::XS::vintersect($lx,$px,$nbits) },
	     'vintersect:perl' => sub { $lpx=Algorithm::BinarySearch::Vec::_vintersect($lx,$px,$nbits) },
	     'vintersect+fetch' => sub { $lpx=vintersect($lx,$mmf->fetchraw($pi),$nbits) },
	     'vintersect+fetch2' => sub { $lpx=vintersect($lx,$mmf2->fetchraw($pi),$nbits) },
	     'vintersect+fetch2m' => sub { $lpx=vintersect($lx,$mmf2m->fetchraw($pi),$nbits) },
	    });

  print STDERR "test_expand_intersect() done\n";
  exit 0;
}
#test_expand_intersect @ARGV;

##----------------------------------------------------------------------
## test multimap variants
#use DiaColloDB::MultiMapFile2;
#use DiaColloDB::MultiMapFile2::MMap;
sub bench_multimap_fetch {
  my ($dbdir,$attr,$val) = @_;
  $dbdir ||= 'kern.d';
  $attr  ||= 'p';
  $val   ||= 'NN';

  DiaColloDB->ensureLog(level=>'INFO');
  my $coldb = DiaColloDB->new(dbdir=>$dbdir)
    or die("open failed for $dbdir: $!");

  ##-- get enum-ids
  utf8::decode($val) if (!utf8::is_utf8($val));
  my $enum = $coldb->{"${attr}enum"} or die("$0: unknown attribute '$attr'");
  my $vali = $enum->s2i($val) or die("$0: unknown value '$val' for attribute '$attr'");

  ##-- get multimaps
  my $mmf    = $coldb->{"${attr}2x"};
  my $mmbase = $mmf->{base};

  my $mmf2   = DiaColloDB::MultiMapFile2->new(base=>"${mmbase}2")
    or die("$0: failed to open MultiMapFile2 ${mmbase}2.*");
  my $mmf2m  = DiaColloDB::MultiMapFile2::MMap->new(base=>"${mmbase}2")
    or die("$0: failed to open MultiMapFile2::MMap ${mmbase}2.*");

  ##-- check consistency
  my $buf  = $mmf->fetchraw($vali);
  my $buf2 = $mmf2->fetchraw($vali);
  my $buf2m = $mmf2m->fetchraw($vali);
  ok($buf eq $buf2, "mmf~mmf2");
  ok($buf eq $buf2m, "mmf~mmf2m");

  ##-- bench: fetchraw
  timethese(-3,
	    {
	     'mmf'       => sub { $buf = $mmf->fetchraw($vali) },
	     'mmf2'      => sub { $buf = $mmf2->fetchraw($vali) },
	     'mmf2:mmap' => sub { $buf = $mmf2m->fetchraw($vali) },
	    },
	   );

  print STDERR "bench_multimap_fetch() done\n";
  exit 0;
}
#bench_multimap_fetch @ARGV;

##----------------------------------------------------------------------
## bench group-extraction
sub bench_group_extract {
  my $pfile = shift || 'prf.json';

  ##-- load profile
  DiaColloDB->ensureLog(level=>'INFO');
  my $logger = 'DiaColloDB';
  $logger->info("loading $pfile and encoding");
  my $mp = DiaColloDB::Utils::loadJsonFile($pfile)
    or die("$0: failed to load multi-profile from '$pfile': $!");

  ##-- get flat- and deep-encodings
  my $f12_flat_s = {};
  my $f12_flat_p = {};
  my $f12_deep_s = {};
  my $f12_deep_p = {};
  my ($pack_g,$pack_date) = qw(N* n);
  my ($f12,$pkey,$slice,$pslice,$sslice);
  foreach my $prf (@{$mp->{profiles}}) {
    $f12    = $prf->{f12};
    $slice  = $prf->{label}+0;
    $sslice = "\t".$slice;
    $pslice = pack($pack_date,$prf->{label}+0);
    foreach (keys %$f12) {
      $f12_flat_s->{$_.$sslice} = $f12->{$_};
      $pkey = pack($pack_g,split(' ',$_));
      $f12_flat_p->{$pkey.$pslice} = $f12->{$_};
      $f12_deep_p->{$slice}{$pkey} = $f12->{$_};
    }
    $f12_deep_s->{$slice} = $f12;
  }

  ##-- get list of all groups (packed)
  my $len_date = DiaColloDB::Utils::packsize($pack_date);
  my @items_p  = sort keys %$f12_flat_p;
  my @items_s  = sort keys %$f12_flat_s;
  my @groups_p = @{DiaColloDB::Utils::sluniq([map {substr($_,0,-$len_date)} @items_p])};
  my @groups_s = @{DiaColloDB::Utils::luniq([map {keys %$_} values %$f12_deep_s])};
  my @slices_s = sort keys %$f12_deep_s;
  my @slices_p = map {pack($pack_date,$_)} @slices_s;
  $logger->info("found ", scalar(@groups_p), " group(s) in ", scalar(@items_p), " item(s) over ", scalar(@slices_s), " slice(s)");

  ##-- benchmark subs
  my ($n,$item);
  my $bench_flat_s = sub {
    $n=0;
    foreach $item (@items_s) {
      foreach $slice (@slices_s) {
	++$n if (exists $f12_flat_s->{"$item\t$slice"});
      }
    }
    return $n;
  };
  my $bench_flat_p = sub {
    $n = 0;
    foreach $item (@items_p) {
      foreach $slice (@slices_p) {
	++$n if (exists $f12_flat_p->{$item.$slice});
      }
    }
    return $n;
  };
  my $bench_deep_s = sub {
    $n = 0;
    foreach $item (@items_s) {
      foreach $slice (@slices_s) {
	++$n if (exists $f12_deep_s->{$slice}{$item});
      }
    }
    return $n;
  };
  my $bench_deep_p = sub {
    $n = 0;
    foreach $item (@items_p) {
      foreach $slice (@slices_s) {
	++$n if (exists $f12_deep_p->{$slice}{$item});
      }
    }
    return $n;
  };

  ##-- sanity check
  my $n_flat_s = $bench_flat_s->();
  my $n_flat_p = $bench_flat_p->();
  my $n_deep_s = $bench_deep_s->();
  my $n_deep_p = $bench_deep_p->();
  is($n_flat_s,$n_flat_p, "n_flat_s==n_flat_p");
  is($n_flat_p,$n_deep_s, "n_flat_p==n_deep_s");
  is($n_deep_s,$n_deep_p, "n_deep_s==n_deep_p");

  ##-- bench
  cmpthese(-3,
	   {
	    flat_s => $bench_flat_s,
	    flat_p => $bench_flat_p,
	    deep_s => $bench_deep_s,
	    deep_p => $bench_deep_p,
	   });
  ##          Rate flat_s flat_p deep_p deep_s
  ## flat_s 9.78/s     --   -25%   -29%   -35%
  ## flat_p 13.0/s    33%     --    -6%   -13%
  ## deep_p 13.8/s    41%     6%     --    -7%
  ## deep_s 14.9/s    53%    15%     8%     --


  print STDERR "bench_group_extract() done\n";
  exit 0;
}
#bench_group_extract @ARGV;

##----------------------------------------------------------------------
## bench pack vs split
sub bench_pack_split {
  my $pfile = shift || 'prf.json';

  ##-- load profile
  DiaColloDB->ensureLog(level=>'INFO');
  my $logger = 'DiaColloDB';
  $logger->info("loading $pfile and encoding");
  my $mp = DiaColloDB::Utils::loadJsonFile($pfile)
    or die("$0: failed to load multi-profile from '$pfile': $!");

  ##-- get data
  my @items_s = qw();
  my @items_p = qw();
  my ($pack_g,$pack_date) = qw(N* n);
  foreach my $prf (@{$mp->{profiles}}) {
    foreach (keys %{$prf->{f12}}) {
      push(@items_s, $_);
      push(@items_p, pack($pack_g,split(' ',$_)));
    }
  }


  ##-- benchmark subs
  my $project = 1;
  my ($prf,@items);
  my $bench_split = sub {
    @items = qw();
    foreach (@items_s) {
      push(@items, (split(' ',$_))[$project]);
    }
    return \@items;
  };

  my $pack_item = '@'.($project*DiaColloDB::Utils::packsize('N')).'N';
  my $bench_unpack = sub {
    @items = qw();
    foreach (@items_p) {
      push(@items, unpack($pack_item,$_));
    }
    return \@items;
  };

  ##-- sanity check
  my $items_s = join(' ', sort {$a<=>$b} @{$bench_split->()});
  my $items_p = join(' ', sort {$a<=>$b} @{$bench_unpack->()});
  ok($items_s eq $items_p, "items_s==items_p");

  ##-- bench
  cmpthese(-3,
	   {
	    split => $bench_split,
	    unpack => $bench_unpack,
	   });
  ##          Rate  split unpack
  ## split  60.3/s     --   -45%
  ## unpack  110/s    82%     --


  print STDERR "bench_group_extract() done\n";
  exit 0;
}
#bench_pack_split @ARGV;

##----------------------------------------------------------------------
## test cof reload (trunk)
sub test_cof_reload {
  my ($dbdir,$cofdat) = @_;
  $dbdir  //= 'kern.d_v0_09';
  $cofdat //= 'kern.dump_v0_09/cof.dat2';

  DiaColloDB->ensureLog(level=>'INFO');
  my $logger = 'DiaColloDB';

  $logger->info("loading $dbdir/cof.hdr");
  my $hdr = DiaColloDB::Utils::loadJsonFile("$dbdir/cof.hdr")
    or die("$0: failed to load $dbdir/cof.hdr");

  $logger->info("creating cof object $dbdir/cof.*");
  my $cof = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", flags=>'rw', map {($_=>$hdr->{$_})} grep {$_ !~ /^(?:N|class|size)/} keys %$hdr)
    or die("$0: failed to create new cof object");

  $logger->info("loading text data from $cofdat");
  $cof->loadTextFile($cofdat)
    or die("$0: failed to load text data from $cofdat: $!");

  $logger->info("saving header");
  $cof->saveHeader()
    or die("$0: failed to save header $dbdir/cof.hdr: $!");
  $cof->close;

  $logger->info("done");
  exit 0;
}
test_cof_reload(@ARGV);


##==============================================================================
## MAIN

foreach $i (1..3) {
  print "---dummy[$i]---\n";
}
exit 0;

