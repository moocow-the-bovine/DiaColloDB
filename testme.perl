#!/usr/bin/perl -w

use lib qw(.);
use DiaColloDB;
use DiaColloDB::Utils qw(:sort);
use PDL;
use File::Path qw(make_path remove_tree);
use File::Find;
use File::Basename qw(basename);
use JSON;
use Benchmark qw(timethese cmpthese);

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
  my $mp = $coldb->coprofile(lemma=>$lemma, slice=>10, kbest=>50, score=>'ld');

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
bench_profile_io(@ARGV);



##==============================================================================
## MAIN

foreach $i (1..3) {
  print "---dummy[$i]---\n";
}
exit 0;

