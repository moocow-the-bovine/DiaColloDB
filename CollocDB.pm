## -*- Mode: CPerl -*-
## File: CollocDB.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, top-level

package CollocDB;
use CollocDB::Logger;
use CollocDB::Enum;
use CollocDB::Corpus;
use CollocDB::Utils qw(:fcntl :json :sort);
use Fcntl;
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = 0.01;
our @ISA = qw(CollocDB::Logger);

##==============================================================================
## Constructors etc.

## $coldb = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##--options
##    dbdir => $dbdir,    ##-- database directory; REQUIRED
##    flags => $fcflags,  ##-- fcntl flags or open()-style mode string; default='rw'
##    index_w => $bool,   ##-- index surface word-forms? (default=1)
##    index_l => $bool,   ##-- index lemmata? (default=1)
##    ##
##    ##-- logging
##    logParseFile => $level, ##-- log-level for file-parsing (default='trace')
##    ##
##    ##-- enums
##    wenum => $wenum,    ##-- enum: words  ($dbdir/wenum.*) : $w<=>$wi : A*<=>N
##    lenum => $wenum,    ##-- enum: lemmas ($dbdir/lenum.*) : $l<=>$wi : A*<=>N
##    xenum => $xenum,    ##-- enum: tuples ($dbdir/xenum.*) : [$wi,$li,$di]<=>$xi : NNn<=>N
##    xpack => $fmt,      ##-- symbol pack-format for $xenum
##    ##
##    ##-- data
##    w2x   => $w2x,      ##-- db: word->tuples  ($dbdir/w2x.db) : $wi=>@xis  : N=>N*
##    l2x   => $l2x,      ##-- db: lemma->tuples ($dbdir/l2x.db) : $li=>@xis  : N=>N*
##    xf    => $xf,       ##-- db (recno): $xi => $f($xi) : N=>N
##   )
sub new {
  my $that = shift;
  my $coldb  = bless({
		     ##-- options
		     dbdir => undef,
		     flags => 'rw',
		     index_w => 1,
		     index_l => 1,
		     xpack   => 'NNn',

		     ##-- logging
		     logParseFile => 'trace',

		     ##-- enums
		     wenum => undef, #CollocDB::Enum->new(pack_i=>'N'),
		     lenum => undef, #CollocDB::Enum->new(pack_i=>'N'),
		     xenum => undef, #CollocDB::Enum->new(pack_i=>'N', pack_s=>$coldb->{xpack}),
		     w2x   => undef, #CollocDB::DBFile->new(pack_key=>'N', pack_val=>'N*'),
		     l2x   => undef, #CollocDB::DBFile->new(pack_key=>'N', pack_val=>'N*'),

		     ##-- data
		     xf    => undef, #CollocDB::DBFile->new(type=>'RECNO',dbopts=>{reclen=>4},pack_val=>'N'),

		     @_, ##-- user arguments
		    },
		    ref($that)||$that);
  return defined($coldb->{dbdir}) ? $coldb->open($coldb->{dbdir}) : $coldb;
}

##==============================================================================
## I/O: open/close : TODO

## $coldb_or_undef = $coldb->open($dbdir,%opts)
## $coldb_or_undef = $coldb->open()
sub open {
  my ($coldb,$dbdir,%opts) = @_;
  $coldb = $coldb->new() if (!ref($coldb));
  @$coldb{keys %opts} = values %opts;
  $dbdir //= $coldb->{dbdir};
  $coldb->close() if ($coldb->opened);
  $coldb->{dbdir} = $dbdir;
  my $flags = fcflags($coldb->{flags});

  ##-- open: truncate
  if (($flags&O_TRUNC) == O_TRUNC) {
    $flags |= O_CREAT;
    !-d $dbdir
      or remove_tree($dbdir)
	or $coldb->logconfess("open(): could not remove old $dbdir: $!");
  }

  ##-- open: create
  if (!-d $dbdir) {
    $coldb->logconfess("open(): no such directory '$dbdir'")
      if (($flags&O_CREAT) != O_CREAT);

    make_path($dbdir)
      or $coldb->logconfess("open(): could not create DB directory '$dbdir': $!");
  }

  ##-- open: header
  $coldb->loadHeader()
    or $coldb->logconfess("open(): failed to load header");

  ##-- open: w*
  delete @$coldb{qw(wenum w2x)};
  if ($coldb->{index_w}) {
    $coldb->{wenum} = CollocDB::Enum->new(base=>"$dbdir/wenum", flags=>$flags, pack_i=>'N')
      or $coldb->logconfess("open(): failed to open word-enum $dbdir/wenum.*: $!");
    $coldb->{w2x} = CollocDB::DBFile->new(file=>"$dbdir/w2x.db", flags=>$flags, pack_key=>'N', pack_val=>'N*')
      or $coldb->logconfess("open(): failed to open word-expansion map $dbdir/w2x.db.*: $!");
  }

  ##-- open: l*
  delete @$coldb{qw(lenum l2x)};
  if ($coldb->{index_l}) {
    $coldb->{lenum} = CollocDB::Enum->new(base=>"$dbdir/lenum", flags=>$flags, pack_i=>'N')
      or $coldb->logconfess("open(): failed to open lemma-enum $dbdir/lenum.*: $!");
    $coldb->{l2x} = CollocDB::DBFile->new(file=>"$dbdir/l2x.db", flags=>$flags, pack_key=>'N', pack_val=>'N*')
      or $coldb->logconfess("open(): failed to open lemma-expansion map $dbdir/l2x.db.*: $!");
  }

  ##-- open: xenum
  $coldb->{xenum} = CollocDB::Enum->new(base=>"$dbdir/xenum", flags=>$flags, pack_i=>'N', pack_s=>$coldb->{xpack})
      or $coldb->logconfess("open(): failed to open tuple-enum $dbdir/xenum.*: $!");

  ##-- open: xf
  $coldb->{xf} = CollocDB::DBFile->new(file=>"$dbdir/xf.dba", flags=>$flags, type=>'RECNO', pack_val=>'N', dbopts=>{reclen=>4})
    or $coldb->logconfess("open(): failed to open tuple-unigrams $dbdir/xf.dba: $!");

  ##-- all done
  return $coldb;
}

## @dbkeys = $coldb->dbkeys()
sub dbkeys {
  return qw(wenum lenum xenum w2x l2x xf);
}

## $coldb_or_undef = $coldb->close()
sub close {
  my $coldb = shift;
  return $coldb if (!ref($coldb));
  foreach ($coldb->dbkeys) {
    next if (!defined($coldb->{$_}));
    return undef if (!$coldb->{$_}->close());
    delete $coldb->{$_};
  }
  $coldb->{dbdir} = undef;
  return $coldb;
}

## $bool = $coldb->opened()
sub opened {
  my $coldb = shift;
  return (defined($coldb->{dbdir})
	  && !grep {!$_->opened} @$coldb{$coldb->dbkeys}
	 );
}


##==============================================================================
## Create/compile

## $bool = $coldb->create($corpus,%opts)
##  + %opts:
##     clobber %$coldb
sub create {
  my ($coldb,$corpus,%opts) = @_;
  @$coldb{keys %opts} = values %opts;
  my $flags = O_RDWR|O_CREAT|O_TRUNC;

  ##-- initialize: output directory
  my $dbdir = $coldb->{dbdir}
    or $coldb->logconfess("create() called but 'dbdir' key not set!");
  !-d $dbdir
    or remove_tree($dbdir)
      or $coldb->logconfess("create(): could not remove stale $dbdir: $!");
  make_path($dbdir)
    or $coldb->logconfess("create(): could not create DB directory $dbdir: $!");

  ##-- setup tuple pack-format
  my ($index_w,$index_l) = @$coldb{qw(index_w index_l)};
  my $xpack  = $coldb->{xpack} = ($index_w ? 'N' : '').($index_l ? 'N' : '').'n';

  ##-- initialize: enums
  my $wenum = $coldb->{wenum} = CollocDB::Enum->new(flags=>$flags, pack_i=>'N');
  my $ws2i  = $wenum->{s2i}{data};
  my $nw    = 0;
  #
  my $lenum = $coldb->{lenum} = CollocDB::Enum->new(flags=>$flags, pack_i=>'N');
  my $ls2i  = $lenum->{s2i}{data};
  my $nl    = 0;
  #
  my $xenum = $coldb->{xenum} = CollocDB::Enum->new(flags=>$flags, pack_i=>'N'); #pack_s=>'NNn'
  my $xs2i  = $xenum->{s2i}{data};
  my $nx    = 0;

  ##-- initialize: corpus token-storage (temporary)
  my $tokfile =  "$dbdir/tokens.dat";
  CORE::open(my $tokfh, ">$tokfile")
    or $coldb->logconfess("$0: open failed for $tokfile: $!");
  #my $tokpack = substr($PDL::Types::pack[$PDL::Types::typehash{PDL_L}{numval}],0,1);
  #my $tokpack = 'L';

  ##-- initialize: enums
  $coldb->vlog('info',"processing corpus files");
  my ($doc, $date,$tok,$w,$l,$wi,$li,$x,$xi);
  for ($corpus->ibegin(); $corpus->iok; $corpus->inext) {
    $coldb->vlog($coldb->{logParseFile}, "processing file ", $corpus->ifile);
    $doc  = $corpus->idocument();
    $date = $doc->{date};

    foreach $tok (@{$doc->{tokens}}) {
      ($w,$l) = @$tok;
      if ($index_w) { $wi = $ws2i->{$w} = ++$nw if (!defined($wi=$ws2i->{$w})); }
      if ($index_l) { $li = $ls2i->{$l} = ++$nl if (!defined($li=$ls2i->{$l})); }

      $x=pack($xpack,($index_w ? $wi : qw()),($index_l ? $li : qw()),$date);
      $xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x}));

      ##-- save to token-fh
      $tokfh->print($xi,"\n");
    }
  }

  ##-- close token storage
  $tokfh->close()
    or $corpus->logconfess("create(): failed to close temporary token storage file '$tokfile': $!");

  ##-- compile: wenum
  if ($index_w) {
    $coldb->vlog('info', "creating word-enum DB $dbdir/wenum.*\n");
    @{$wenum->{i2s}{data}}{values %$ws2i} = keys %$ws2i;
    $wenum->{size} = $nw;
    $wenum->saveDbFile("$dbdir/wenum")
      or $coldb->logconfess("create(): failed to save $dbdir/wenum: $!");
  } else {
    delete $coldb->{wenum};
  }

  ##-- compile: lenum
  if ($index_l) {
    $coldb->vlog('info',"creating lemma-enum DB $dbdir/lenum.*\n");
    @{$lenum->{i2s}{data}}{values %$ls2i} = keys %$ls2i;
    $lenum->{size} = $nl;
    $lenum->saveDbFile("$dbdir/lenum")
      or $coldb->logconfess("create(): failed to save $dbdir/lenum: $!");
  } else {
    delete $coldb->{lenum};
  }

  ##-- compile: xenum
  $coldb->vlog('info', "creating tuple-enum DB $dbdir/xenum.*\n");
  @{$xenum->{i2s}{data}}{values %$xs2i} = keys %$xs2i;
  $xenum->{size} = $nx;
  $xenum->saveDbFile("$dbdir/xenum")
    or $coldb->logconfess("create(): failed to save $dbdir/xenum: $!");

  ##-- expansion map: w2x
  if ($index_w) {
    $coldb->vlog('info',"creating expansion map $dbdir/w2x.db");
    my %w2xi = qw();
    while (($x,$xi)=each %$xs2i) {
      ($wi)       = unpack('N',$x);
      $w2xi{$wi} .= pack('N',$xi);
    }
    my $w2xdb = $coldb->{w2x} = CollocDB::DBFile->new(file=>"$dbdir/w2x.db", flags=>$flags, pack_key=>'N')
      or $coldb->logconfess("create(): failed to create $dbdir/w2x.db: $!");
    my $w2xdata = $w2xdb->{data};
    $w2xdata->{$_} = $w2xi{$_} foreach (sort {$a<=>$b} keys %w2xi);
  } else {
    delete $coldb->{w2x};
  }

  ##-- expansion map: l2x
  if ($index_l) {
    $coldb->vlog('info',"creating expansion map $dbdir/l2x.db");
    my %l2xi  = qw();
    my $lpack = $index_w ? '@4N' : 'N';
    while (($x,$xi)=each %$xs2i) {
      ($li)       = unpack($lpack,$x);
      $l2xi{$li} .= pack('N',$xi);
    }
    my $l2xdb = $coldb->{l2x} = CollocDB::DBFile->new(file=>"$dbdir/l2x.db", flags=>$flags, pack_key=>'N')
      or $coldb->logconfess("create(): failed to create $dbdir/l2x.db: $!");
    my $l2xdata = $l2xdb->{data};
    $l2xdata->{$_} = $l2xi{$_} foreach (sort {$a<=>$b} keys %l2xi);
    %l2xi = qw();
  } else {
    delete $coldb->{l2x};
  }

  ##-- compute unigrams
  $coldb->info("creating tuple 1-gram file $dbdir/xf.dba");
  my $xfdb = $coldb->{xf} = CollocDB::DBFile->new(file=>"$dbdir/xf.dba", type=>'RECNO', pack_val=>'N', dbopts=>{reclen=>4})
    or $coldb->logconfess("create(): could not create $dbdir/xf.dba: $!");
  my $xfdata   = $xfdb->{data};
  $xfdata->[0] = 0;
  my ($f);
  csortuc(['-n',$tokfile],
	  sub {
	    ($f,$xi) = split(' ',$_[0],2);
	    $xfdata->[$xi+0] = $f;
	  });

  ##-- save header
  $coldb->saveHeader()
    or $coldb->logconfess("create(): failed to save header: $!");

  ##-- all done
  $coldb->info("DB created.");

  ##-- cleanup
  #unlink($tokfile);

  return $coldb;
}

##--------------------------------------------------------------
## I/O: header

## @keys = $coldb->headerKeys()
##  + keys to save as header
sub headerKeys {
  my $coldb = shift;
  return qw(index_w index_l logParseFile xpack);
}

## $bool = $coldb->loadHeader()
## $bool = $coldb->loadHeader($headerFile)
sub loadHeader {
  my ($coldb,$hfile) = @_;
  $hfile //= "$coldb->{dbdir}/header.json";
  my $hdr = loadJsonFile($hfile);
  if (!defined($hdr) && (fcflags($coldb->{flags})&O_CREAT) != O_CREAT) {
    $coldb->logconfess("loadHeader() failed to load '$hfile': $!");
  }
  elsif (defined($hdr)) {
    @$coldb{keys %$hdr} = values(%$hdr);
  }
  return $coldb;
}

## $bool = $coldb->saveHeader()
## $bool = $coldb->saveHeader($headerFile)
sub saveHeader {
  my ($coldb,$hfile) = @_;
  $hfile //= "$coldb->{dbdir}/header.json";
  my $hdr  = {map {($_=>$coldb->{$_})} $coldb->headerKeys()};
  saveJsonFile($hdr, $hfile)
    or $coldb->logconfess("saveHeader() failed to save '$hfile': $!");
  return $coldb;
}


##==============================================================================
## Dump

## $bool = $collocdb->export($outdir)
## $bool = $collocdb->export()
##  + $outdir defaults to "$coldb->{dbdir}/export"
sub export {
  my ($coldb,$outdir) = @_;
  $coldb->logconfess("cannot export() an un-opened DB") if (!$coldb->opened);
  $outdir //= "$coldb->{dbdir}/export";
  $coldb->vlog("info", "exporting DB to $outdir/");

  ##-- create export directory
  -d $outdir
    or make_path($outdir)
      or $coldb->logconfess("export(): could not create export directory $outdir: $!");

  ##-- dump: header
  $coldb->saveHeader("$outdir/header.json")
    or $coldb->logconfess("export(): could not export header to $outdir/header.json: $!");

  ##-- dump: enums
  if ($coldb->{wenum}) {
    $coldb->vlog('info', "exporting word-enum file $outdir/wenum.dat");
    $coldb->{wenum}->saveTextFile("$outdir/wenum.dat")
      or $coldb->logconfess("export() failed for $outdir/wenum.dat");
  }
  if ($coldb->{lenum}) {
    $coldb->vlog('info', "exporting lemma-enum file $outdir/lenum.dat");
    $coldb->{lenum}->saveTextFile("$outdir/lenum.dat")
      or $coldb->logconfess("export() failed for $outdir/lenum.dat");
  }

  ##-- dump: tuple-enum
  $coldb->vlog('info', "exporting tuple-enum file $outdir/xenum.dat");

  ##-- dump tuple-enum (raw)
  my $xpack = $coldb->{xpack};
  $coldb->{xenum}->setFilters(pack_s=>$coldb->{xpack}, pack_i=>'N');
  $coldb->{xenum}->saveTextFile("$outdir/xenum.idat")
    or $coldb->logconfess("export failed for $outdir/xenum.idat");

  ##-- dump interpolated tuple-enum
  my (@x);
  my @ai2s  = (($coldb->{index_w} ? $coldb->{wenum}{i2s}{data} : qw()),
	       ($coldb->{index_l} ? $coldb->{lenum}{i2s}{data} : qw()),
	      );
  $coldb->{xenum}->setFilters(pack_s=>[undef,sub {
					 @x = unpack($xpack,$_);
					 $_ = join("\t", (map {$ai2s[$_]{$x[$_]}} (0..$#ai2s)),$x[$#x]);
				       }]);
  $coldb->{xenum}->saveTextFile("$outdir/xenum.sdat")
    or $coldb->logconfess("export() failed for $outdir/xenum.sdat");

  ##-- xenum: reset filters
  $coldb->{xenum}->setFilters();

  ##-- dump: w2x
  if ($coldb->{w2x}) {
    $coldb->vlog('info', "exporting word-expansion db $outdir/w2x.dat");
    $coldb->{w2x}->setFilters(pack_key=>'N',pack_val=>'N*');
    $coldb->{w2x}->saveTextFile("$outdir/w2x.dat")
      or $coldb->logconfess("export failed for $outdir/w2x.dat");
    $coldb->{w2x}->setFilters();
  }

  ##-- dump: l2x
  if ($coldb->{l2x}) {
    $coldb->vlog('info', "exporting word-expansion db $outdir/l2x.dat");
    $coldb->{l2x}->setFilters(pack_key=>'N',pack_val=>'N*');
    $coldb->{l2x}->saveTextFile("$outdir/l2x.dat")
      or $coldb->logconfess("export failed for $outdir/l2x.dat");
    $coldb->{l2x}->setFilters();
  }

  ##-- dump: xf
  if ($coldb->{xf}) {
    $coldb->vlog('info', "exporting tuple-frequency db $outdir/xf.dat");
    $coldb->{xf}->setFilters(pack_val=>'N');
    $coldb->{xf}->saveTextFile("$outdir/xf.dat")
      or $coldb->logconfess("export failed for $outdir/xf.dat");
    $coldb->{xf}->setFilters();
  }

  ##-- all done
  $coldb->vlog('info', "export complete.");
  return $coldb;
}


##==============================================================================
## Footer
1;

__END__




