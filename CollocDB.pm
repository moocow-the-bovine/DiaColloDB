## -*- Mode: CPerl -*-
## File: CollocDB.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, top-level

package CollocDB;
use CollocDB::Logger;
use CollocDB::Enum;
use CollocDB::EnumFile;
use CollocDB::EnumFile::MMap;
use CollocDB::EnumFile::FixedLen;
use CollocDB::EnumFile::FixedMap;
use CollocDB::MultiMapFile;
use CollocDB::DBFile;
use CollocDB::PackedFile;
use CollocDB::Unigrams;
use CollocDB::Cofreqs;
use CollocDB::Corpus;
use CollocDB::Utils qw(:fcntl :json :sort :pack);
use Fcntl;
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = 0.01;
our @ISA = qw(CollocDB::Logger);

## $PGOOD_DEFAULT
##  + default positive pos regex for document parsing
##  + don't use qr// here, since Storable doesn't like pre-compiled Regexps
our $PGOOD_DEFAULT   = q/^(?:N|TRUNC|VV|ADJ)/; #ITJ|FM|XY

## $PBAD_DEFAULT
##  + default negative pos regex for document parsing
our $PBAD_DEFAULT   = undef;

## $WGOOD_DEFAULT
##  + default positive word regex for document parsing
our $WGOOD_DEFAULT   = q/[[:alpha:]]/;

## $WBAD_DEFAULT
##  + default negative word regex for document parsing
our $WBAD_DEFAULT   = q/[\.]/;

## $LGOOD_DEFAULT
##  + default positive lemma regex for document parsing
our $LGOOD_DEFAULT   = undef;

## $WBAD_DEFAULT
##  + default negative lemma regex for document parsing
our $LBAD_DEFAULT   = undef;

## $ECLASS
##  + enum class
#our $ECLASS = 'CollocDB::EnumFile';
our $ECLASS = 'CollocDB::EnumFile::MMap';

## $XECLASS
##  + fixed-length enum class
#our $XECLASS = 'CollocDB::EnumFile::FixedLen';
our $XECLASS = 'CollocDB::EnumFile::FixedMap';

## $MMCLASS
##  + multimap class
our $MMCLASS = 'CollocDB::MultiMapFile';
#our $MMCLASS = 'CollocDB::MultiMapFile::MMap';

##==============================================================================
## Constructors etc.

## $coldb = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- options
##    dbdir => $dbdir,    ##-- database directory; REQUIRED
##    flags => $fcflags,  ##-- fcntl flags or open()-style mode string; default='rw'
##    index_w => $bool,   ##-- index surface word-forms? (default=0)
##    index_l => $bool,   ##-- index lemmata? (default=1)
##    bos => $bos,        ##-- special string to use for BOS, undef or empty for none (default=undef)
##    eos => $eos,        ##-- special string to use for EOS, undef or empty for none (default=undef)
##    pack_id => $fmt,    ##-- pack-format for IDs (default='N')
##    pack_f  => $fmt,    ##-- pack-format for frequencies (default='N')
##    pack_date => $fmt,  ##-- pack-format for dates (default='n')
##    pack_off => $fmt,   ##-- pack-format for file offsets (default='N')
##    pack_len => $len,   ##-- pack-format for string lengths (default='n')
##    dmax => $dmax,      ##-- maximum distance for collocation-frequencies (default=5)
##    ##
##    ##-- filtering
##    pgood  => $regex,   ##-- positive filter regex for part-of-speech tags
##    pbad   => $regex,   ##-- negative filter regex for part-of-speech tags
##    wgood  => $regex,   ##-- positive filter regex for word text
##    wbad   => $regex,   ##-- negative filter regex for word text
##    lgood  => $regex,   ##-- positive filter regex for lemma text
##    lbad   => $regex,   ##-- negative filter regex for lemma text
##    ##
##    ##-- logging
##    logOpen => $level,        ##-- log-level for open/close (default='info')
##    logParseFile => $level,   ##-- log-level for corpus file-parsing (default='trace')
##    logCreate => $level,      ##-- log-level for create messages (default='info')
##    logExport => $level,      ##-- log-level for export messages (default='trace')
##    ##
##    ##-- enums
##    wenum => $wenum,    ##-- enum: words  ($dbdir/wenum.*) : $w<=>$wi : A*<=>N
##    lenum => $wenum,    ##-- enum: lemmas ($dbdir/lenum.*) : $l<=>$wi : A*<=>N
##    xenum => $xenum,    ##-- enum: tuples ($dbdir/xenum.*) : [$wi,$li,$di]<=>$xi : NNn<=>N
##    pack_x => $fmt,     ##-- symbol pack-format for $xenum
##    ##
##    ##-- data
##    w2x   => $w2x,      ##-- multimap: word->tuples  ($dbdir/w2x.*) : $wi=>@xis  : N=>N*
##    l2x   => $l2x,      ##-- multimap: lemma->tuples ($dbdir/l2x.*) : $li=>@xis  : N=>N*
##    xf    => $xf,       ##-- ug: $xi => $f($xi) : N=>N
##    cof   => $cof,      ##-- cf: [$i1,$i2] => $f12
##   )
sub new {
  my $that = shift;
  my $coldb  = bless({
		     ##-- options
		      dbdir => undef,
		      flags => 'rw',
		      index_w => 0,
		      index_l => 1,
		      bos => undef,
		      eos => undef,
		      pack_id => 'N',
		      pack_f  => 'N',
		      pack_date => 'n',
		      pack_off => 'N',
		      pack_len =>'n',
		      #pack_x   => 'NNn',
		      dmax => 5,

		      ##-- filters
		      pgood => $PGOOD_DEFAULT,
		      pbad  => $PBAD_DEFAULT,
		      wgood => $WGOOD_DEFAULT,
		      wbad  => $WBAD_DEFAULT,
		      lgood => $LGOOD_DEFAULT,
		      lbad  => $LBAD_DEFAULT,

		      ##-- logging
		      logOpen => 'info',
		      logParseFile => 'trace',
		      logCreate => 'info',
		      logExport => 'trace',

		      ##-- enums
		      wenum => undef, #CollocDB::EnumFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      lenum => undef, #CollocDB::EnumFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      xenum => undef, #CollocDB::EnumFile::FixedLen->new(pack_i=>$coldb->{pack_id}, pack_s=>$coldb->{pack_x}),
		      w2x   => undef, #CollocDB::MultiMapFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      l2x   => undef, #CollocDB::MultiMapFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),

		      ##-- data
		      xf    => undef, #CollocDB::Unigrams->new(packas=>$coldb->{pack_f}),
		      cof   => undef, #CollocDB::Cofreqs->new(pack_f=>$pack_f, pack_i=>$pack_i, dmax=>$dmax),

		      @_,	##-- user arguments
		     },
		     ref($that)||$that);
  $coldb->{class}  = ref($coldb);
  $coldb->{pack_x} = ($coldb->{pack_id} x 2) . $coldb->{pack_date};
  return defined($coldb->{dbdir}) ? $coldb->open($coldb->{dbdir}) : $coldb;
}

sub DESTROY {
  $_[0]->close() if ($_[0]->opened);
}

##==============================================================================
## I/O: open/close

## $coldb_or_undef = $coldb->open($dbdir,%opts)
## $coldb_or_undef = $coldb->open()
sub open {
  my ($coldb,$dbdir,%opts) = @_;
  $coldb = $coldb->new() if (!ref($coldb));
  @$coldb{keys %opts} = values %opts;
  $dbdir //= $coldb->{dbdir};
  $dbdir =~ s{/$}{};
  $coldb->close() if ($coldb->opened);
  $coldb->{dbdir} = $dbdir;
  my $flags = fcflags($coldb->{flags});
  $coldb->vlog($coldb->{logOpen}, "open($dbdir)");

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
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = %efopts;
  delete @$coldb{qw(wenum w2x)};
  if ($coldb->{index_w}) {
    $coldb->{wenum} = $ECLASS->new(base=>"$dbdir/wenum", %efopts)
      or $coldb->logconfess("open(): failed to open word-enum $dbdir/wenum.*: $!");
    $coldb->{w2x} = $MMCLASS->new(base=>"$dbdir/w2x", %mmopts)
      or $coldb->logconfess("open(): failed to open word-expansion multimap $dbdir/w2x.*: $!");
  }

  ##-- open: l*
  delete @$coldb{qw(lenum l2x)};
  if ($coldb->{index_l}) {
    $coldb->{lenum} = $ECLASS->new(base=>"$dbdir/lenum", %efopts)
      or $coldb->logconfess("open(): failed to open lemma-enum $dbdir/lenum.*: $!");
    $coldb->{l2x} = $MMCLASS->new(base=>"$dbdir/l2x", %mmopts)
      or $coldb->logconfess("open(): failed to open lemma-expansion multimap $dbdir/l2x.*: $!");
  }

  ##-- open: xenum
  $coldb->{xenum} = $XECLASS->new(base=>"$dbdir/xenum", %efopts, pack_s=>$coldb->{pack_x})
      or $coldb->logconfess("open(): failed to open tuple-enum $dbdir/xenum.*: $!");

  ##-- open: xf
  $coldb->{xf} = CollocDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$coldb->{pack_f})
    or $coldb->logconfess("open(): failed to open tuple-unigrams $dbdir/xf.dba: $!");

  ##-- open: cof
  $coldb->{cof} = CollocDB::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags, pack_i=>$coldb->{pack_i}, pack_f=>$coldb->{pack_f}, dmax=>$coldb->{dmax})
    or $coldb->logconfess("open(): failed to open co-frequency file $dbdir/cof.*: $!");

  ##-- all done
  return $coldb;
}

## @dbkeys = $coldb->dbkeys()
sub dbkeys {
  return qw(wenum lenum xenum w2x l2x xf cof);
}

## $coldb_or_undef = $coldb->close()
sub close {
  my $coldb = shift;
  return $coldb if (!ref($coldb));
  $coldb->vlog($coldb->{logOpen}, "close()");
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
	  && !grep {!$_->opened} grep {defined($_)} @$coldb{$coldb->dbkeys}
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
  $coldb->vlog('info', "create(", ($coldb->{dbdir}//''), ")");
  my $dbdir = $coldb->{dbdir}
    or $coldb->logconfess("create() called but 'dbdir' key not set!");
  $dbdir =~ s{/$}{};
  !-d $dbdir
    or remove_tree($dbdir)
      or $coldb->logconfess("create(): could not remove stale $dbdir: $!");
  make_path($dbdir)
    or $coldb->logconfess("create(): could not create DB directory $dbdir: $!");

  ##-- pack-formats
  my ($index_w,$index_l) = @$coldb{qw(index_w index_l)};
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_x     = $coldb->{pack_x} = ($index_w ? $pack_id : '').($index_l ? $pack_id : '').$pack_date;
  my $pack_mmb   = "${pack_id}*";

  ##-- initialize: enums
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = %efopts;
  my $wenum = $coldb->{wenum} = $ECLASS->new(%efopts);
  my $ws2i  = $wenum->{s2i};
  my $nw    = 0;
  #
  my $lenum = $coldb->{lenum} = $ECLASS->new(%efopts);
  my $ls2i  = $lenum->{s2i};
  my $nl    = 0;
  #
  my $xenum = $coldb->{xenum} = $XECLASS->new(%efopts, pack_s=>$coldb->{pack_x});
  my $xs2i  = $xenum->{s2i};
  my $nx    = 0;

  ##-- initialize: corpus token-list (temporary)
  my $tokfile =  "$dbdir/tokens.dat";
  CORE::open(my $tokfh, ">$tokfile")
    or $coldb->logconfess("$0: open failed for $tokfile: $!");
  #my $tokpack = substr($PDL::Types::pack[$PDL::Types::typehash{PDL_L}{numval}],0,1);
  #my $tokpack = 'L';

  ##-- initialize: filter regexes
  my $pgood = $coldb->{pgood} ? qr{$coldb->{pgood}} : undef;
  my $pbad  = $coldb->{pbad}  ? qr{$coldb->{pbad}}  : undef;
  my $wgood = $coldb->{wgood} ? qr{$coldb->{wgood}} : undef;
  my $wbad  = $coldb->{wbad}  ? qr{$coldb->{wbad}}  : undef;
  my $lgood = $coldb->{lgood} ? qr{$coldb->{lgood}} : undef;
  my $lbad  = $coldb->{lbad}  ? qr{$coldb->{lbad}}  : undef;

  ##-- initialize: enums
  $coldb->vlog($coldb->{logCreate},"create(): processing corpus files");
  my ($bos,$eos) = @$coldb{qw(bos eos)};
  my ($doc, $date,$tok,$w,$p,$l,$wi,$li,$x,$xi);
  my ($last_was_eos,$bosxi,$eosxi);
  for ($corpus->ibegin(); $corpus->iok; $corpus->inext) {
    $coldb->vlog($coldb->{logParseFile}, "create(): processing file ", $corpus->ifile);
    $doc  = $corpus->idocument();
    $date = $doc->{date};

    ##-- allocate bos,eos
    undef $bosxi;
    undef $eosxi;
    foreach $w (grep {($_//'') ne ''} $bos,$eos) {
      if ($index_w) { $wi = $ws2i->{$w} = ++$nw if (!defined($wi=$ws2i->{$w})); }
      if ($index_l) { $li = $ls2i->{$w} = ++$nl if (!defined($li=$ls2i->{$w})); }
      $x=pack($pack_x,($index_w ? $wi : qw()),($index_l ? $li : qw()),$date);
      $xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x}));
      $bosxi = $xi if (defined($bos) && $w eq $bos);
      $eosxi = $xi if (defined($eos) && $w eq $eos);
    }

    ##-- iterate over tokens
    $last_was_eos = 1;
    foreach $tok (@{$doc->{tokens}}) {
      if (defined($tok)) {
	##-- normal token
	($w,$p,$l) = @$tok{qw(w p l)};

	##-- apply regex filters
	next if ((defined($pgood)    && $p !~ $pgood)
		 || (defined($pbad)  && $p =~ $pbad)
		 || (defined($wgood) && $w !~ $wgood)
		 || (defined($wbad)  && $w =~ $wbad)
		 || (defined($lgood) && $l !~ $lgood)
		 || (defined($lbad)  && $l =~ $lbad));

	if ($index_w) { $wi = $ws2i->{$w} = ++$nw if (!defined($wi=$ws2i->{$w})); }
	if ($index_l) { $li = $ls2i->{$l} = ++$nl if (!defined($li=$ls2i->{$l})); }

	$x  = pack($pack_x,($index_w ? $wi : qw()),($index_l ? $li : qw()),$date);
	$xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x}));

	$tokfh->print(($last_was_eos && defined($bosxi) ? ($bosxi,"\n") : qw()), $xi,"\n");
	$last_was_eos = 0;
      }
      elsif (!$last_was_eos) {
	##-- eos
	$tokfh->print((defined($eosxi) ? ($eosxi,"\n") : qw()), "\n");
	$last_was_eos = 1;
      }
    }
  }

  ##-- close token storage
  $tokfh->close()
    or $corpus->logconfess("create(): failed to close temporary token storage file '$tokfile': $!");

  ##-- compile: wenum
  if ($index_w) {
    $coldb->vlog($coldb->{logCreate}, "create(): creating word-enum DB $dbdir/wenum.*");
    $wenum->fromHash($ws2i);
    $wenum->save("$dbdir/wenum")
      or $coldb->logconfess("create(): failed to save $dbdir/wenum: $!");
  } else {
    delete $coldb->{wenum};
  }

  ##-- compile: lenum
  if ($index_l) {
    $coldb->vlog($coldb->{logCreate},"create(): creating lemma-enum DB $dbdir/lenum.*");
    $lenum->fromHash($ls2i);
    $lenum->save("$dbdir/lenum")
      or $coldb->logconfess("create(): failed to save $dbdir/lenum: $!");
  } else {
    delete $coldb->{lenum};
  }

  ##-- compile: xenum
  $coldb->vlog($coldb->{logCreate}, "create(): creating tuple-enum DB $dbdir/xenum.*");
  $xenum->fromHash($xs2i);
  $xenum->save("$dbdir/xenum")
    or $coldb->logconfess("create(): failed to save $dbdir/xenum: $!");

  ##-- expansion map: w2x
  if ($index_w) {
    $coldb->vlog($coldb->{logCreate},"create(): creating word-expansion multimap $dbdir/w2x.*");
    my @w2xi = qw();
    while (($x,$xi)=each %$xs2i) {
      ($wi)       = unpack($pack_id,$x);
      $w2xi[$wi] .= pack($pack_id,$xi);
    }
    $_ = pack($pack_mmb, sort {$a<=>$b} unpack($pack_mmb,$_//'')) foreach (@w2xi); ##-- ensure multimap target-sets are sorted
    my $w2x = $coldb->{w2x} = $MMCLASS->new(base=>"$dbdir/w2x", %mmopts)
      or $coldb->logconfess("create(): failed to create $dbdir/w2x.*: $!");
    $w2x->fromArray(\@w2xi)
      or $coldb->logconfess("create(): failed to populate $dbdir/w2x.*: $!");
    $w2x->flush()
      or $coldb->logconfess("create(): failed to flush $dbdir/w2x.*: $!");
  } else {
    delete $coldb->{w2x};
  }

  ##-- expansion map: l2x
  if ($index_l) {
    $coldb->vlog($coldb->{logCreate},"create(): creating expansion map $dbdir/l2x.db");
    my @l2xi  = qw();
    my $lpack = $index_w ? ("\@".packsize($pack_id).$pack_id) : $pack_id;
    while (($x,$xi)=each %$xs2i) {
      ($li)       = unpack($lpack,$x);
      $l2xi[$li] .= pack($pack_id,$xi);
    }
    $_ = pack($pack_mmb, sort {$a<=>$b} unpack($pack_mmb,$_//'')) foreach (@l2xi); ##-- ensure multimap target-sets are sorted
    my $l2x = $coldb->{l2x} = $MMCLASS->new(base=>"$dbdir/l2x", %mmopts)
      or $coldb->logconfess("create(): failed to create $dbdir/l2x.*: $!");
    $l2x->fromArray(\@l2xi)
      or $coldb->logconfess("create(): failed to populate $dbdir/l2x.*: $!");
    $l2x->flush()
      or $coldb->logconfess("create(): failed to flush $dbdir/l2x.*: $!");
  } else {
    delete $coldb->{l2x};
  }

  ##-- compute unigrams
  $coldb->info("creating tuple 1-gram file $dbdir/xf.dba");
  my $xfdb = $coldb->{xf} = CollocDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$pack_f)
    or $coldb->logconfess("create(): could not create $dbdir/xf.dba: $!");
  $xfdb->setsize($xenum->{size})
    or $coldb->logconfess("create(): could not set unigram db size = $xenum->{size}: $!");
  $xfdb->create($tokfile)
    or $coldb->logconfess("create(): failed to create unigram db: $!");

  ##-- compute collocation frequencies
  $coldb->info("creating co-frequency db $dbdir/cof.*");
  my $cof = $coldb->{cof} = CollocDB::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags, pack_i=>$pack_id, pack_f=>$pack_f)
    or $coldb->logconfess("create(): failed to open co-frequency db $dbdir/cof.*: $!");
  $cof->create($tokfile, dmax=>$coldb->{dmax})
    or $coldb->logconfess("create(): failed to create co-frequency db: $!");

  ##-- save header
  $coldb->saveHeader()
    or $coldb->logconfess("create(): failed to save header: $!");

  ##-- all done
  $coldb->vlog($coldb->{logCreate}, "create(): DB $dbdir created.");

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
  return grep {!ref($coldb->{$_}) && $_ !~ m{^(?:dbdir$|flags$|perms$|log)}} keys %$coldb;
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

## $bool = $collocdb->export()
## $bool = $collocdb->export($outdir,%opts)
##  + $outdir defaults to "$coldb->{dbdir}/export"
##  + %opts:
##     export_sdat => $bool,  ##-- whether to export *.sdat (stringified tuple files for debugging; default=0)
##     export_cof  => $bool,  ##-- do/don't export cof.* (default=do)
sub export {
  my ($coldb,$outdir,%opts) = @_;
  $coldb->logconfess("cannot export() an un-opened DB") if (!$coldb->opened);
  $outdir //= "$coldb->{dbdir}/export";
  $coldb->vlog('info', "export($outdir)");

  ##-- options
  my $export_sdat = exists($opts{export_sdat}) ? $opts{export_sdat} : 0;
  my $export_cof  = exists($opts{export_cof}) ? $opts{export_cof} : 1;

  ##-- create export directory
  -d $outdir
    or make_path($outdir)
      or $coldb->logconfess("export(): could not create export directory $outdir: $!");

  ##-- dump: header
  $coldb->saveHeader("$outdir/header.json")
    or $coldb->logconfess("export(): could not export header to $outdir/header.json: $!");

  ##-- dump: enums
  if ($coldb->{wenum}) {
    $coldb->vlog($coldb->{logExport}, "export(): exporting word-enum file $outdir/wenum.dat");
    $coldb->{wenum}->saveTextFile("$outdir/wenum.dat")
      or $coldb->logconfess("export() failed for $outdir/wenum.dat");
  }
  if ($coldb->{lenum}) {
    $coldb->vlog($coldb->{logExport}, "export(): exporting lemma-enum file $outdir/lenum.dat");
    $coldb->{lenum}->saveTextFile("$outdir/lenum.dat")
      or $coldb->logconfess("export() failed for $outdir/lenum.dat");
  }

  ##-- dump: tuple-enum: raw
  my $pack_x = $coldb->{pack_x};
  $coldb->vlog($coldb->{logExport}, "export(): exporting raw tuple-enum file $outdir/xenum.dat");
  $coldb->{xenum}->saveTextFile("$outdir/xenum.dat", pack_s=>$pack_x)
    or $coldb->logconfess("export failed for $outdir/xenum.dat");

  ##-- dump: tuple-enum: stringified
  my (@x,@ai2s); ##-- these are re-used for cof.sdat
  if ($export_sdat) {
    $coldb->vlog($coldb->{logExport}, "export(): preparing tuple-stringification structures");
    @ai2s  = (($coldb->{index_w} ? $coldb->{wenum}->toArray : qw()),
	      ($coldb->{index_l} ? $coldb->{lenum}->toArray : qw()),
	     );
    my $xs2wld = sub {
      @x = unpack($pack_x,$_[0]);
      return join("\t", (map {$ai2s[$_][$x[$_]//0]//''} (0..$#ai2s)),$x[$#x]//'');
    };

    $coldb->vlog($coldb->{logExport}, "export(): exporting stringified tuple-enum file $outdir/xenum.sdat");
    $coldb->{xenum}->saveTextFile("$outdir/xenum.sdat", pack_s=>$xs2wld)
      or $coldb->logconfess("export() failed for $outdir/xenum.sdat");
  }

  ##-- dump: w2x
  if ($coldb->{w2x}) {
    $coldb->vlog($coldb->{logExport}, "export(): exporting word-expansion multimap $outdir/w2x.dat");
    $coldb->{w2x}->saveTextFile("$outdir/w2x.dat")
      or $coldb->logconfess("export() failed for $outdir/w2x.dat");
  }

  ##-- dump: l2x
  if ($coldb->{l2x}) {
    $coldb->vlog($coldb->{logExport}, "export(): exporting lemma-expansion multimap $outdir/l2x.dat");
    $coldb->{l2x}->saveTextFile("$outdir/l2x.dat")
      or $coldb->logconfess("export() failed for $outdir/l2x.dat");
  }

  ##-- dump: xf
  if ($coldb->{xf}) {
    $coldb->vlog($coldb->{logExport}, "export(): exporting tuple-frequency db $outdir/xf.dat");
    $coldb->{xf}->setFilters($coldb->{pack_f});
    $coldb->{xf}->saveTextFile("$outdir/xf.dat", keys=>1)
      or $coldb->logconfess("export failed for $outdir/xf.dat");
    $coldb->{xf}->setFilters();
  }

  ##-- dump: cof
  if ($coldb->{cof} && $export_cof) {
    $coldb->vlog($coldb->{logExport}, "export(): exporting raw co-frequency db $outdir/cof.dat");
    $coldb->{cof}->saveTextFile("$outdir/cof.dat")
      or $coldb->logconfess("export failed for $outdir/cof.dat");

    if ($export_sdat) {
      $coldb->vlog($coldb->{logExport}, "export(): preparing tuple-stringification index");
      my $xs2i   = $coldb->{xenum}->toArray;
      my $xi2wld = sub {
	@x = unpack($pack_x,$xs2i->[$_[0]]//'');
	return join("\t", (map {$ai2s[$_][$x[$_]//0]//''} (0..$#ai2s)),$x[$#x]//'');
      };
      $coldb->vlog($coldb->{logExport}, "export(): exporting stringified co-frequency db $outdir/cof.sdat");
      $coldb->{cof}->saveTextFile("$outdir/cof.sdat", i2s=>$xi2wld)
	or $coldb->logconfess("export failed for $outdir/cof.sdat");
    }
  }

  ##-- all done
  $coldb->vlog($coldb->{logExport}, "export(): export to $outdir complete.");
  return $coldb;
}


##==============================================================================
## Footer
1;

__END__




