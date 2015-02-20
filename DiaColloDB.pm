## -*- Mode: CPerl -*-
## File: DiaColloDB.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, top-level

package DiaColloDB;
use DiaColloDB::Logger;
use DiaColloDB::EnumFile;
use DiaColloDB::EnumFile::MMap;
use DiaColloDB::EnumFile::FixedLen;
use DiaColloDB::EnumFile::FixedMap;
use DiaColloDB::MultiMapFile;
use DiaColloDB::PackedFile;
use DiaColloDB::Unigrams;
use DiaColloDB::Cofreqs;
use DiaColloDB::Profile;
use DiaColloDB::Profile::Multi;
use DiaColloDB::Corpus;
use DiaColloDB::Persistent;
use DiaColloDB::Utils qw(:fcntl :json :sort :pack :regex);
use Fcntl;
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = 0.01;
our @ISA = qw(DiaColloDB::Persistent);

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
#our $ECLASS = 'DiaColloDB::EnumFile';
our $ECLASS = 'DiaColloDB::EnumFile::MMap';

## $XECLASS
##  + fixed-length enum class
#our $XECLASS = 'DiaColloDB::EnumFile::FixedLen';
our $XECLASS = 'DiaColloDB::EnumFile::FixedLen::MMap';

## $MMCLASS
##  + multimap class
our $MMCLASS = 'DiaColloDB::MultiMapFile';
#our $MMCLASS = 'DiaColloDB::MultiMapFile::MMap';

##==============================================================================
## Constructors etc.

## $coldb = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- options
##    dbdir => $dbdir,    ##-- database directory; REQUIRED
##    flags => $fcflags,  ##-- fcntl flags or open()-style mode string; default='r'
##    #index_w => $bool,   ##-- index surface word-forms? (default=0) : DISABLED
##    index_l => $bool,   ##-- index lemmata? (default=1) : REDUNDANT
##    bos => $bos,        ##-- special string to use for BOS, undef or empty for none (default=undef)
##    eos => $eos,        ##-- special string to use for EOS, undef or empty for none (default=undef)
##    pack_id => $fmt,    ##-- pack-format for IDs (default='N')
##    pack_f  => $fmt,    ##-- pack-format for frequencies (default='N')
##    pack_date => $fmt,  ##-- pack-format for dates (default='n')
##    pack_off => $fmt,   ##-- pack-format for file offsets (default='N')
##    pack_len => $len,   ##-- pack-format for string lengths (default='n')
##    dmax => $dmax,      ##-- maximum distance for collocation-frequencies (default=5)
##    cfmin => $cfmin,    ##-- minimum co-occurrence frequency for Cofreqs (default=2)
##    keeptmp => $bool,   ##-- keep temporary files? (default=0)
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
##    logCreate => $level,      ##-- log-level for create messages (default='info')
##    logCorpusFile => $level,  ##-- log-level for corpus file-parsing (default='trace')
##    logCorpusFileN => $N,     ##-- log corpus file-parsing only for every N files (0 for none; default:undef ~ $corpus->size()/100)
##    logExport => $level,      ##-- log-level for export messages (default='info')
##    logProfile => $level,     ##-- log-level for verbose profiling messages (default='trace')
##    ##
##    ##-- enums
##    #wenum => $wenum,    ##-- enum: words  ($dbdir/wenum.*) : $w<=>$wi : A*<=>N # DISABLED
##    lenum => $wenum,    ##-- enum: lemmas ($dbdir/lenum.*) : $l<=>$li : A*<=>N
##    xenum => $xenum,    ##-- enum: tuples ($dbdir/xenum.*) : [$li,$di]<=>$xi : NNn<=>N
##    pack_x => $fmt,     ##-- symbol pack-format for $xenum : "${pack_id}${pack_date}"
##    ##
##    ##-- data
##    #w2x   => $w2x,      ##-- multimap: word->tuples  ($dbdir/w2x.*) : $wi=>@xis  : N=>N* # DISABLED
##    l2x   => $l2x,      ##-- multimap: lemma->tuples ($dbdir/l2x.*) : $li=>@xis  : N=>N*
##    xf    => $xf,       ##-- ug: $xi => $f($xi) : N=>N
##    cof   => $cof,      ##-- cf: [$xi1,$xi2] => $f12
##   )
sub new {
  my $that = shift;
  my $coldb  = bless({
		     ##-- options
		      dbdir => undef,
		      flags => 'r',
		      #index_w => 0,
		      #index_l => 1,
		      bos => undef,
		      eos => undef,
		      pack_id => 'N',
		      pack_f  => 'N',
		      pack_date => 'n',
		      pack_off => 'N',
		      pack_len =>'n',
		      dmax => 5,
		      cfmin => 2,
		      keeptmp => 0,

		      ##-- filters
		      pgood => $PGOOD_DEFAULT,
		      pbad  => $PBAD_DEFAULT,
		      wgood => $WGOOD_DEFAULT,
		      wbad  => $WBAD_DEFAULT,
		      lgood => $LGOOD_DEFAULT,
		      lbad  => $LBAD_DEFAULT,

		      ##-- logging
		      logOpen => 'info',
		      logCreate => 'info',
		      logCorpusFile => 'trace',
		      logCorpusFileN => undef,
		      logExport => 'info',
		      logProfile => 'trace',

		      ##-- enums
		      #wenum => undef, #DiaColloDB::EnumFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      lenum => undef, #DiaColloDB::EnumFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      xenum => undef, #DiaColloDB::EnumFile::FixedLen->new(pack_i=>$coldb->{pack_id}, pack_s=>$coldb->{pack_x}),
		      #w2x   => undef, #DiaColloDB::MultiMapFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      l2x   => undef, #DiaColloDB::MultiMapFile->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),

		      ##-- data
		      xf    => undef, #DiaColloDB::Unigrams->new(packas=>$coldb->{pack_f}),
		      cof   => undef, #DiaColloDB::Cofreqs->new(pack_f=>$pack_f, pack_i=>$pack_i, dmax=>$dmax, fmin=>$cfmin),

		      @_,	##-- user arguments
		     },
		     ref($that)||$that);
  $coldb->{class}  = ref($coldb);
  $coldb->{pack_x} = $coldb->{pack_id} . $coldb->{pack_date};
  if (defined($coldb->{dbdir})) {
    ##-- avoid initial close() if called with dbdir=>$dbdir argument
    my $dbdir = $coldb->{dbdir};
    delete $coldb->{dbdir};
    return $coldb->open($dbdir);
  }
  return $coldb;
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
  DiaColloDB::Logger->ensureLog();
  $coldb = $coldb->new() if (!ref($coldb));
  @$coldb{keys %opts} = values %opts;
  $dbdir //= $coldb->{dbdir};
  $dbdir =~ s{/$}{};
  $coldb->close() if ($coldb->opened);
  $coldb->{dbdir} = $dbdir;
  my $flags = fcflags($coldb->{flags});
  $coldb->vlog($coldb->{logOpen}, "open($dbdir)");

  ##-- open: truncate
  if (fctrunc($flags)) {
    $flags |= O_CREAT;
    !-d $dbdir
      or remove_tree($dbdir)
	or $coldb->logconfess("open(): could not remove old $dbdir: $!");
  }

  ##-- open: create
  if (!-d $dbdir) {
    $coldb->logconfess("open(): no such directory '$dbdir'") if (!fccreat($flags));
    make_path($dbdir)
      or $coldb->logconfess("open(): could not create DB directory '$dbdir': $!");
  }

  ##-- open: header
  $coldb->loadHeader()
    or $coldb->logconfess("open(): failed to load header file", $coldb->headerFile, ": $!");

  ##-- open: common options
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = %efopts;

  ##-- open: l*
  $coldb->{lenum} = $ECLASS->new(base=>"$dbdir/lenum", %efopts)
    or $coldb->logconfess("open(): failed to open lemma-enum $dbdir/lenum.*: $!");
  $coldb->{l2x} = $MMCLASS->new(base=>"$dbdir/l2x", %mmopts)
    or $coldb->logconfess("open(): failed to open lemma-expansion multimap $dbdir/l2x.*: $!");

  ##-- open: xenum
  $coldb->{xenum} = $XECLASS->new(base=>"$dbdir/xenum", %efopts, pack_s=>$coldb->{pack_x})
      or $coldb->logconfess("open(): failed to open tuple-enum $dbdir/xenum.*: $!");

  ##-- open: xf
  $coldb->{xf} = DiaColloDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$coldb->{pack_f})
    or $coldb->logconfess("open(): failed to open tuple-unigrams $dbdir/xf.dba: $!");
  $coldb->{xf}{N} = $coldb->{xN} if ($coldb->{xN} && !$coldb->{xf}{N});

  ##-- open: cof
  $coldb->{cof} = DiaColloDB::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
					 pack_i=>$coldb->{pack_id}, pack_f=>$coldb->{pack_f},
					 dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
					)
    or $coldb->logconfess("open(): failed to open co-frequency file $dbdir/cof.*: $!");

  ##-- all done
  return $coldb;
}

## @dbkeys = $coldb->dbkeys()
sub dbkeys {
  return qw(lenum xenum l2x xf cof); #wenum w2x
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
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_x     = $coldb->{pack_x} = $pack_id.$pack_date; ##-- pack("${pack_id}${pack_date}", $li, $date)
  my $pack_mmb   = "${pack_id}*";

  ##-- initialize: enums
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = %efopts;
  #
  my $lenum = $coldb->{lenum} = $ECLASS->new(%efopts);
  my $ls2i  = $lenum->{s2i};
  my $nl    = 0;
  #
  my $xenum = $coldb->{xenum} = $XECLASS->new(%efopts, pack_s=>$pack_x);
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

  ##-- initialize: logging
  my $logFileN = $coldb->{logCorpusFileN};
  my $nfiles   = $corpus->size();
  if (!defined($logFileN)) {
    $logFileN = int($nfiles / 100);
    $logFileN = 1 if ($logFileN < 1);
  }

  ##-- initialize: enums
  $coldb->vlog($coldb->{logCreate},"create(): processing $nfiles corpus file(s)");
  my ($bos,$eos) = @$coldb{qw(bos eos)};
  my ($doc, $date,$tok,$w,$p,$l,$wi,$li,$x,$xi, $filei);
  my ($last_was_eos,$bosxi,$eosxi);
  for ($corpus->ibegin(); $corpus->iok; $corpus->inext) {
    $coldb->vlog($coldb->{logCorpusFile}, sprintf("create(): processing files [%3d%%]: %s", 100*$filei/$nfiles, $corpus->ifile))
      if ($logFileN && ($filei++ % $logFileN)==0);
    $doc  = $corpus->idocument();
    $date = $doc->{date};

    ##-- allocate bos,eos
    undef $bosxi;
    undef $eosxi;
    foreach $w (grep {($_//'') ne ''} $bos,$eos) {
      $li = $ls2i->{$w} = ++$nl if (!defined($li=$ls2i->{$w}));
      $x  = pack($pack_x,$li,$date);
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

	$li = $ls2i->{$l} = ++$nl if (!defined($li=$ls2i->{$l}));

	$x  = pack($pack_x, $li,$date);
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

  ##-- compile: lenum
  $coldb->vlog($coldb->{logCreate},"create(): creating lemma-enum DB $dbdir/lenum.*");
  $lenum->fromHash($ls2i);
  $lenum->save("$dbdir/lenum")
    or $coldb->logconfess("create(): failed to save $dbdir/lenum: $!");

  ##-- compile: xenum
  $coldb->vlog($coldb->{logCreate}, "create(): creating tuple-enum DB $dbdir/xenum.*");
  $xenum->fromHash($xs2i);
  $xenum->save("$dbdir/xenum")
    or $coldb->logconfess("create(): failed to save $dbdir/xenum: $!");

  ##-- expansion map: l2x
  {
    $coldb->vlog($coldb->{logCreate},"create(): creating lemma-expansion multimap $dbdir/l2x.*");
    my @l2xi  = qw();
    while (($x,$xi)=each %$xs2i) {
      ($li)       = unpack($pack_id,$x);
      $l2xi[$li] .= pack($pack_id,$xi);
    }
    $_ = pack($pack_mmb, sort {$a<=>$b} unpack($pack_mmb,$_//'')) foreach (@l2xi); ##-- ensure multimap target-sets are sorted
    my $l2x = $coldb->{l2x} = $MMCLASS->new(base=>"$dbdir/l2x", %mmopts)
      or $coldb->logconfess("create(): failed to create $dbdir/l2x.*: $!");
    $l2x->fromArray(\@l2xi)
      or $coldb->logconfess("create(): failed to populate $dbdir/l2x.*: $!");
    $l2x->flush()
      or $coldb->logconfess("create(): failed to flush $dbdir/l2x.*: $!");
  }

  ##-- compute unigrams
  $coldb->info("creating tuple 1-gram file $dbdir/xf.dba");
  my $xfdb = $coldb->{xf} = DiaColloDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$pack_f)
    or $coldb->logconfess("create(): could not create $dbdir/xf.dba: $!");
  $xfdb->setsize($xenum->{size})
    or $coldb->logconfess("create(): could not set unigram db size = $xenum->{size}: $!");
  $xfdb->create($tokfile)
    or $coldb->logconfess("create(): failed to create unigram db: $!");

  ##-- compute collocation frequencies
  $coldb->info("creating co-frequency db $dbdir/cof.* [dmax=$coldb->{dmax}, fmin=$coldb->{cfmin}]");
  my $cof = $coldb->{cof} = DiaColloDB::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
						   pack_i=>$pack_id, pack_f=>$pack_f,
						   dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
						   keeptmp=>$coldb->{keeptmp},
						  )
    or $coldb->logconfess("create(): failed to open co-frequency db $dbdir/cof.*: $!");
  $cof->create($tokfile)
    or $coldb->logconfess("create(): failed to create co-frequency db: $!");

  ##-- save header
  $coldb->saveHeader()
    or $coldb->logconfess("create(): failed to save header: $!");

  ##-- all done
  $coldb->vlog($coldb->{logCreate}, "create(): DB $dbdir created.");

  ##-- cleanup
  unlink($tokfile) if (!$coldb->{keeptmp});

  return $coldb;
}

##--------------------------------------------------------------
## I/O: header
##  + largely INHERITED from DiaColloDB::Persistent

## @keys = $coldb->headerKeys()
##  + keys to save as header
sub headerKeys {
  return grep {!ref($_[0]{$_}) && $_ !~ m{^(?:dbdir$|flags$|perms$|log)}} keys %{$_[0]};
}

## $bool = $coldb->loadHeaderData()
## $bool = $coldb->loadHeaderData($data)
sub loadHeaderData {
  my ($coldb,$hdr) = @_;
  if (!defined($hdr) && !fccreat($coldb->{flags})) {
    $coldb->logconfess("loadHeader() failed to load header data from ", $coldb->headerFile, ": $!");
  }
  elsif (defined($hdr)) {
    return $coldb->SUPER::loadHeaderData($hdr);
  }
  return $coldb;
}

## $bool = $coldb->saveHeader()
## $bool = $coldb->saveHeader($headerFile)
##  + INHERITED from DiaColloDB::Persistent

##==============================================================================
## Export/Import

## $bool = $coldb->dbexport()
## $bool = $coldb->dbexport($outdir,%opts)
##  + $outdir defaults to "$coldb->{dbdir}/export"
##  + %opts:
##     export_sdat => $bool,  ##-- whether to export *.sdat (stringified tuple files for debugging; default=0)
##     export_cof  => $bool,  ##-- do/don't export cof.* (default=do)
sub dbexport {
  my ($coldb,$outdir,%opts) = @_;
  $coldb->logconfess("cannot dbexport() an un-opened DB") if (!$coldb->opened);
  $outdir //= "$coldb->{dbdir}/export";
  $coldb->vlog('info', "export($outdir)");

  ##-- options
  my $export_sdat = exists($opts{export_sdat}) ? $opts{export_sdat} : 0;
  my $export_cof  = exists($opts{export_cof}) ? $opts{export_cof} : 1;

  ##-- create export directory
  -d $outdir
    or make_path($outdir)
      or $coldb->logconfess("dbexport(): could not create export directory $outdir: $!");

  ##-- dump: header
  $coldb->saveHeader("$outdir/header.json")
    or $coldb->logconfess("dbexport(): could not export header to $outdir/header.json: $!");

  ##-- dump: load enums
  $coldb->vlog($coldb->{logExport}, "dbexport(): loading enums to memory");
  $coldb->{lenum}->load() if ($coldb->{lenum} && !$coldb->{lenum}->loaded);
  $coldb->{xenum}->load() if ($coldb->{xenum} && !$coldb->{xenum}->loaded);

  ##-- dump: enums
  $coldb->vlog($coldb->{logExport}, "dbexport(): exporting lemma-enum file $outdir/lenum.dat");
  $coldb->{lenum}->saveTextFile("$outdir/lenum.dat")
    or $coldb->logconfess("dbexport() failed for $outdir/lenum.dat");

  ##-- dump: tuple-enum: raw
  my $pack_x = $coldb->{pack_x};
  $coldb->vlog($coldb->{logExport}, "dbexport(): exporting raw tuple-enum file $outdir/xenum.dat");
  $coldb->{xenum}->saveTextFile("$outdir/xenum.dat", pack_s=>$pack_x)
    or $coldb->logconfess("export failed for $outdir/xenum.dat");

  ##-- dump: tuple-enum: stringified
  if ($export_sdat) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): preparing tuple-stringification structures");
    my ($li,$d);
    my $li2s   = $coldb->{lenum}->toArray;
    my $xs2txt = sub {
      ($li,$d) = unpack($pack_x,$_[0]);
      return join("\t", $li2s->[$li//0]//'', $d//0);
    };

    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting stringified tuple-enum file $outdir/xenum.sdat");
    $coldb->{xenum}->saveTextFile("$outdir/xenum.sdat", pack_s=>$xs2txt)
      or $coldb->logconfess("dbexport() failed for $outdir/xenum.sdat");
  }

  ##-- dump: l2x
  if ($coldb->{l2x}) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting lemma-expansion multimap $outdir/l2x.dat");
    $coldb->{l2x}->saveTextFile("$outdir/l2x.dat")
      or $coldb->logconfess("dbexport() failed for $outdir/l2x.dat");
  }

  ##-- dump: xf
  if ($coldb->{xf}) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting tuple-frequency db $outdir/xf.dat");
    $coldb->{xf}->setFilters($coldb->{pack_f});
    $coldb->{xf}->saveTextFile("$outdir/xf.dat", keys=>1)
      or $coldb->logconfess("export failed for $outdir/xf.dat");
    $coldb->{xf}->setFilters();
  }

  ##-- dump: cof
  if ($coldb->{cof} && $export_cof) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting raw co-frequency db $outdir/cof.dat");
    $coldb->{cof}->saveTextFile("$outdir/cof.dat")
      or $coldb->logconfess("export failed for $outdir/cof.dat");

    if ($export_sdat) {
      $coldb->vlog($coldb->{logExport}, "dbexport(): preparing tuple-stringification index");
      my ($li,$d);
      my $xi2s   = $coldb->{xenum}->toArray;
      my $li2s   = $coldb->{lenum}->toArray;
      my $xi2txt = sub {
	($li,$d) = unpack($pack_x,$xi2s->[$_[0]//'']//'');
	return join("\t", $li2s->[$li//0]//'', $d//0);
      };
      $coldb->vlog($coldb->{logExport}, "dbexport(): exporting stringified co-frequency db $outdir/cof.sdat");
      $coldb->{cof}->saveTextFile("$outdir/cof.sdat", i2s=>$xi2txt)
	or $coldb->logconfess("export failed for $outdir/cof.sdat");
    }
  }

  ##-- all done
  $coldb->vlog($coldb->{logExport}, "dbexport(): export to $outdir complete.");
  return $coldb;
}

## $coldb = $coldb->dbimport()
## $coldb = $coldb->dbimport($txtdir,%opts)
##  + import ColocDB data from $txtdir
##  + TODO
sub dbimport {
  my ($coldb,$txtdir,%opts) = @_;
  $coldb = $coldb->new() if (!ref($coldb));
  $coldb->logconfess("dbimport() not yet implemented");
}

##==============================================================================
## Profiling

##--------------------------------------------------------------
## Profiling: Co-Frequencies

## $mprf = $coldb->xfprofile(%opts)
##  + get unigram frequency profile for selected items as a DiaColloDB::Profile::Multi object
##  + really just wraps $coldb->profile('xf', %opts)
##  + %opts: see profile() method
sub xfprofile {
  return $_[0]->profile('xf',@_[1..$#_]);
}


## $mprf = $coldb->coprofile(%opts)
##  + get co-frequency profile for selected items as a DiaColloDB::Profile::Multi object
##  + really just wraps $coldb->profile('cof', %opts)
##  + %opts: see profile() method
sub coprofile {
  return $_[0]->profile('cof',@_[1..$#_]);
}

## $mprf = $coldb->profile($relation, %opts)
##  + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts:
##    (
##     ##-- selection parameters
##     lemma => $lemma1,          ##-- string or array or regex "/REGEX/[gi]*"        : REQUIRED
##     date  => $date1,           ##-- string or array or range "MIN-MAX" (inclusive) : default=all
##     ##
##     ##-- aggregation parameters
##     #groupby => $attrs,         ##-- string or array; ("lemma"|"date")* : default=lemma,date : NYI
##     slice   => $slice,         ##-- date slice (default=1, 0 for global profile)
##     ##
##     ##-- scoring and trimming parameters
##     eps     => $eps,           ##-- smoothing constant (default=0)
##     score   => $func,          ##-- scoring function ("f"|"fm"|"mi"|"ld") : default="f"
##     kbest   => $k,             ##-- return only $k best collocates per date (slice) : default=-1:all
##     cutoff  => $cutoff,        ##-- minimum score
##     ##
##     ##-- profiling and debugging parameters
##     strings => $bool,          ##-- do/don't stringify (default=do)
sub profile {
  my ($coldb,$rel,%opts) = @_;

  ##-- common variables
  my ($logProfile,$lenum,$xenum,$l2x) = @$coldb{qw(logProfile lenum xenum l2x)};
  my ($l,$li,$lxids,@xids);
  my $lemma   = $opts{lemma} // '';
  my $date    = $opts{date}  // '';
  my $slice   = $opts{slice} // 1;
  my $score   = $opts{score} // 'f';
  my $eps     = $opts{eps} // 0;
  my $kbest   = $opts{kbest} // -1;
  my $cutoff  = $opts{cutoff} // '';
  my $strings = $opts{strings} // 1;

  ##-- debug
  $coldb->debug("profile(lemma='$lemma', date='$date', slice=$slice, score=$score, eps=$eps, kbest=$kbest, cutoff=$cutoff)");

  ##-- sanity check(s)
  if (!UNIVERSAL::can($coldb->{$rel},'profile')) {
    if ($rel =~ m/^(?:xf|f?1|ug)$/) {
      $rel = 'xf';
    }
    elsif ($rel =~ m/^(?:f?1?2$|c)/) {
      $rel = 'cof';
    }
    else {
      $coldb->logwarn("profile(): unknown relation '$rel'");
      return undef;
    }
  }
  if ($lemma eq '') {
    $coldb->logwarn($coldb->{error}="profile(): missing required parameter 'lemma'");
    return undef;
  }
  if (!UNIVERSAL::can($coldb->{$rel},'profile')) {
    $coldb->logwarn($coldb->{error}="profile(): unknown relation '$rel'");
    return undef;
  }

  ##-- prepare: get target lemmas
  my ($lis);
  if (UNIVERSAL::isa($lemma,'ARRAY')) {
    ##-- lemmas: array
    $coldb->vlog($logProfile, "profile(): get target lemmata (ARRAY)");
    $lis = [map {$lenum->s2i($_)} @$lemma];
  }
  elsif ($lemma =~ m{^/}) {
    ##-- lemmas: regex
    $coldb->vlog($logProfile, "profile(): get target lemmata (REGEX)");
    $lis = $lenum->re2i($lemma);
  }
  else {
    ##-- lemmas: space-separated literals
    $coldb->vlog($logProfile, "profile(): get target lemmata (STRINGS)");
    $lis = [grep {defined($_)} map {$lenum->s2i($_)} grep {($_//'') ne ''} map {s{\\(.)}{$1}g; $_} split(/(?:(?<!\\)[\,\s])+/,$lemma)];
  }
  if (!@$lis) {
    $coldb->logwarn($coldb->{error}="profile(): no lemmata matching user query '$lemma'");
    return undef;
  }

  ##-- prpare: map lemmas => tuple-ids
  $coldb->vlog($logProfile, "profile(): get target tuple IDs");
  my $xis = [sort {$a<=>$b} map {@{$l2x->fetch($_)}} @$lis];

  ##-- prepare: parse and filter tuples
  $coldb->vlog($logProfile, "profile(): parse and filter target tuples");
  my $dfilter = undef;
  if ($date && $date =~ /^\//) {
    my $dre  = regex($date);
    $dfilter = sub { $_[0] =~ $dre };
  }
  elsif ($date && $date =~ /^\s*([0-9]+)\s*[\-\:]+\s*([0-9]+)\s*$/) {
    my ($dlo,$dhi) = ($1+0,$2+0);
    $dfilter = sub { $_[0]>=$dlo && $_[0]<=$dhi };
  }
  elsif ($date && $date =~ /[\s\,]/) {
    my %dwant = map {($_=>undef)} grep {($_//'') ne ''} split(/[\s\,]+/,$date);
    $dfilter  = sub { exists($dwant{$_[0]}) };
  }
  elsif ($date) {
    $dfilter = sub { $_[0] == $date };
  }
  my $d2xis  = {}; ##-- ($dateKey => \@xis_at_date, ...)
  my $pack_x = $coldb->{pack_x};
  my $pack_i = $coldb->{pack_id};
  my $pack_d = $coldb->{pack_date};
  my $pack_xd = "@".packsize($pack_i).$pack_d;
  my ($xi,$d);
  foreach $xi (@$xis) {
    $d = unpack($pack_xd, $xenum->i2s($xi));
    next if ($dfilter && !$dfilter->($d));
    $d = $slice ? int($d/$slice)*$slice : 0;
    push(@{$d2xis->{$d}}, $xi);
  }

  ##-- profile: get low-level co-frequency profiles
  $coldb->vlog($logProfile, "profile(): get frequency profile(s)");
  my $gbsub = sub { unpack($pack_i,$xenum->i2s($_[0])) };
  my $d2prf = {}; ##-- ($dateKey => $profileForDateKey, ...)
  my $reldb = $coldb->{$rel};
  my ($prf);
  foreach $d (sort {$a<=>$b} keys %$d2xis) {
    $prf = $reldb->profile($d2xis->{$d}, groupby=>$gbsub);
    $prf->compile($score, eps=>$eps);
    $prf->trim(kbest=>$kbest, cutoff=>$cutoff);
    $prf = $prf->stringify($lenum) if ($strings);
    $d2prf->{$d} = $prf;
  }

  return DiaColloDB::Profile::Multi->new(data=>$d2prf);
}

##==============================================================================
## Footer
1;

__END__




