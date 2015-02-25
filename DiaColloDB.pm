## -*- Mode: CPerl -*-
## File: DiaColloDB.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, top-level

package DiaColloDB;
use DiaColloDB::Client;
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
use DiaColloDB::Profile::MultiDiff;
use DiaColloDB::Corpus;
use DiaColloDB::Persistent;
use DiaColloDB::Utils qw(:fcntl :json :sort :pack :regex);
use Fcntl;
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = 0.03;
our @ISA = qw(DiaColloDB::Client);

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
##    logRequest => $level,     ##-- log-level for request-level profiling messages (default='debug')
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
		      logRequest => 'debug',

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

## undef = $obj->DESTROY
##  + destructor calls close() if necessary
##  + INHERITED from DiaColloDB::Client

## $cli_or_undef = $cli->promote($class,%opts)
##  + DiaColloDB::Client method override
sub promote {
  $_[0]->logconfess("promote(): not supported");
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

##--------------------------------------------------------------
## create: utils

## $multimap = $coldb->create_xmap($base, \%xs2i, $packfmt, $label="multimap")
sub create_xmap {
  my ($coldb,$base,$xs2i,$packfmt,$label) = @_;
  $label //= "multimap";
  $coldb->vlog($coldb->{logCreate},"create_xmap(): creating $label $base.*");

  my $pack_id  = $coldb->{pack_id};
  my $pack_mmb = "${pack_id}*"; ##-- multimap target-set pack format
  my @v2xi     = qw();
  my ($x,$xi,$vi);
  while (($x,$xi)=each %$xs2i) {
    ($vi)       = unpack($packfmt,$x);
    $v2xi[$vi] .= pack($pack_id,$xi);
  }
  $_ = pack($pack_mmb, sort {$a<=>$b} unpack($pack_mmb,$_//'')) foreach (@v2xi); ##-- ensure multimap target-sets are sorted

  my $v2x = $MMCLASS->new(base=>$base, flags=>'rw', perms=>$coldb->{perms}, pack_i=>$pack_id, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len})
      or $coldb->logconfess("create_xmap(): failed to create $base.*: $!");
  $v2x->fromArray(\@v2xi)
    or $coldb->logconfess("create_xmap(): failed to populate $base.*: $!");
  $v2x->flush()
    or $coldb->logconfess("create_xmap(): failed to flush $base.*: $!");

  return $v2x;
}

##--------------------------------------------------------------
## create: from corpus

## $bool = $coldb->create($corpus,%opts)
##  + %opts:
##     clobber %$coldb
sub create {
  my ($coldb,$corpus,%opts) = @_;
  $coldb = $coldb->new() if (!ref($coldb));
  @$coldb{keys %opts} = values %opts;
  my $flags = O_RDWR|O_CREAT|O_TRUNC;

  ##-- initialize: output directory
  my $dbdir = $coldb->{dbdir}
    or $coldb->logconfess("create() called but 'dbdir' key not set!");
  $dbdir =~ s{/$}{};
  $coldb->vlog('info', "create($dbdir)");
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
  $coldb->create_xmap("$dbdir/l2x",$xs2i,$pack_id,"lemma-expansion multimap");

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
## create: union (aka merge)

## $coldb = $CLASS_OR_OBJECT->union(\@coldbs_or_dbdirs,%opts)
##  + populates $coldb as union over @coldbs_or_dbdirs
##  + clobbers argument dbs {li2u}, {xi2u}
BEGIN { *merge = \&union; }
sub union {
  my ($coldb,$args,%opts) = @_;
  $coldb = $coldb->new() if (!ref($coldb));
  @$coldb{keys %opts} = values %opts;
  my @dbargs = map {ref($_) ? $_ : $coldb->new(dbdir=>$_)} @$args;
  my $flags = O_RDWR|O_CREAT|O_TRUNC;

  ##-- initialize: output directory
  my $dbdir = $coldb->{dbdir}
    or $coldb->logconfess("union() called but 'dbdir' key not set!");
  $dbdir =~ s{/$}{};
  $coldb->vlog('info', "union($dbdir): ", join(' ', map {$_->{dbdir}//''} @dbargs));
  !-d $dbdir
    or remove_tree($dbdir)
      or $coldb->logconfess("union(): could not remove stale $dbdir: $!");
  make_path($dbdir)
    or $coldb->logconfess("union(): could not create DB directory $dbdir: $!");

  ##-- pack-formats
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_x     = $coldb->{pack_x} = $pack_id.$pack_date; ##-- pack("${pack_id}${pack_date}", $li, $date)

  ##-- common variables: enums
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my ($db);

  ##-- lenum: populate
  $coldb->vlog($coldb->{logCreate}, "union(): creating lemma-enum DB $dbdir/lenum.*");
  my $lenum = $coldb->{lenum} = $ECLASS->new(%efopts);
  my $ls2i  = $lenum->{s2i};
  my $nl    = 0;
  my ($ali,$uli,$li2u);
  foreach $db (@dbargs) {
    $db->{lenum}->load()
      or $coldb->logconfess("union(): failed to load lemma-enum for $db->{dbdir}");
    $li2u = $db->{li2u} = [];
    $ali  = 0;
    foreach (@{$db->{lenum}{i2s}}) {
      $uli = $ls2i->{$_} = $nl++ if (!defined($uli=$ls2i->{$_}));
      $li2u->[$ali++] = $uli;
    }
    $db->{lenum}->rollback();
  }
  $lenum->fromHash($ls2i);
  $lenum->save("$dbdir/lenum")
    or $coldb->logconfess("union(): failed to save $dbdir/lenum: $!");

  ##-- xenum: populate
  $coldb->vlog($coldb->{logCreate}, "union(): creating tuple DB $dbdir/xenum.*");
  my $xenum = $coldb->{xenum} = $XECLASS->new(%efopts, pack_s=>$pack_x);
  my $xs2i  = $xenum->{s2i};
  my $nx    = 0;
  my ($db_pack_x,$xi2u,$axi,@dbx,$uxs,$uxi);
  foreach $db (@dbargs) {
    $db->{xenum}->load()
      or $coldb->logconfess("union(): failed to load tuple-enum for $db->{dbdir}");
    $db_pack_x = $db->{pack_x};
    $li2u      = $db->{li2u};
    $xi2u      = $db->{xi2u} = [];
    $axi       = 0;
    foreach (@{$db->{xenum}{i2s}}) {
      @dbx    = unpack($db_pack_x,$_);
      $dbx[0] = $li2u->[$dbx[0]//0];
      $uxs    = pack($pack_x,@dbx);
      $uxi    = $xs2i->{$uxs} = $nx++ if (!defined($uxi=$xs2i->{$uxs}));
      $xi2u->[$axi++] = $uxi;
    }
    $db->{xenum}->rollback();
  }
  $xenum->fromHash($xs2i);
  $xenum->save("$dbdir/xenum")
    or $coldb->logconfess("union(): failed to save $dbdir/xenum: $!");

  ##-- expansion map: l2x
  $coldb->create_xmap("$dbdir/l2x",$xs2i,$pack_id,"lemma-expansion multimap");

  ##-- unigrams: populate
  $coldb->vlog($coldb->{logCreate}, "union(): tuple 1-gram file $dbdir/xf.dba");
  my $xfdata = [];
  my ($axf);
  foreach $db (@dbargs) {
    $axf  = $db->{xf}->toArray();
    $xi2u = $db->{xi2u};
    $axi  = 0;
    foreach (@$axf) {
      $xfdata->[$xi2u->[$axi++]] += $_;
    }
  }
  my $xfdb = $coldb->{xf} = DiaColloDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$pack_f)
    or $coldb->logconfess("union(): could not create $dbdir/xf.dba: $!");
  $xfdb->fromArray($xfdata)
    or $coldb->logconfess("union(): could not populate unigram db $dbdir/xf.dba: $!");
  $xfdb->flush()
    or $coldb->logconfess("union(): could not flush unigram db $dbdir/xf.dba: $!");

  ##-- cof: TODO

  ##-- cleanup
  delete @$_{qw(li2u xi2u)} foreach (@dbargs);

  ##-- save header
  $coldb->saveHeader()
    or $coldb->logconfess("union(): failed to save header: $!");

  ##-- all done
  $coldb->vlog($coldb->{logCreate}, "union(): union DB $dbdir created.");

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
  $coldb->logconfess("dbimport(): not yet implemented");
}

##==============================================================================
## Profiling

##--------------------------------------------------------------
## Profiling: Wrappers
##  + INHERITED from DiaColloDB::Client

## $mprf = $coldb->query($rel,%opts)
##  + get a generic DiaColloDB::Profile::Multi object for $rel
##  + calls $coldb->profile() or $coldb->compare() as appropriate
##  + INHERITED from DiaColloDB::Client

## $mprf = $coldb->profile1(%opts)
##  + get unigram frequency profile for selected items as a DiaColloDB::Profile::Multi object
##  + really just wraps $coldb->profile('xf', %opts)
##  + %opts: see profile() method
##  + INHERITED from DiaColloDB::Client

## $mprf = $coldb->profile2(%opts)
##  + get co-frequency profile for selected items as a DiaColloDB::Profile::Multi object
##  + really just wraps $coldb->profile('cof', %opts)
##  + %opts: see profile() method
##  + INHERITED from DiaColloDB::Client

## $mprf = $coldb->compare1(%opts)
##  + get unigram comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + really just wraps $coldb->compare('xf', %opts)
##  + %opts: see compare() method
##  + INHERITED from DiaColloDB::Client

## $mprf = $coldb->compare2(%opts)
##  + get co-frequency comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + really just wraps $coldb->profile('cof', %opts)
##  + %opts: see compare() method
##  + INHERITED from DiaColloDB::Client


##--------------------------------------------------------------
## Profiling: Utils

## $relname = $coldb->relname($rel)
##  + returns an appropriate relation name for profile() and friends
##  + returns $rel if $coldb->{$rel} supports a profile() method
##  + otherwise heuristically parses $relationName /xf|f?1|ug/ or /f1?2|c/
sub relname {
  my ($coldb,$rel) = @_;
  if (UNIVERSAL::can($coldb->{$rel},'profile')) {
    return $rel;
  }
  elsif ($rel =~ m/^(?:[ux]|f?1$)/) {
    return 'xf';
  }
  elsif ($rel =~ m/^(?:c|f?1?2$)/) {
    return 'cof';
  }
  return $rel;
}

## $obj_or_undef = $coldb->relation($rel)
##  + returns an appropriate relation-like object for profile() and friends
##  + wraps $coldb->{$coldb->relname($rel)}
sub relation {
  return $_[0]->{$_[0]->relname($_[1])};
}

## \@ids = $coldb->enumIds($enum,$req,%opts)
##  + %opts:
##     logLevel => $logLevel, ##-- logging level (default=undef)
##     logPrefix => $prefix,  ##-- logging prefix (default="enumIds(): fetch ids")
sub enumIds {
  my ($coldb,$enum,$req,%opts) = @_;
  $opts{logPrefix} //= "enumIds(): fetch ids";
  if (UNIVERSAL::isa($req,'ARRAY')) {
    ##-- lemmas: array
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (ARRAY)");
    return [map {$enum->s2i($_)} @$req];
  }
  elsif ($req =~ m{^/}) {
    ##-- lemmas: regex
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (REGEX)");
    return $enum->re2i($req);
  }
  else {
    ##-- lemmas: space-separated literals
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (STRINGS)");
    return [grep {defined($_)} map {$enum->s2i($_)} grep {($_//'') ne ''} map {s{\\(.)}{$1}g; $_} split(/(?:(?<!\\)[\,\s])+/,$req)];
  }
  return [];
}

## \%slice2xids = $coldb->xidsByDate(\@xids, $dateRequest, $sliceRequest)
##   + parse and filter \@xids by $dateRequest, $sliceRequest
##   + returns a HASH-ref from slice-ids to \@xids in that date-slice
sub xidsByDate {
  my ($coldb,$xids,$date,$slice) = @_;
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
  my $xenum  = $coldb->{xenum};
  my $pack_x = $coldb->{pack_x};
  my $pack_i = $coldb->{pack_id};
  my $pack_d = $coldb->{pack_date};
  my $pack_xd = "@".packsize($pack_i).$pack_d;
  my $d2xis  = {}; ##-- ($dateKey => \@xis_at_date, ...)
  my ($xi,$d);
  foreach $xi (@$xids) {
    $d = unpack($pack_xd, $xenum->i2s($xi));
    next if ($dfilter && !$dfilter->($d));
    $d = $slice ? int($d/$slice)*$slice : 0;
    push(@{$d2xis->{$d}}, $xi);
  }
  return $d2xis;
}

##--------------------------------------------------------------
## Profiling: Generic

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
  $coldb->vlog($coldb->{logRequest},"profile(rel=$rel, lemma='$lemma', date='$date', slice=$slice, score=$score, eps=$eps, kbest=$kbest, cutoff=$cutoff)");

  ##-- sanity check(s)
  my ($reldb);
  if (!defined($reldb=$coldb->relation($rel||'cof'))) {
    $coldb->logwarn($coldb->{error}="profile(): unknown relation '".($rel//'-undef-')."'");
    return undef;
  }
  if ($lemma eq '') {
    $coldb->logwarn($coldb->{error}="profile(): missing required parameter 'lemma'");
    return undef;
  }

  ##-- prepare: get target lemmata
  my $lis = $coldb->enumIds($lenum,$lemma,logLevel=>$logProfile, logPrefix=>"profile(): get target lemmata");
  if (!@$lis) {
    $coldb->logwarn($coldb->{error}="profile(): no lemmata matching user query '$lemma'");
    return undef;
  }

  ##-- prpare: map lemmas => tuple-ids
  $coldb->vlog($logProfile, "profile(): get target tuple IDs");
  my $xis = [sort {$a<=>$b} map {@{$l2x->fetch($_)}} @$lis];

  ##-- prepare: parse and filter tuples
  $coldb->vlog($logProfile, "profile(): parse and filter target tuples");
  my $d2xis = $coldb->xidsByDate($xis, $date, $slice);

  ##-- profile: get low-level co-frequency profiles
  $coldb->vlog($logProfile, "profile(): get frequency profile(s)");
  my $pack_i = $coldb->{pack_id};
  my $gbsub  = sub { unpack($pack_i,$xenum->i2s($_[0])) };
  my @dprfs  = qw();
  my ($d,$prf);
  foreach $d (sort {$a<=>$b} keys %$d2xis) {
    $prf = $reldb->profile($d2xis->{$d}, groupby=>$gbsub);
    $prf->compile($score, eps=>$eps);
    $prf->trim(kbest=>$kbest, cutoff=>$cutoff);
    $prf = $prf->stringify($lenum) if ($strings);
    $prf->{label} = $d;
    push(@dprfs, $prf);
  }

  return DiaColloDB::Profile::Multi->new(profiles=>\@dprfs);
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $coldb->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
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
BEGIN { *diff = \&compare; }
sub compare {
  my ($coldb,$rel,%opts) = @_;
  $rel //= 'cof';

  ##-- common variables
  my ($logProfile,$lenum) = @$coldb{qw(logProfile lenum)};
  $opts{"a$_"} //= $opts{$_}//'' foreach qw(lemma date slice);
  $opts{"b$_"} //= $opts{$_}//'' foreach qw(lemma date slice);
  my %aopts = map {($_=>$opts{"a$_"})} qw(lemma date slice);
  my %bopts = map {($_=>$opts{"b$_"})} qw(lemma date slice);
  my %popts = (kbest=>-1,cutoff=>'',strings=>0);

  ##-- debug
  $coldb->vlog($coldb->{logRequest},"compare(".join(', ', map {"$_=".($opts{$_}//'')} qw(rel alemma blemma adate bdate aslice bslice score eps kbest cutoff)).")");

  ##-- get profiles to compare
  my $mpa = $coldb->profile($rel,%opts, %aopts,%popts)
    or return undef;
  my $mpb = $coldb->profile($rel,%opts, %bopts,%popts)
    or return undef;

  ##-- alignment and trimming
  $coldb->vlog($logProfile, "compare(): align and trim");
  my $ppairs = DiaColloDB::Profile::MultiDiff->align($mpa,$mpb);
  my %trim   = (kbest=>($opts{kbest}//-1), cutoff=>($opts{cutoff}//''));
  my ($pa,$pb,%pkeys);
  foreach (@$ppairs) {
    ($pa,$pb) = @$_;
    %pkeys = map {($_=>undef)} (@{$pa->which(%trim)}, @{$pb->which(%trim)});
    $pa->trim(keep=>\%pkeys);
    $pb->trim(keep=>\%pkeys);
  }

  ##-- diff and stringification
  $coldb->vlog($logProfile, "compare(): diff and stringification");
  my $diff = DiaColloDB::Profile::MultiDiff->new($mpa,$mpb);
  $diff->trim(kbesta=>$opts{kbest}) if ($opts{kbest});
  $diff->stringify($lenum) if ($opts{strings}//1);

  return $diff;
}

##==============================================================================
## Footer
1;

__END__




