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
use DiaColloDB::Timer;
use Fcntl;
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = "0.03_0001";
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
##    attrs => \@attrs,   ##-- index attributes (input as space-separated or array; compiled to array); default=[] (==>['l'])
##                        ##    + each attribute can be token-attribute qw(w p l) or a document metadata attribute "doc.ATTR"
##                        ##    + document "date" attribute is always indexed
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
##    ##-- attribute data
##    ${a}enum => $aenum,   ##-- attribute enum: $aenum : ($dbdir/${a}_enum.*) : $astr<=>$ai : A*<=>N
##                          ##    e.g.  lemmata: $lenum : ($dbdir/l_enum.*   )  : $lstr<=>$li : A*<=>N
##    ${a}2x   => $a2x,     ##-- attribute multimap: $a2x : ($dbdir/${a}_2x.*) : $ai=>@xis  : N=>N*
##    pack_x$a => $fmt      ##-- pack format: extract attribute-id $ai from a packed tuple-string $xs ; $ai=unpack($coldb->{"pack_x$a"},$xs)
##    ##
##    ##-- tuple data
##    xenum  => $xenum,     ##-- enum: tuples ($dbdir/xenum.*) : [@ais,$di]<=>$xi : N*n<=>N
##    pack_x => $fmt,       ##-- symbol pack-format for $xenum : "${pack_id}[Nattrs]${pack_date}"
##    ##
##    ##-- relation data
##    xf    => $xf,       ##-- ug: $xi => $f($xi) : N=>N
##    cof   => $cof,      ##-- cf: [$xi1,$xi2] => $f12
##   )
sub new {
  my $that = shift;
  my $coldb  = bless({
		      ##-- options
		      dbdir => undef,
		      flags => 'r',
		      attrs => [],
		      #bos => undef,
		      #eos => undef,
		      pack_id => 'N',
		      pack_f  => 'N',
		      pack_date => 'n',
		      pack_off => 'N',
		      pack_len =>'n',
		      dmax => 5,
		      cfmin => 2,
		      #keeptmp => 0,

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

		      ##-- administrivia
		      version => "$VERSION",

		      ##-- attributes
		      #lenum => undef, #$ECLASS->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      #l2x   => undef, #$MMCLASS->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      #pack_xl => 'N',

		      ##-- tuples
		      #xenum  => undef, #$XECLASS::FixedLen->new(pack_i=>$coldb->{pack_id}, pack_s=>$coldb->{pack_x}),
		      #pack_x => 'Nn',

		      ##-- relations
		      #xf    => undef, #DiaColloDB::Unigrams->new(packas=>$coldb->{pack_f}),
		      #cof   => undef, #DiaColloDB::Cofreqs->new(pack_f=>$pack_f, pack_i=>$pack_i, dmax=>$dmax, fmin=>$cfmin),

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

  ##-- open: attributes
  my $attrs = $coldb->{attrs} = $coldb->attrs(undef,['l']);

  ##-- open: by attribute
  my $axat = 0;
  foreach my $a (@$attrs) {
    ##-- open: ${a}*
    my $abase = (-r "$dbdir/${a}_enum.hdr" ? "$dbdir/${a}_" : "$dbdir/${a}"); ##-- v0.03-compatibility hack
    $coldb->{"${a}enum"} = $ECLASS->new(base=>"${abase}enum", %efopts)
      or $coldb->logconfess("open(): failed to open enum ${abase}enum.*: $!");
    $coldb->{"${a}2x"} = $MMCLASS->new(base=>"${abase}2x", %mmopts)
      or $coldb->logconfess("open(): failed to open expansion multimap ${abase}2x.*: $!");
    $coldb->{"pack_x$a"} //= "\@${axat}$coldb->{pack_id}";
    $axat += packsize($coldb->{pack_id});
  }

  ##-- open: xenum
  $coldb->{xenum} = $XECLASS->new(base=>"$dbdir/xenum", %efopts, pack_s=>$coldb->{pack_x})
      or $coldb->logconfess("open(): failed to open tuple-enum $dbdir/xenum.*: $!");

  ##-- open: xf
  $coldb->{xf} = DiaColloDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$coldb->{pack_f})
    or $coldb->logconfess("open(): failed to open tuple-unigrams $dbdir/xf.dba: $!");
  $coldb->{xf}{N} = $coldb->{xN} if ($coldb->{xN} && !$coldb->{xf}{N}); ##-- compat

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
  return (
	  (ref($_[0]) ? (map {($_."enum",$_."2x")} $_[0]->attrs) : qw()),
	  qw(xenum xf cof),
	 );
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

## \@attrs = $coldb->attrs()
## \@attrs = $coldb->attrs($attrs=$coldb->{attrs}, $default=[])
##  + parse attributes in $attrs as array
sub attrs {
  my ($coldb,$attrs,$default) = @_;
  $attrs //= $coldb->{attrs} // $default // [];
  return $attrs if (UNIVERSAL::isa($attrs,'ARRAY'));
  return [grep {defined($_) && $_ ne ''} split(/[\s\,]+/, $attrs)];
}

## \@attrdata = $coldb->attrData()
## \@attrdata = $coldb->attrData(\@attrs=$coldb->attrs)
##  + get attribute data for \@attrs
##  + return @attrdata = ({a=>$a, i=>$i, enum=>$aenum, pack_x=>$pack_xa, a2x=>$a2x, ...})
sub attrData {
  my ($coldb,$attrs) = @_;
  $attrs //= $coldb->attrs;
  my ($a);
  return [map {
    $a = $attrs->[$_];
    {i=>$_, a=>$a, enum=>$coldb->{"${a}enum"}, pack_x=>$coldb->{"pack_x$a"}, a2x=>$coldb->{"${a}2x"}}
  } (0..$#$attrs)];
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

  ##-- initialize: attributes
  my $attrs = $coldb->{attrs} = $coldb->attrs(undef,['l']);

  ##-- pack-formats
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_x     = $coldb->{pack_x} = $pack_id."[".scalar(@$attrs)."]".$pack_date;

  ##-- initialize: common flags
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = %efopts;

  ##-- initialize: attribute enums
  my $aconf = [];  ##-- [{a=>$a, i=>$i, enum=>$aenum, pack_x=>$pack_xa, s2i=>\%s2i, ns=>$nstrings, #a2x=>$a2x, ...}, ]
  my $axpos = 0;
  my ($a,$ac);
  foreach (0..$#$attrs) {
    push(@$aconf,$ac={i=>$_, a=>($a=$attrs->[$_])});
    $ac->{enum}   = $coldb->{"${a}enum"} = $ECLASS->new(%efopts);
    $ac->{pack_x} = $coldb->{"pack_x$a"} = '@'.$axpos.$pack_id;
    $ac->{s2i}    = $ac->{enum}{s2i};
    $ac->{ma}     = $1 if ($a =~ /^(?:meta|doc)\.(.*)$/);
    $axpos       += packsize($pack_id);
  }
  my @aconfm = grep { defined($_->{ma})} @$aconf; ##-- meta-attributes
  my @aconfw = grep {!defined($_->{ma})} @$aconf; ##-- token-attributes

  ##-- initialize: tuple enum
  my $xenum = $coldb->{xenum} = $XECLASS->new(%efopts, pack_s=>$pack_x);
  my $xs2i  = $xenum->{s2i};
  my $nx    = 0;


  ##-- initialize: corpus token-list (temporary)
  my $tokfile =  "$dbdir/tokens.dat";
  CORE::open(my $tokfh, ">$tokfile")
    or $coldb->logconfess("$0: open failed for $tokfile: $!");

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
  my ($doc, $date,$tok,$w,$p,$l,@ais,$x,$xi, $filei);
  my ($last_was_eos,$bosxi,$eosxi);
  for ($corpus->ibegin(); $corpus->iok; $corpus->inext) {
    $coldb->vlog($coldb->{logCorpusFile}, sprintf("create(): processing files [%3d%%]: %s", 100*$filei/$nfiles, $corpus->ifile))
      if ($logFileN && ($filei++ % $logFileN)==0);
    $doc  = $corpus->idocument();
    $date = $doc->{date};

    ##-- get meta-attributes
    @ais = qw();
    $ais[$_->{i}] = ($_->{s2i}{$doc->{meta}{$_->{ma}}} //= ++$_->{ns}) foreach (@aconfm);

    ##-- allocate bos,eos
    undef $bosxi;
    undef $eosxi;
    foreach $w (grep {($_//'') ne ''} $bos,$eos) {
      $ais[$_->{i}] = ($_->{s2i}{$w} //= ++$_->{ns}) foreach (@aconfw);
      $x   = pack($pack_x,@ais,$date);
      $xi  = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x}));
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

	##-- get attribute value-ids and build tuple
	$ais[$_->{i}] = ($_->{s2i}{$tok->{$_->{a}//''}} //= ++$_->{ns}) foreach (@aconfw);

	$x  = pack($pack_x, @ais,$date);
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

  ##-- compile: xenum
  $coldb->vlog($coldb->{logCreate}, "create(): creating tuple-enum $dbdir/xenum.*");
  $xenum->fromHash($xs2i);
  $xenum->save("$dbdir/xenum")
    or $coldb->logconfess("create(): failed to save $dbdir/xenum: $!");

  ##-- compile: by attribute
  foreach $ac (@$aconf) {
    ##-- compile: by attribte: enum
    $coldb->vlog($coldb->{logCreate},"create(): creating enum $dbdir/$ac->{a}_enum.*");
    $ac->{enum}->fromHash($ac->{s2i});
    $ac->{enum}->save("$dbdir/$ac->{a}_enum")
      or $coldb->logconfess("create(): failed to save $dbdir/$ac->{a}_enum: $!");

    ##-- compile: by attribute: expansion multimaps
    $coldb->create_xmap("$dbdir/$ac->{a}_2x",$xs2i,$ac->{pack_x},"attribute expansion multimap");
  }

  ##-- compute unigrams
  $coldb->info("creating tuple unigram index $dbdir/xf.dba");
  my $xfdb = $coldb->{xf} = DiaColloDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$pack_f)
    or $coldb->logconfess("create(): could not create $dbdir/xf.dba: $!");
  $xfdb->setsize($xenum->{size})
    or $coldb->logconfess("create(): could not set unigram index size = $xenum->{size}: $!");
  $xfdb->create($tokfile)
    or $coldb->logconfess("create(): failed to create unigram index: $!");

  ##-- compute collocation frequencies
  $coldb->info("creating co-frequency index $dbdir/cof.* [dmax=$coldb->{dmax}, fmin=$coldb->{cfmin}]");
  my $cof = $coldb->{cof} = DiaColloDB::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
						     pack_i=>$pack_id, pack_f=>$pack_f,
						     dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
						     keeptmp=>$coldb->{keeptmp},
						    )
    or $coldb->logconfess("create(): failed to create co-frequency index $dbdir/cof.*: $!");
  $cof->create($tokfile)
    or $coldb->logconfess("create(): failed to create co-frequency index: $!");

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

  ##-- attributes
  my $attrs = $coldb->attrs(undef,[]);
  if (!@$attrs) {
    ##-- use union of @dbargs attrs
    my %akeys = qw();
    foreach (map {@{$_->attrs(undef,['l'])}} @dbargs) {
      next if (exists($akeys{$_}));
      $akeys{$_}=undef;
      push(@$attrs, $_);
    }
  }
  $coldb->{attrs} = $attrs;

  ##-- pack-formats
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_x     = $coldb->{pack_x} = $pack_id."[".scalar(@$attrs)."]".$pack_date; ##-- pack("${pack_id}*${pack_date}", @ais, $date)

  ##-- common variables: enums
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});

  ##-- union: attribute enums; also sets $db->{"${a}i2u"} for each attribute $a
  my ($db,$ac,$a,$aenum,$as2i);
  my $adata = $coldb->attrData($attrs);
  foreach $ac (@$adata) {
    $coldb->vlog($coldb->{logCreate}, "union(): creating attribute enum $dbdir/$ac->{a}_enum.*");
    $a     = $ac->{a};
    $aenum = $coldb->{"${a}enum"} = $ac->{enum} = $ECLASS->new(%efopts);
    $as2i  = $aenum->{s2i};
    foreach $db (@dbargs) {
      ##-- enum union: guts
      $aenum->addEnum($db->{"${a}enum"});
      $db->{"${a}i2u"} = [ @$as2i{@{$aenum->toArray}} ];
    }
    $aenum->save("$dbdir/${a}_enum")
      or $coldb->logconfess("union(): failed to save attribute enum $dbdir/${a}_enum: $!");
  }

  ##-- union: xenum
  $coldb->vlog($coldb->{logCreate}, "union(): creating tuple-enum $dbdir/xenum.*");
  my $xenum = $coldb->{xenum} = $XECLASS->new(%efopts, pack_s=>$pack_x);
  my $xs2i  = $xenum->{s2i};
  my $nx    = 0;
  foreach $db (@dbargs) {
    my $db_pack_x  = $db->{pack_x};
    my $dbattrs = $db->{attrs};
    my %a2dbxi  = map { ($dbattrs->[$_]=>$_) } (0..$#$dbattrs);
    my %a2i2u   = map { ($_=>$db->{"${_}i2u"}) } @$attrs;
    my $xi2u    = $db->{xi2u} = [];
    my $dbxi    = 0;
    my (@dbx,@ux,$uxs,$uxi);
    foreach (@{$db->{xenum}->toArray}) {
      @dbx = unpack($db_pack_x,$_);
      ##-- CONTINUE HERE: Use of uninitialized value in pack at ...
      $uxs = pack($pack_x,
		  (map  {
		    (exists($a2dbxi{$_})
		     ? $a2i2u{$_}[$dbx[$a2dbxi{$_}]//0]
		     : $a2i2u{$_}[0])
		  } @$attrs),
		  $dbx[$#dbx]);
      $uxi = $xs2i->{$uxs} = $nx++ if (!defined($uxi=$xs2i->{$uxs}));
      $xi2u->[$dbxi++] = $uxi;
    }
  }
  $xenum->fromHash($xs2i);
  $xenum->save("$dbdir/xenum")
    or $coldb->logconfess("union(): failed to save $dbdir/xenum: $!");

  ##-- union: expansion maps
  foreach $ac (@$adata) {
    $coldb->create_xmap("$dbdir/$ac->{a}_2x",$xs2i,$ac->{pack_x},"attribute expansion multimap");
  }

  ##-- unigrams: populate
  $coldb->vlog($coldb->{logCreate}, "union(): creating tuple unigram index $dbdir/xf.*");
  $coldb->{xf} = DiaColloDB::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$pack_f)
    or $coldb->logconfess("union(): could not create $dbdir/xf.*: $!");
  $coldb->{xf}->union([map {[@$_{qw(xf xi2u)}]} @dbargs])
    or $coldb->logconfess("union(): could not populate unigram index $dbdir/xf.*: $!");

  ##-- co-frequencies: populate
  $coldb->vlog($coldb->{logCreate}, "union(): creating co-frequency index $dbdir/cof.* [fmin=$coldb->{cfmin}]");
  $coldb->{cof} = DiaColloDB::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
					   pack_i=>$pack_id, pack_f=>$pack_f,
					   dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
					   keeptmp=>$coldb->{keeptmp},
					  )
    or $coldb->logconfess("create(): failed to open co-frequency index $dbdir/cof.*: $!");
  $coldb->{cof}->union([map {[@$_{qw(cof xi2u)}]} @dbargs])
    or $coldb->logconfess("union(): could not populate co-frequency index $dbdir/cof.*: $!");

  ##-- cleanup
  delete @$_{('xi2u', map {"${_}i2u"} @$attrs)} foreach (@dbargs);

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
  return (qw(attrs), grep {!ref($_[0]{$_}) && $_ !~ m{^(?:dbdir$|flags$|perms$|log)}} keys %{$_[0]});
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
  $outdir  =~ s{/$}{};
  $coldb->vlog('info', "export($outdir/)");

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

  ##-- dump: common: stringification
  my ($li2txt,$xs2txt,$xi2txt);
  if ($export_sdat) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): preparing tuple-stringification structures");
    my ($li,$d);
    my $pack_x = $coldb->{pack_x};
    my $li2s   = $coldb->{lenum}->toArray;
    my $xi2s   = $coldb->{xenum}->toArray;
    $li2txt = sub { return $li2s->[$_[0]//0]//''; };
    $xs2txt = sub {
      ($li,$d) = unpack($pack_x,$_[0]);
      return join("\t", $li2s->[$li//0]//'', $d//0);
    };
    $xi2txt = sub {
      ($li,$d) = unpack($pack_x,$xi2s->[$_[0]//'']//'');
      return join("\t", $li2s->[$li//0]//'', $d//0);
    };
  }

  ##-- dump: lenum
  $coldb->vlog($coldb->{logExport}, "dbexport(): exporting lemma-enum file $outdir/lenum.dat");
  $coldb->{lenum}->saveTextFile("$outdir/lenum.dat")
    or $coldb->logconfess("dbexport() failed for $outdir/lenum.dat");

  ##-- dump: xenum: raw
  my $pack_x = $coldb->{pack_x};
  $coldb->vlog($coldb->{logExport}, "dbexport(): exporting raw tuple-enum file $outdir/xenum.dat");
  $coldb->{xenum}->saveTextFile("$outdir/xenum.dat", pack_s=>$pack_x)
    or $coldb->logconfess("export failed for $outdir/xenum.dat");

  ##-- dump: xenum: stringified
  $coldb->vlog($coldb->{logExport}, "dbexport(): exporting stringified tuple-enum file $outdir/xenum.sdat");
  $coldb->{xenum}->saveTextFile("$outdir/xenum.sdat", pack_s=>$xs2txt)
    or $coldb->logconfess("dbexport() failed for $outdir/xenum.sdat");

  ##-- dump: l2x
  if ($coldb->{l2x}) {
    ##-- dump: l2x: raw
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting raw lemma-expansion multimap $outdir/l2x.dat");
    $coldb->{l2x}->saveTextFile("$outdir/l2x.dat")
      or $coldb->logconfess("dbexport() failed for $outdir/l2x.dat");

    ##-- dump: l2x: stringified
    if ($export_sdat) {
      $coldb->vlog($coldb->{logExport}, "dbexport(): exporting stringified lemma-expansion multimap $outdir/l2x.sdat");
      $coldb->{l2x}->saveTextFile("$outdir/l2x.sdat", a2s=>$li2txt, b2s=>$xi2txt)
	or $coldb->logconfess("dbexport() failed for $outdir/l2x.sdat");
    }
  }

  ##-- dump: xf
  if ($coldb->{xf}) {
    ##-- dump: xf: raw
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting tuple-frequency index $outdir/xf.dat");
    $coldb->{xf}->setFilters($coldb->{pack_f});
    $coldb->{xf}->saveTextFile("$outdir/xf.dat", keys=>1)
      or $coldb->logconfess("export failed for $outdir/xf.dat");
    $coldb->{xf}->setFilters();

    ##-- dump: xf: stringified
    if ($export_sdat) {
      $coldb->vlog($coldb->{logExport}, "dbexport(): exporting stringified tuple-frequency index $outdir/xf.sdat");
      $coldb->{xf}->saveTextFile("$outdir/xf.sdat", key2s=>$xi2txt)
      or $coldb->logconfess("dbexport() failed for $outdir/xf.sdat");
    }
  }

  ##-- dump: cof
  if ($coldb->{cof} && $export_cof) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting raw co-frequency index $outdir/cof.dat");
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
      $coldb->vlog($coldb->{logExport}, "dbexport(): exporting stringified co-frequency index $outdir/cof.sdat");
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




