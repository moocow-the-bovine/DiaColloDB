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
use DiaColloDB::EnumFile::Tied;
use DiaColloDB::MultiMapFile;
use DiaColloDB::PackedFile;
use DiaColloDB::Relation;
use DiaColloDB::Relation::Unigrams;
use DiaColloDB::Relation::Cofreqs;
use DiaColloDB::Relation::DDC;
use DiaColloDB::Profile;
use DiaColloDB::Profile::Multi;
use DiaColloDB::Profile::MultiDiff;
use DiaColloDB::Corpus;
use DiaColloDB::Persistent;
use DiaColloDB::Utils qw(:fcntl :json :sort :pack :regex :file :si);
use DiaColloDB::Timer;
use DDC::XS; ##-- for query parsing
use Fcntl;
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = "0.06.004";
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
##    attrs => \@attrs,   ##-- index attributes (input as space-separated or array; compiled to array); default=undef (==>['l'])
##                        ##    + each attribute can be token-attribute qw(w p l) or a document metadata attribute "doc.ATTR"
##                        ##    + document "date" attribute is always indexed
##    info => \%info,     ##-- additional data to return in info() method (e.g. collection, maintainer)
##    bos => $bos,        ##-- special string to use for BOS, undef or empty for none (default=undef)
##    eos => $eos,        ##-- special string to use for EOS, undef or empty for none (default=undef)
##    pack_id => $fmt,    ##-- pack-format for IDs (default='N')
##    pack_f  => $fmt,    ##-- pack-format for frequencies (default='N')
##    pack_date => $fmt,  ##-- pack-format for dates (default='n')
##    pack_off => $fmt,   ##-- pack-format for file offsets (default='N')
##    pack_len => $len,   ##-- pack-format for string lengths (default='n')
##    dmax  => $dmax,     ##-- maximum distance for collocation-frequencies and implicit ddc near() queries (default=5)
##    cfmin => $cfmin,    ##-- minimum co-occurrence frequency for Cofreqs and ddc queries (default=2)
##    keeptmp => $bool,   ##-- keep temporary files? (default=0)
##    ##
##    ##-- runtime ddc relation options
##    ddcServer => "$host:$port", ##-- server for ddc relation
##    ddcTimeout => $seconds,     ##-- timeout for ddc relation
##    ##
##    ##-- source filtering (for create())
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
##    ##-- runtime limits
##    maxExpand => $size,       ##-- maximum number of elements in query expansions (default=65535)
##    ##
##    ##-- attribute data
##    ${a}enum => $aenum,   ##-- attribute enum: $aenum : ($dbdir/${a}_enum.*) : $astr<=>$ai : A*<=>N
##                          ##    e.g.  lemmata: $lenum : ($dbdir/l_enum.*   )  : $lstr<=>$li : A*<=>N
##    ${a}2x   => $a2x,     ##-- attribute multimap: $a2x : ($dbdir/${a}_2x.*) : $ai=>@xis  : N=>N*
##    pack_x$a => $fmt      ##-- pack format: extract attribute-id $ai from a packed tuple-string $xs ; $ai=unpack($coldb->{"pack_x$a"},$xs)
##    ##
##    ##-- tuple data (+dates)
##    xenum  => $xenum,     ##-- enum: tuples ($dbdir/xenum.*) : [@ais,$di]<=>$xi : N*n<=>N
##    pack_x => $fmt,       ##-- symbol pack-format for $xenum : "${pack_id}[Nattrs]${pack_date}"
##    xdmin => $xdmin,      ##-- minimum date
##    xdmax => $xdmax,      ##-- maximum date
##    ##
##    ##-- tuple data (-dates)
##    #tenum  => $tenum,     ##-- enum: attribute-tuples (no dates), only if $coldb->{indexAttrs}
##    #pack_t => $fmt,       ##-- symbol pack-format for $tenum : "${pack_id}[Nattrs]"
##    ##
##    ##-- relation data
##    xf    => $xf,       ##-- ug: $xi => $f($xi) : N=>N
##    cof   => $cof,      ##-- cf: [$xi1,$xi2] => $f12
##    ddc   => $ddc,      ##-- ddc client relation
##   )
sub new {
  my $that = shift;
  my $coldb  = bless({
		      ##-- options
		      dbdir => undef,
		      flags => 'r',
		      attrs => undef,
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

		      ##-- limits
		      maxExpand => 65535,

		      ##-- administrivia
		      version => "$VERSION",

		      ##-- attributes
		      #lenum => undef, #$ECLASS->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}),
		      #l2x   => undef, #$MMCLASS->new(pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_id}),
		      #pack_xl => 'N',

		      ##-- tuples
		      #xenum  => undef, #$XECLASS::FixedLen->new(pack_i=>$coldb->{pack_id}, pack_s=>$coldb->{pack_x}),
		      #pack_x => 'Nn',

		      ##-- relations
		      #xf    => undef, #DiaColloDB::Relation::Unigrams->new(packas=>$coldb->{pack_f}),
		      #cof   => undef, #DiaColloDB::Relation::Cofreqs->new(pack_f=>$pack_f, pack_i=>$pack_i, dmax=>$dmax, fmin=>$cfmin),
		      #ddc   => undef, #DiaColloDB::Relation::DDC->new(),

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
  my %mmopts = (%efopts, pack_l=>$coldb->{pack_id});

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
  if (!defined($coldb->{xdmin}) || !defined($coldb->{xdmax})) {
    ##-- hack: guess date-range if not specified
    $coldb->vlog('warn', "Warning: extracting date-range from xenum: you should update $coldb->{dbdir}/header.json");
    my $pack_xdate  = '@'.(packsize($coldb->{pack_id}) * scalar(@{$coldb->attrs})).$coldb->{pack_date};
    my ($dmin,$dmax,$d) = ('inf','-inf');
    foreach (@{$coldb->{xenum}->toArray}) {
      next if (!$_);
      $d = unpack($pack_xdate,$_);
      $dmin = $d if ($d < $dmin);
      $dmax = $d if ($d > $dmax);
    }
    $coldb->vlog('warn', "extracted date-range \"xdmin\":$dmin, \"xdmax\":$dmax");
    @$coldb{qw(xdmin xdmax)} = ($dmin,$dmax);
  }

  ##-- open: xf
  $coldb->{xf} = DiaColloDB::Relation::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$coldb->{pack_f})
    or $coldb->logconfess("open(): failed to open tuple-unigrams $dbdir/xf.dba: $!");
  $coldb->{xf}{N} = $coldb->{xN} if ($coldb->{xN} && !$coldb->{xf}{N}); ##-- compat

  ##-- open: cof
  $coldb->{cof} = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
						     pack_i=>$coldb->{pack_id}, pack_f=>$coldb->{pack_f},
						     dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
						    )
    or $coldb->logconfess("open(): failed to open co-frequency file $dbdir/cof.*: $!");

  ##-- open: ddc (undef if ddcServer isn't set in ddc.hdr or $coldb)
  $coldb->{ddc} = DiaColloDB::Relation::DDC->new(-r "$dbdir/ddc.hdr" ? (base=>"$dbdir/ddc") : qw())->fromDB($coldb)
    // 'DiaColloDB::Relation::DDC';

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
  $coldb->vlog($coldb->{logOpen}, "close(".($coldb->{dbdir}//'').")");
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

## @files = $obj->diskFiles()
##  + get db size (must be opened)
sub diskFiles {
  my $coldb = shift;
  return ("$coldb->{dbdir}/header.json", map {$_->diskFiles} grep {UNIVERSAL::can(ref($_),'diskFiles')} values %$coldb);
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

  my $v2x = $MMCLASS->new(base=>$base, flags=>'rw', perms=>$coldb->{perms}, pack_i=>$pack_id, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_id})
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

## $aname = $CLASS_OR_OBJECT->attrName($attr)
##  + returns canonical (short) attribute name for $attr
##  + supports aliases in %ATTR_ALIAS = ($alias=>$name, ...)
##  + see also:
##     %ATTR_RALIAS = ($name=>\@aliases, ...)
##     %ATTR_CBEXPR = ($name=>$ddcCountByExpr, ...)
##     %ATTR_TITLE = ($name_or_alias=>$title, ...)
our (%ATTR_ALIAS,%ATTR_RALIAS,%ATTR_TITLE,%ATTR_CBEXPR);
BEGIN {
  %ATTR_RALIAS = (
		  'l' => [map {(uc($_),ucfirst($_),$_)} qw(lemma lem l)],
		  'w' => [map {(uc($_),ucfirst($_),$_)} qw(token word w)],
		  'p' => [map {(uc($_),ucfirst($_),$_)} qw(postag tag pt pos p)],
		  ##
		  'doc.collection' => [qw(doc.collection collection doc.corpus corpus)],
		  'doc.textClass'  => [qw(doc.textClass textClass textclass tc doc.genre genre)],
		  'doc.title'      => [qw(doc.title title)],
		  'doc.author'     => [qw(doc.author author)],
		  'doc.basename'   => [qw(doc.basename basename)],
		  'doc.bibl'	   => [qw(doc.bibl bibl)],
		  ##
		  date  => [map {(uc($_),ucfirst($_),$_)} qw(date d)],
		  slice => [map {(uc($_),ucfirst($_),$_)} qw(dslice slice sl ds s)],
		 );
  %ATTR_ALIAS = (map {my $a=$_; map {($_=>$a)} @{$ATTR_RALIAS{$a}}} keys %ATTR_RALIAS);
  %ATTR_TITLE = (
		 'l'=>'lemma',
		 'w'=>'word',
		 'p'=>'pos',
		);
  %ATTR_CBEXPR = (
		  'doc.textClass' => DDC::XS::CQCountKeyExprRegex->new(DDC::XS::CQCountKeyExprBibl->new('textClass'),':.*$',''),
		 );
}
sub attrName {
  shift if (UNIVERSAL::isa($_[0],__PACKAGE__));
  return $ATTR_ALIAS{($_[0]//'')} // $_[0];
}

## $atitle = $CLASS_OR_OBJECT->attrTitle($attr_or_alias)
##  + returns an attribute title for $attr_or_alias
sub attrTitle {
  my ($that,$a) = @_;
  $a = $that->attrName($a//'');
  return $ATTR_TITLE{$a} if (exists($ATTR_TITLE{$a}));
  $a =~ s/^(?:doc|meta)\.//;
  return $a;
}

## $acbexpr = $CLASS_OR_OBJECT->attrCountBy($attr_or_alias,$matchid=0)
sub attrCountBy {
  my ($that,$a,$matchid) = @_;
  $a = $that->attrName($a//'');
  if (exists($ATTR_CBEXPR{$a})) {
    ##-- aliased attribute
    return $ATTR_CBEXPR{$a};
  }
  if ($a =~ /^doc\.(.*)$/) {
    ##-- document attribute ("doc.ATTR" convention)
    return DDC::XS::CQCountKeyExprBibl->new($1);
  } else {
    ##-- token attribute
    return DDC::XS::CQCountKeyExprToken->new($a, ($matchid||0), 0);
  }
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
    $a = $coldb->attrName($attrs->[$_]);
    {i=>$_, a=>$a, enum=>$coldb->{"${a}enum"}, pack_x=>$coldb->{"pack_x$a"}, a2x=>$coldb->{"${a}2x"}}
  } (0..$#$attrs)];
}

## $bool = $coldb->hasAttr($attr)
sub hasAttr {
  return 0 if (!defined($_[1]));
  return $_[1] ne 'x' && defined($_[0]{$_[0]->attrName($_[1]).'enum'});
}


##--------------------------------------------------------------
## create: from corpus

## $bool = $coldb->create($corpus,%opts)
##  + %opts:
##     $key => $val,  ##-- clobbers $coldb->{$key}
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
  my $attrs = $coldb->{attrs} = [map {$coldb->attrName($_)} @{$coldb->attrs(undef,['l'])}];

  ##-- pack-formats
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_x     = $coldb->{pack_x} = $pack_id."[".scalar(@$attrs)."]".$pack_date;

  ##-- initialize: common flags
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = (%efopts, pack_l=>$coldb->{pack_id});

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

  ##-- initialize: enums, date-range
  $coldb->vlog($coldb->{logCreate},"create(): processing $nfiles corpus file(s)");
  my ($xdmin,$xdmax) = ('inf','-inf');
  my ($bos,$eos) = @$coldb{qw(bos eos)};
  my ($doc, $date,$tok,$w,$p,$l,@ais,$x,$xi, $filei);
  my ($last_was_eos,$bosxi,$eosxi);
  for ($corpus->ibegin(); $corpus->iok; $corpus->inext) {
    $coldb->vlog($coldb->{logCorpusFile}, sprintf("create(): processing files [%3d%%]: %s", 100*$filei/$nfiles, $corpus->ifile))
      if ($logFileN && ($filei++ % $logFileN)==0);
    $doc  = $corpus->idocument();
    $date = $doc->{date};

    ##-- get date-range
    $xdmin = $date if ($date < $xdmin);
    $xdmax = $date if ($date > $xdmax);

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
  ##-- store date-range
  @$coldb{qw(xdmin xdmax)} = ($xdmin,$xdmax);

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
  my $xfdb = $coldb->{xf} = DiaColloDB::Relation::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$pack_f)
    or $coldb->logconfess("create(): could not create $dbdir/xf.dba: $!");
  $xfdb->setsize($xenum->{size})
    or $coldb->logconfess("create(): could not set unigram index size = $xenum->{size}: $!");
  $xfdb->create($coldb, $tokfile)
    or $coldb->logconfess("create(): failed to create unigram index: $!");

  ##-- compute collocation frequencies
  $coldb->info("creating co-frequency index $dbdir/cof.* [dmax=$coldb->{dmax}, fmin=$coldb->{cfmin}]");
  my $cof = $coldb->{cof} = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
							       pack_i=>$pack_id, pack_f=>$pack_f,
							       dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
							       keeptmp=>$coldb->{keeptmp},
							      )
    or $coldb->logconfess("create(): failed to create co-frequency index $dbdir/cof.*: $!");
  $cof->create($coldb, $tokfile)
    or $coldb->logconfess("create(): failed to create co-frequency index: $!");

  ##-- create ddc client relation (no-op if ddcServer option is not set)
  if ($coldb->{ddcServer}) {
    $coldb->info("creating ddc client configuration $dbdir/ddc.hdr [ddcServer=$coldb->{ddcServer}]");
    $coldb->{ddc} = DiaColloDB::Relation::DDC->create($coldb);
  } else {
    $coldb->info("ddcServer option unset, NOT creating ddc client configuration");
  }

  ##-- save header
  $coldb->saveHeader()
    or $coldb->logconfess("create(): failed to save header: $!");

  ##-- all done
  $coldb->vlog($coldb->{logCreate}, "create(): DB $dbdir created.");

  ##-- cleanup
  CORE::unlink($tokfile) if (!$coldb->{keeptmp});

  return $coldb;
}

##--------------------------------------------------------------
## create: union (aka merge)

## $coldb = $CLASS_OR_OBJECT->union(\@coldbs_or_dbdirs,%opts)
##  + populates $coldb as union over @coldbs_or_dbdirs
##  + clobbers argument dbs {_union_${a}i2u}, {_union_xi2u}, {_union_argi}
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
  my $attrs = [map {$coldb->attrName($_)} @{$coldb->attrs(undef,[])}];
  my ($db,$dba);
  if (!@$attrs) {
    ##-- use intersection of @dbargs attrs
    my @dbakeys = map {$db=$_; scalar {map {($_=>undef)} @{$db->attrs}}} @dbargs;
    my %akeys   = qw();
    foreach $dba (map {@{$_->attrs}} @dbargs) {
      next if (exists($akeys{$dba}) || grep {!exists($_->{$dba})} @dbakeys);
      $akeys{$dba}=undef;
      push(@$attrs, $dba);
    }
  }
  $coldb->{attrs} = $attrs;
  $coldb->logconfess("union(): no attributes defined and intersection over db attributes is empty!") if (!@$attrs);

  ##-- pack-formats
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_x     = $coldb->{pack_x} = $pack_id."[".scalar(@$attrs)."]".$pack_date; ##-- pack("${pack_id}*${pack_date}", @ais, $date)

  ##-- tuple packing
  $coldb->{"pack_x$attrs->[$_]"} = '@'.($_*packsize($pack_id)).$pack_id foreach (0..$#$attrs);

  ##-- common variables: enums
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});

  ##-- union: attribute enums; also sets $db->{"_union_${a}i2u"} for each attribute $a
  ##   + $db->{"${a}i2u"} is a PackedFile temporary in $dbdir/"${a}_i2u.tmp${argi}"
  my ($ac,$a,$aenum,$as2i,$argi);
  my $adata = $coldb->attrData($attrs);
  foreach $ac (@$adata) {
    $coldb->vlog($coldb->{logCreate}, "union(): creating attribute enum $dbdir/$ac->{a}_enum.*");
    $a     = $ac->{a};
    $aenum = $coldb->{"${a}enum"} = $ac->{enum} = $ECLASS->new(%efopts);
    $as2i  = $aenum->{s2i};
    foreach $argi (0..$#dbargs) {
      ##-- enum union: guts
      $db        = $dbargs[$argi];
      my $dbenum = $db->{"${a}enum"};
      $coldb->vlog($coldb->{logCreate}, "union(): processing $dbenum->{base}.*");
      $aenum->addEnum($dbenum);
      $db->{"_union_argi"}    = $argi;
      $db->{"_union_${a}i2u"} = (DiaColloDB::PackedFile
				 ->new(file=>"$dbdir/${a}_i2u.tmp${argi}", flags=>'rw', packas=>$coldb->{pack_id})
				 ->fromArray( [@$as2i{$dbenum ? @{$dbenum->toArray} : ''}] ))
	or $coldb->logconfess("union(): failed to create temporary $dbdir/${a}_i2u.tmp${argi}");
      $db->{"_union_${a}i2u"}->flush();
    }
    $aenum->save("$dbdir/${a}_enum")
      or $coldb->logconfess("union(): failed to save attribute enum $dbdir/${a}_enum: $!");
  }

  ##-- union: date-range
  $coldb->vlog($coldb->{logCreate}, "union(): computing date-range");
  @$coldb{qw(xdmin xdmax)} = qw(inf -inf);
  foreach $db (@dbargs) {
    $coldb->{xdmin} = $db->{xdmin} if ($db->{xdmin} < $coldb->{xdmin});
    $coldb->{xdmax} = $db->{xdmax} if ($db->{xdmax} > $coldb->{xdmax});
  }

  ##-- union: xenum
  $coldb->vlog($coldb->{logCreate}, "union(): creating tuple-enum $dbdir/xenum.*");
  my $xenum = $coldb->{xenum} = $XECLASS->new(%efopts, pack_s=>$pack_x);
  my $xs2i  = $xenum->{s2i};
  my $nx    = 0;
  foreach $db (@dbargs) {
    $coldb->vlog($coldb->{logCreate}, "union(): processing $db->{xenum}{base}.*");
    my $db_pack_x  = $db->{pack_x};
    my $dbattrs = $db->{attrs};
    my %a2dbxi  = map { ($dbattrs->[$_]=>$_) } (0..$#$dbattrs);
    my %a2i2u = map { ($_=>$db->{"_union_${_}i2u"}) } @$attrs;
    $argi       = $db->{_union_argi};
    my $xi2u    = $db->{_union_xi2u} = DiaColloDB::PackedFile->new(file=>"$dbdir/x_i2u.tmp${argi}", flags=>'rw', packas=>$coldb->{pack_id});
    my $dbxi    = 0;
    my (@dbx,@ux,$uxs,$uxi);
    foreach (@{$db->{xenum}->toArray}) {
      @dbx = unpack($db_pack_x,$_);
      $uxs = pack($pack_x,
		  (map  {
		    (exists($a2dbxi{$_})
		     ? $a2i2u{$_}->fetch($dbx[$a2dbxi{$_}]//0)//0
		     : $a2i2u{$_}->fetch(0)//0)
		  } @$attrs),
		  $dbx[$#dbx]//0);
      $uxi = $xs2i->{$uxs} = $nx++ if (!defined($uxi=$xs2i->{$uxs}));
      $xi2u->store($dbxi++, $uxi);
    }
    $xi2u->flush()
      or $coldb->logconfess("could not flush temporary $dbdir/x_i2u.tmp${argi}");
  }
  $xenum->fromHash($xs2i);
  $xenum->save("$dbdir/xenum")
    or $coldb->logconfess("union(): failed to save $dbdir/xenum: $!");

  ##-- union: expansion maps
  foreach (@$adata) {
    $coldb->create_xmap("$dbdir/$_->{a}_2x",$xs2i,$_->{pack_x},"attribute expansion multimap");
  }

  ##-- intermediate cleanup: xs2i
  undef $xs2i;

  ##-- unigrams: populate
  $coldb->vlog($coldb->{logCreate}, "union(): creating tuple unigram index $dbdir/xf.*");
  $coldb->{xf} = DiaColloDB::Relation::Unigrams->new(file=>"$dbdir/xf.dba", flags=>$flags, packas=>$pack_f)
    or $coldb->logconfess("union(): could not create $dbdir/xf.*: $!");
  $coldb->{xf}->union($coldb, [map {[@$_{qw(xf _union_xi2u)}]} @dbargs])
    or $coldb->logconfess("union(): could not populate unigram index $dbdir/xf.*: $!");

  ##-- co-frequencies: populate
  $coldb->vlog($coldb->{logCreate}, "union(): creating co-frequency index $dbdir/cof.* [fmin=$coldb->{cfmin}]");
  $coldb->{cof} = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
						     pack_i=>$pack_id, pack_f=>$pack_f,
						     dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
						     keeptmp=>$coldb->{keeptmp},
						    )
    or $coldb->logconfess("create(): failed to open co-frequency index $dbdir/cof.*: $!");
  $coldb->{cof}->union($coldb, [map {[@$_{qw(cof _union_xi2u)}]} @dbargs])
    or $coldb->logconfess("union(): could not populate co-frequency index $dbdir/cof.*: $!");

  ##-- cleanup
  if (!$coldb->{keeptmp}) {
    $coldb->vlog($coldb->{logCreate}, "union(): cleaning up temporary files");
    foreach $db (@dbargs) {
      foreach my $pfkey ('_union_xi2u', map {"_union_${_}i2u"} @$attrs) {
	$db->{$pfkey}->unlink() if ($db->{$pfkey}->can('unlink'));
	delete $db->{$pfkey};
      }
      delete $db->{_union_argi};
    }
  }

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
    $coldb->{version} = undef;
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
  my $adata  = $coldb->attrData();
  $coldb->vlog($coldb->{logExport}, "dbexport(): loading enums to memory");
  $coldb->{xenum}->load() if ($coldb->{xenum} && !$coldb->{xenum}->loaded);
  foreach (@$adata) {
    $_->{enum}->load() if ($_->{enum} && !$_->{enum}->loaded);
  }

  ##-- dump: common: stringification
  my $pack_x = $coldb->{pack_x};
  my ($xs2txt,$xi2txt);
  if ($export_sdat) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): preparing tuple-stringification structures");

    foreach (@$adata) {
      my $i2s     = $_->{i2s} = $_->{enum}->toArray;
      $_->{i2txt} = sub { return $i2s->[$_[0]//0]//''; };
    }

    my $xi2s = $coldb->{xenum}->toArray;
    my @ai2s = map {$_->{i2s}} @$adata;
    my (@x);
    $xs2txt = sub {
      @x = unpack($pack_x,$_[0]);
      return join("\t", (map {$ai2s[$_][$x[$_]//0]//''} (0..$#ai2s)), $x[$#x]//0);
    };
    $xi2txt = sub {
      @x = unpack($pack_x, $xi2s->[$_[0]//0]//'');
      return join("\t", (map {$ai2s[$_][$x[$_]//0]//''} (0..$#ai2s)), $x[$#x]//0);
    };
  }

  ##-- dump: xenum: raw
  $coldb->vlog($coldb->{logExport}, "dbexport(): exporting raw tuple-enum file $outdir/xenum.dat");
  $coldb->{xenum}->saveTextFile("$outdir/xenum.dat", pack_s=>$pack_x)
    or $coldb->logconfess("export failed for $outdir/xenum.dat");

  ##-- dump: xenum: stringified
  if ($export_sdat) {
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting stringified tuple-enum file $outdir/xenum.sdat");
    $coldb->{xenum}->saveTextFile("$outdir/xenum.sdat", pack_s=>$xs2txt)
      or $coldb->logconfess("dbexport() failed for $outdir/xenum.sdat");
  }

  ##-- dump: by attribute: enum
  foreach (@$adata) {
    ##-- dump: by attribute: enum
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting attribute enum file $outdir/$_->{a}_enum.dat");
    $_->{enum}->saveTextFile("$outdir/$_->{a}_enum.dat")
      or $coldb->logconfess("dbexport() failed for $outdir/$_->{a}_enum.dat");
  }

  ##-- dump: by attribute: a2x
  foreach (@$adata) {
    ##-- dump: by attribute: a2x: raw
    $coldb->vlog($coldb->{logExport}, "dbexport(): exporting attribute expansion multimap $outdir/$_->{a}_2x.dat (raw)");
    $_->{a2x}->saveTextFile("$outdir/$_->{a}_2x.dat")
      or $coldb->logconfess("dbexport() failed for $outdir/$_->{a}_2x.dat");

    ##-- dump: by attribute: a2x: stringified
    if ($export_sdat) {
      $coldb->vlog($coldb->{logExport}, "dbexport(): exporting attribute expansion multimap $outdir/$_->{a}_2x.sdat (strings)");
      $_->{a2x}->saveTextFile("$outdir/$_->{a}_2x.sdat", a2s=>$_->{i2txt}, b2s=>$xi2txt)
	or $coldb->logconfess("dbexport() failed for $outdir/$_->{a}_2x.sdat");
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
## Info

## \%info = $coldb->dbinfo()
##  + get db info
sub dbinfo {
  my $coldb = shift;
  my $adata = $coldb->attrData();
  my $du    = $coldb->du();
  my $info  = {
	       ##-- literals
	       (map {exists($coldb->{$_}) ? ($_=>$coldb->{$_}) : qw()}
		qw(dbdir bos eos dmax cfmin xdmin xdmax version label collection maintainer)),

	       ##-- disk usage
	       du_b => $du,
	       du_h => si_str($du),

	       ##-- timestamp
	       timestamp => $coldb->timestamp,

	       ##-- attributes
	       attrs => [map {
		 {(
		   name  => $_->{a},
		   title => $coldb->attrTitle($_->{a}),
		   size  => $_->{enum}->size,
		   alias => $ATTR_RALIAS{$_->{a}},
		 )}
	       } @$adata],

	       ##-- relations
	       relations => [grep {UNIVERSAL::isa(ref($coldb->{$_}),'DiaColloDB::Relation')} keys %$coldb],

	       ##-- overrides
	       %{$coldb->{info}//{}},
	      };
  return $info;
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

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
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

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## $obj_or_undef = $coldb->relation($rel)
##  + returns an appropriate relation-like object for profile() and friends
##  + wraps $coldb->{$coldb->relname($rel)}
sub relation {
  return $_[0]->{$_[0]->relname($_[1])};
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## \@ids = $coldb->enumIds($enum,$req,%opts)
##  + parses enum IDs for $req, which is one of:
##    - a DDC::XS::CQTokExact, ::CQTokInfl, ::CQTokSet, ::CQTokSetInfl, or ::CQTokRegex : interpreted
##    - an ARRAY-ref     : list of literal symbol-values
##    - a Regexp ref     : regexp for target strings, passed to $enum->re2i()
##    - a string /REGEX/ : regexp for target strings, passed to $enum->re2i()
##    - another string   : space-, comma-, or |-separated list of literal values
##  + %opts:
##     logLevel => $logLevel, ##-- logging level (default=undef)
##     logPrefix => $prefix,  ##-- logging prefix (default="enumIds(): fetch ids")
sub enumIds {
  my ($coldb,$enum,$req,%opts) = @_;
  $opts{logPrefix} //= "enumIds(): fetch ids";
  if (UNIVERSAL::isa($req,'DDC::XS::CQTokInfl') || UNIVERSAL::isa($req,'DDC::XS::CQTokExact')) {
    ##-- CQuery: CQTokExact
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (", ref($req), ")");
    return [$enum->s2i($req->getValue)];
  }
  elsif (UNIVERSAL::isa($req,'DDC::XS::CQTokSet') || UNIVERSAL::isa($req,'DDC::XS::CQTokSetInfl')) {
    ##-- CQuery: CQTokSet
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (", ref($req), ")");
    return [map {$enum->s2i($_)} @{$req->getValues}];
  }
  elsif (UNIVERSAL::isa($req,'DDC::XS::CQTokRegex')) {
    ##-- CQuery: CQTokRegex
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (", ref($req), ")");
    return $enum->re2i($req->getValue);
  }
  elsif (UNIVERSAL::isa($req,'DDC::XS::CQTokAny')) {
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (", ref($req), ")");
    return undef;
  }
  elsif (UNIVERSAL::isa($req,'ARRAY')) {
    ##-- compat: array
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (ARRAY)");
    return [map {$enum->s2i($_)} @$req];
  }
  elsif (UNIVERSAL::isa($req,'Regexp') || $req =~ m{^/}) {
    ##-- compat: regex
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (REGEX)");
    return $enum->re2i($req);
  }
  elsif (!ref($req)) {
    ##-- compat: space-, comma-, or |-separated literals
    $coldb->vlog($opts{logLevel}, $opts{logPrefix}, " (STRINGS)");
    return [grep {defined($_)} map {$enum->s2i($_)} grep {($_//'') ne ''} map {s{\\(.)}{$1}g; $_} split(/(?:(?<!\\)[\,\s\|])+/,$req)];
  }
  else {
    ##-- reference: unhandled
    $coldb->logconfess($coldb->{error}="$opts{logPrefix}: can't handle request of type ".ref($req));
  }
  return [];
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ($dfilter,$sliceLo,$sliceHi,$dateLo,$dateHi) = $coldb->parseDateRequest($dateRequest='', $sliceRequest=0, $fill=0, $ddcMode=0)
sub parseDateRequest {
  my ($coldb,$date,$slice,$fill,$ddcmode) = @_;
  my ($dfilter,$slo,$shi,$dlo,$dhi);
  if ($date && (UNIVERSAL::isa($date,'Regexp') || $date =~ /^\//)) {
    ##-- date request: regex string
    $coldb->logconfess("parseDateRequest(): can't handle date regex '$date' in ddc mode") if ($ddcmode);
    my $dre  = regex($date);
    $dfilter = sub { $_[0] =~ $dre };
  }
  elsif ($date && $date =~ /^\s*([0-9]+)\s*[\-\:]+\s*([0-9]+)\s*$/) {
    ##-- date request: range MIN:MAX (inclusive)
    ($dlo,$dhi) = ($1+0,$2+0);
    $dfilter = sub { $_[0]>=$dlo && $_[0]<=$dhi };
  }
  elsif ($date && $date =~ /[\s\,\|]/) {
    ##-- date request: list
    my %dwant = map {($_=>undef)} grep {($_//'') ne ''} split(/[\s\,\|]+/,$date);
    $dfilter  = sub { exists($dwant{$_[0]}) };
  }
  elsif ($date) {
    ##-- date request: single value
    $dlo = $dhi = $date;
    $dfilter = sub { $_[0] == $date };
  }

  ##-- force-fill?
  if ($fill) {
    $dlo = $coldb->{xdmin} if (!$dlo || $dlo < $coldb->{xdmin});
    $dhi = $coldb->{xdmax} if (!$dhi || $dhi > $coldb->{xdmax});
  }

  ##-- slice-range
  ($slo,$shi) = map {$slice ? ($slice*int(($_//0)/$slice)) : 0} ($dlo,$dhi);

  return wantarray ? ($dfilter,$slo,$shi,$dlo,$dhi) : $dfilter;
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## \%slice2xids = $coldb->xidsByDate(\@xids, $dateRequest, $sliceRequest, $fill)
##   + parse and filter \@xids by $dateRequest, $sliceRequest
##   + returns a HASH-ref from slice-ids to \@xids in that date-slice
##   + if $fill is true, returned HASH-ref has a key for each date-slice in range
sub xidsByDate {
  my ($coldb,$xids,$date,$slice,$fill) = @_;
  my ($dfilter,$slo,$shi,$dlo,$dhi) = $coldb->parseDateRequest($date,$slice,$fill);

  ##-- filter xids
  my $xenum  = $coldb->{xenum};
  my $pack_x = $coldb->{pack_x};
  my $pack_i = $coldb->{pack_id};
  my $pack_d = $coldb->{pack_date};
  my $pack_xd = "@".(packsize($pack_i) * scalar(@{$coldb->{attrs}})).$pack_d;
  my $d2xis  = {}; ##-- ($dateKey => \@xis_at_date, ...)
  my ($xi,$d);
  foreach $xi (@$xids) {
    $d = unpack($pack_xd, $xenum->i2s($xi));
    next if ($dfilter && !$dfilter->($d));
    $d = $slice ? int($d/$slice)*$slice : 0;
    push(@{$d2xis->{$d}}, $xi);
  }

  ##-- force-fill?
  if ($fill && $slice) {
    for ($d=$slo; $d <= $shi; $d += $slice) {
      $d2xis->{$d} //= [];
    }
  }

  return $d2xis;
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## $compiler = $coldb->qcompiler();
##  + get DDC::XS::CQueryCompiler for this object (cached in $coldb->{_qcompiler})
sub qcompiler {
  return $_[0]{_qcompiler} ||= DDC::XS::CQueryCompiler->new();
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## $cquery_or_undef = $coldb->qparse($ddc_query_string)
##  + wraps parse in an eval {...} block and sets $coldb->{error} on failure
sub qparse {
  my ($coldb,$qstr) = @_;
  my ($q);
  eval { $q=$coldb->compiler->ParseQuery($qstr); };
  if ($@ || !defined($q)) {
    $coldb->{error}="$@";
    return undef;
  }
  return $q;
}


##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## $cquery = $coldb->parseQuery([[$attr1,$val1],...], %opts) ##-- compat: ARRAY-of-ARRAYs
## $cquery = $coldb->parseQuery(["$attr1:$val1",...], %opts) ##-- compat: ARRAY-of-requests
## $cquery = $coldb->parseQuery({$attr1=>$val1, ...}, %opts) ##-- compat: HASH
## $cquery = $coldb->parseQuery("$attr1=$val1, ...", %opts)  ##-- compat: string
## $cquery = $coldb->parseQuery($ddcQueryString, %opts)      ##-- ddc string (with shorthand ","->WITH, "&&"->WITH)
##  + guts for parsing user target and groupby requests
##  + returns a DDC::XS::CQuery object representing the request
##  + index-only items "$l" are mapped to $l=@{}
##  + %opts:
##     warn  => $level,       ##-- log-level for unknown attributes (default: 'warn')
##     logas => $reqtype,     ##-- request type for warnings
##     default => $attr,      ##-- default attribute (for query requests)
##     mapand => $bool,       ##-- map CQAnd to CQWith? (default=true unless '&&' occurs in query string)
##     ddcmode => $bool,      ##-- force ddc query mode? (default=false)
sub parseQuery {
  my ($coldb,$req,%opts) = @_;
  my $req0   = $req;
  my $wlevel = $opts{warn} // 'warn';
  my $defaultIndex = $opts{default};

  ##-- compat: accept ARRAY or HASH requests
  my $areqs = (UNIVERSAL::isa($req,'ARRAY') ? [@$req]
	       : (UNIVERSAL::isa($req,'HASH') ? [%$req]
		  : undef));

  ##-- compat: parse into attribute-local requests $areqs=[[$attr1,$areq1],...]
  my $sepre  = qr{[\s\,]};
  my $charre = qr{(?:\\[^ux0-9]|\w)};
  my $attrre = qr{(?:\$?${charre}+)};
  my $setre  = qr{(?:${charre}|\|)+};			##-- value: |-separated barewords
  my $regre  = qr{(?:/(?:\\/|[^/]*)/(?:[gimsadlux]*))};	##-- value regexes
  my $valre  = qr{(?:${setre}|${regre})};
  my $reqre  = qr{(?:(?:${attrre}[:=])?${valre})};
  if (!$areqs
      && !$opts{ddcmode}
      && $req =~ m/^${sepre}*			##-- initial separators (optional)
		   (?:${reqre}${sepre}+)*	##-- separated components
		   (?:${reqre})			##-- final component
		   ${sepre}*			##-- final separators (optional)
		   $/x) {
    $areqs = [grep {defined($_)} ($req =~ m/${sepre}*(${reqre})/g)];
  }

  ##-- construct DDC query $q
  my ($q);
  if ($areqs) {
    ##-- compat: diacollo<=v0.06-style attribute-wise request in @$areqs; construct DDC query by hand
    my ($a,$areq,$aq);
    foreach (@$areqs) {
      if (UNIVERSAL::isa($_,'ARRAY')) {
	##-- compat: attribute request: ARRAY
	($a,$areq) = @$_;
      } elsif (UNIVERSAL::isa($_,'HASH')) {
	##-- compat: attribute request: HASH
	($a,$areq) = %$_;
      } else {
	##-- compat: attribute request: STRING (native)
	($a,$areq) = m{^(${attrre})[:=](${valre})$} ? ($1,$2) : ($_,undef);
	$a    =~ s/\\(.)/$1/g;
	$areq =~ s/\\(.)/$1/g if (defined($areq));
      }

      ##-- compat: parse defaults
      ($a,$areq) = ('',$a)   if (defined($defaultIndex) && !defined($areq));
      $a = $defaultIndex//'' if (($a//'') eq '');
      $a =~ s/^\$//;

      if (UNIVERSAL::isa($areq,'DDC::XS::CQuery')) {
	##-- compat: value: ddc query object
	$aq = $areq;
	$aq->setIndexName($a) if ($aq->can('setIndexName') && $a ne '');
      }
      elsif (UNIVERSAL::isa($areq,'ARRAY')) {
	##-- compat: value: array --> CQTokSet @{VAL1,...,VALN}
	$aq = DDC::XS::CQTokSet->new($a, '', $areq);
      }
      elsif (UNIVERSAL::isa($areq,'RegExp') || (!$opts{ddcmode} && $areq && $areq =~ m{^${regre}$})) {
	##-- compat: value: regex --> CQTokRegex /REGEX/
	my $re = regex($areq)."";
	$re    =~ s{^\(\?\^\:(.*)\)$}{$1};
	$aq = DDC::XS::CQTokRegex->new($a, $re);
      }
      elsif ($opts{ddcmode} && ($areq//'') ne '') {
	##-- compat: ddcmode: parse requests as ddc queries
	$aq = $coldb->qparse($areq)
	  or $coldb->logconfess($coldb->{error}="parseQuery(): failed to parse request \`$areq': $coldb->{error}");
      }
      else {
	##-- compat: value: space- or |-separated literals --> CQTokExact $a=@VAL or CQTokSet $a=@{VAL1,...VALN} or CQTokAny $a=*
	##   + also applies to empty $areq, e.g. in groupby clauses
	my $vals = [grep {($_//'') ne ''} map {s{\\(.)}{$1}g; $_} split(/(?:(?<!\\)[\,\s\|])+/,($areq//''))];
	$aq = (@$vals<=1
	       ? (($vals->[0]//'*') eq '*'
		  ? DDC::XS::CQTokAny->new($a,'*')
		  : DDC::XS::CQTokExact->new($a,$vals->[0]))
	       : DDC::XS::CQTokSet->new($a,($areq//''),$vals));
      }
      ##-- push request to query
      $q = $q ? DDC::XS::CQWith->new($q,$aq) : $aq;
    }
  }
  else {
    ##-- ddc: diacollo>=v0.06: ddc request parsing: allow shorthands (',' --> 'WITH'), ('INDEX=VAL' --> '$INDEX=VAL'), and ($INDEX --> $INDEX=@{})
    my $compiler = $coldb->qcompiler();
    my ($err);
    while (!defined($q)) {
      #$coldb->trace("req=$req\n");
      undef $@;
      eval { $q=$compiler->ParseQuery($req); };
      last if (!($err=$@) && defined($q));
      if ($err =~ /syntax error/) {
	if ($err =~ /unexpected ','/) {
	  ##-- (X Y) --> (X WITH Y)
	  $req =~ s/(?!<\\)\s*,\s*/ WITH /;
	  next;
	}
	elsif ($err =~ /expecting '='/) {
	  ##-- ($INDEX) --> ($INDEX=*) (for group-by)
	  $req =~ s/(\$\w+)(?!\s*\=)/$1=*/;
	  next;
	}
	elsif ($err =~ /unexpected SYMBOL, expecting INTEGER at line \d+, near token \`([^\']*)\'/) {
	  ##-- (INDEX=) --> ($INDEX=)
	  my $tok = $1;
	  $req =~ s/(?!<\$)(\S+)\s*=\s*\Q$tok\E/\$$1=$tok/;
	  next;
	}
      }
      $coldb->logconfess("parseQuery(): could not parse request '$req0': ", ($err//''));
    }
  }

  ##-- tweak query: map CQAnd to CQWith
  $q = $q->mapTraverse(sub {
			 return UNIVERSAL::isa($_[0],'DDC::XS::CQAnd') ? DDC::XS::CQWith->new($_[0]->getDtr1,$_[0]->getDtr2) : $_[0];
		       })
    if ($opts{mapand} || (!defined($opts{mapand}) && $req0 !~ /\&\&/));

  return $q;
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## \@aqs = $coldb->queryAttributes($cquery,%opts)
##  + utility for decomposing DDC queries into attribute-wise requests
##   + returns an ARRAY-ref [[$attr1,$val1], ...]
##   + each value $vali is empty or undef (all values), a CQTokSet, a CQTokExact, CQTokRegex, or CQTokAny
##   + chokes on unsupported query types or filters
##   + %opts:
##     warn  => $level,       ##-- log-level for unknown attributes (default: 'warn')
##     logas => $reqtype,     ##-- request type for warnings
##     default => $attr,      ##-- default attribute (for query requests)
##     allowUnknown => $bool, ##-- allow unknown attributes? (default: 0)
sub queryAttributes {
  my ($coldb,$cquery,%opts) = @_;
  my $wlevel = $opts{warn} // 'warn';
  my $default = $opts{default};
  my $logas  = $opts{logas}//'';

  my $warnsub = sub {
    $coldb->logconfess($coldb->{error}="queryAttributes(): can't handle ".join('',@_)) if (!$opts{relax});
    $coldb->vlog($wlevel, "queryAttributes(): ignoring ", @_);
  };

  my $areqs = [];
  my ($q,$a,$aq);
  foreach $q (@{$cquery->Descendants}) {
    if (!defined($q)) {
      ##-- NULL: ignore
      next;
    }
    elsif ($q->isa('DDC::XS::CQWith')) {
      ##-- CQWith: ignore (just recurse)
      next;
    }
    elsif ($q->isa('DDC::XS::CQueryOptions')) {
      ##-- CQueryOptions: check for nontrivial user requests
      $warnsub->("#WITHIN clause") if (@{$q->getWithin});
      $warnsub->("#CNTXT clause") if ($q->getContextSentencesCount);
    }
    elsif ($q->isa('DDC::XS::CQToken')) {
      ##-- CQToken: create attribute clause
      $warnsub->("negated query clause in native $logas request (".$q->toString.")") if ($q->getNegated);
      $warnsub->("explicit term-expansion chain in native $logas request (".$q->toString.")") if ($q->can('getExpanders') && @{$q->getExpanders});

      my $a = $q->getIndexName || $default;
      if (ref($q) =~ /^DDC::XS::CQTok(?:Exact|Set|Regex|Any)$/) {
	$aq = $q;
      } elsif (ref($q) eq 'DDC::XS::CQTokInfl') {
	$aq = DDC::XS::CQTokExact->new($q->getIndexName, $q->getValue);
      } elsif (ref($q) eq 'DDC::XS::CQTokSetInfl') {
	$aq = DDC::XS::CQTokSet->new($q->getIndexName, $q->getValue, $q->getValues);
      } elsif (ref($q) eq 'DDC::XS::CQTokPrefix') {
	$aq = DDC::XS::CQTokRegex->new($q->getIndexName, '^'.quotemeta($q->getValue));
      } elsif (ref($q) eq 'DDC::XS::CQTokSuffix') {
	$aq = DDC::XS::CQTokRegex->new($q->getIndexName, quotemeta($q->getValue).'$');
      } elsif (ref($q) eq 'DDC::XS::CQTokInfix') {
	$aq = DDC::XS::CQTokRegex->new($q->getIndexName, quotemeta($q->getValue));
      } else {
	$warnsub->("token query clause of type ".ref($q)." in native $logas request (".$q->toString.")");
      }
      $aq=undef if ($aq && $aq->isa('DDC::XS::CQTokAny')); ##-- empty value, e.g. for groupby
      push(@$areqs, [$a,$aq]);
    }
    elsif ($q->isa('DDC::XS::CQFilter')) {
      ##-- CQFilter
      if ($q->isa('DDC::XS::CQFRandomSort') || $q->isa('DDC::XS::CQFRankSort')) {
	next;
      } elsif ($q->isa('DDC::XS::CQFSort') && ($q->getArg1 ne '' || $q->getArg2 ne '')) {
	$warnsub->("filter of type ".ref($q)." with nontrivial bounds in native $logas request (".$q->toString.")");
      }
    }
    else {
      ##-- something else
      $warnsub->("query clause of type ".ref($q)." in native $logas request (".$q->toString.")");
    }
  }

  ##-- check for unsupported attributes & normalize attribute names
  @$areqs = grep {
    $a = $coldb->attrName($_->[0]);
    if (!$opts{allowUnknown} && !$coldb->hasAttr($a)) {
      $warnsub->("unsupported attribute '".($_->[0]//'(undef)')."' in native $logas request");
      0
    } else {
      $_->[0] = $a;
      1
    }
  } @$areqs;

  return $areqs;
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## \@aqs = $coldb->parseRequest($request, %opts)
##  + guts for parsing user target and groupby requests into attribute-wise ARRAY-ref [[$attr1,$val1], ...]
##  + see parseQuery() method for supported $request formats and %opts
##  + wraps $coldb->queryAttributes($coldb->parseQuery($request,%opts))
sub parseRequest {
  my ($coldb,$req,%opts) = @_;
  return $coldb->queryAttributes($coldb->parseQuery($req,%opts),%opts);
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## \%groupby = $coldb->groupby($groupby_request, %opts)
## \%groupby = $coldb->groupby(\%groupby,        %opts)
##  + $grouby_request : see parseRequest()
##  + returns a HASH-ref:
##    (
##     req => $request,    ##-- save request
##     x2g => \&x2g,       ##-- group-id extraction code suitable for e.g. DiaColloDB::Relation::Cofreqs::profile(groupby=>\&x2g)
##     g2s => \&g2s,       ##-- stringification object suitable for DiaColloDB::Profile::stringify() [CODE,enum, or undef]
##     areqs => \@areqs,   ##-- parsed attribute requests ([$attr,$ahaving],...)
##     attrs => \@attrs,   ##-- like $coldb->attrs($groupby_request), modulo "having" parts
##     titles => \@titles, ##-- like map {$coldb->attrTitle($_)} @attrs
##    )
##  + %opts:
##     warn  => $level,    ##-- log-level for unknown attributes (default: 'warn')
##     relax => $bool,     ##-- allow unsupported attributes (default=0)
sub groupby {
  my ($coldb,$gbreq,%opts) = @_;
  return $gbreq if (UNIVERSAL::isa($gbreq,'HASH'));

  ##-- get data
  my $wlevel = $opts{warn} // 'warn';
  my $gb = { req=>$gbreq };

  ##-- get attribute requests
  my $gbareqs = $gb->{areqs} = $coldb->parseRequest($gb->{req}, %opts,logas=>'groupby');

  ##-- get attribute names (compat)
  my $gbattrs = $gb->{attrs} = [map {$_->[0]} @$gbareqs];

  ##-- get attribute titles
  $gb->{titles} = [map {$coldb->attrTitle($_)} @$gbattrs];

  ##-- get groupby-sub
  my $xenum = $coldb->{xenum};
  my $gbpack = join('',map {$coldb->{"pack_x$_"}} @$gbattrs);
  my ($ids);
  my @gbids  = (
		map {
		  ($_->[1] && !UNIVERSAL::isa($_->[1],'DDC::XS::CQTokAny')
		   ? {
		      map {($_=>undef)}
		      @{$coldb->enumIds($coldb->{"$_->[0]enum"}, $_->[1], logLevel=>$coldb->{logProfile}, logPrefix=>"groupby(): fetch filter ids: $_->[0]")}
		     }
		   : undef)
		} @$gbareqs);
  my ($gbcode,@gi);
  if (grep {$_} @gbids) {
    ##-- group-by code: with having-filters
    $gbcode = (''
	       .qq{ \@gi=unpack('$gbpack',\$xenum->i2s(\$_[0]));}
	       .qq{ return undef if (}.join(' || ', map {"!exists(\$gbids[$_]{\$gi[$_]})"} grep {defined($gbids[$_])} (0..$#gbids)).qq{);}
	       .qq{ return join("\t",\@gi);}
	      );
  }
  else {
    ##-- group-by code: no filters
    $gbcode = qq{join("\t",unpack('$gbpack',\$xenum->i2s(\$_[0])))};
  }
  my $gbsub  = eval qq{sub {$gbcode}};
  $coldb->logconfess($coldb->{error}="groupby(): could not compile aggregation code sub {$gbcode}: $@") if (!$gbsub);
  $@='';
  $gb->{x2g} = $gbsub;

  ##-- get stringification sub
  if (@$gbattrs == 1) {
    $gb->{g2s} = $coldb->{$gbattrs->[0]."enum"}; ##-- stringify a single attribute
  }
  else {
    my @gbe = map {$coldb->{$_."enum"}} @$gbattrs;
    my $g2scode = (q{@gi=split(/\t/,$_[0]);}
		   .q{join("\t",}.join(', ', map {"\$gbe[$_]->i2s(\$gi[$_])"} (0..$#gbe)).q{)}
		    );
    my $g2s = eval qq{sub {$g2scode}};
    $coldb->logconfess($coldb->{error}="groupby(): could not compile stringification code sub {$g2scode}: $@") if (!$g2s);
    $@='';
    $gb->{g2s} = $g2s;
  }

  return $gb;
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## ($CQCountKeyExprs,\$CQRestrict,\@CQFilters) = $coldb->parseGroupBy($groupby_string_or_request,%opts)
##  + for ddc-mode parsing
##  + %opts:
##     date => $date,
##     slice => $slice,
##     matchid => $matchid,    ##-- default match-id
sub parseGroupBy {
  my ($coldb,$req,%opts) = @_;
  $req //= $coldb->attrs;

  ##-- groupby clause: date
  my $gbdate = ($opts{slice}<=0
		? DDC::XS::CQCountKeyExprConstant->new($opts{slice}||'0')
		: DDC::XS::CQCountKeyExprDateSlice->new('date',$opts{slice}));

  ##-- groupby clause: user request
  my $gbexprs   = [$gbdate];
  my $gbfilters = [];
  my ($gbrestr);
  if (!ref($req) && $req =~ m{^\s*(?:\#by)?\[([^\]]*)\]}) {
    ##-- ddc-style request; no restriction-clauses are allowed
    my $cbstr = $1;
    my $gbq = $coldb->qparse("count(*) #by[$cbstr]")
      or $coldb->logconfess($coldb->{error}="failed to parse DDC groupby request \`$req': $coldb->{error}");
    push(@$gbexprs, @{$gbq->getKeys->getExprs});
    $_->setMatchId($opts{matchid}//0)
      foreach (grep {UNIVERSAL::isa($_,'DDC::XS::CQCountKeyExprToken') && !$_->HasMatchId} @$gbexprs);
  }
  else {
    ##-- native-style request with optional restrictions
    my $gbreq  = $coldb->parseRequest($req, logas=>'groupby', default=>undef, relax=>1, allowUnknown=>1);
    foreach (@$gbreq) {
      push(@$gbexprs, $coldb->attrCountBy($_->[0], 2));
      if ($_->[0] =~ /^doc\.(.*)$/) {
	##-- document attribute ("doc.ATTR" convention)
	my $field = $1;
	if (!defined($_->[1]) || UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokAny')) {
	  ;
	}
	elsif (UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokExact') || UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokInfl')) {
	  push(@$gbfilters, DDC::XS::CQFHasField->new($field, $_->[1]->getValue, $_->[1]->getNegated));
	}
	elsif (UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokSet') || UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokSetInfl')) {
	  push(@$gbfilters, DDC::XS::CQFHasFieldSet->new($field, $_->[1]->getValues, $_->[1]->getNegated));
	}
	elsif (UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokRegex')) {
	  push(@$gbfilters, DDC::XS::CQFHasFieldRegex->new($field, $_->[1]->getValue, $_->[1]->getNegated));
	}
	elsif (UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokPrefix')) {
	  push(@$gbfilters, DDC::XS::CQFHasFieldPrefix->new($field, $_->[1]->getValue, $_->[1]->getNegated));
	}
	elsif (UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokSuffix')) {
	  push(@$gbfilters, DDC::XS::CQFHasFieldSuffix->new($field, $_->[1]->getValue, $_->[1]->getNegated));
	}
	elsif (UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokInfix')) {
	  push(@$gbfilters, DDC::XS::CQFHasFieldInfix->new($field, $_->[1]->getValue, $_->[1]->getNegated));
	}
	else {
	  $coldb->logconfess("can't handle metadata restriction of type ", ref($_->[1]), " in groupby request: \`", $_->[1]->toString, "'");
	}
      }
      else {
	##-- token attribute
	if (defined($_->[1]) && !UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokAny')) {
	  $gbrestr = (defined($gbrestr) ? DDC::XS::CQWith->new($gbrestr,$_->[1]) : $_->[1]);
	}
      }
    }
  }

  ##-- finalize: expression list
  my $xlist = DDC::XS::CQCountKeyExprList->new;
  $xlist->setExprs($gbexprs);

  return ($xlist,$gbrestr,$gbfilters);
}

##--------------------------------------------------------------
## Profiling: Generic

## $mprf = $coldb->profile($relation, %opts)
##  + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts:
##    (
##     ##-- selection parameters
##     query => $query,           ##-- target request ATTR:REQ...
##     date  => $date1,           ##-- string or array or range "MIN-MAX" (inclusive) : default=all
##     ##
##     ##-- aggregation parameters
##     slice   => $slice,         ##-- date slice (default=1, 0 for global profile)
##     groupby => $groupby,       ##-- string or array "ATTR1[:HAVING1] ...": default=$coldb->attrs; see groupby() method
##     ##
##     ##-- scoring and trimming parameters
##     eps     => $eps,           ##-- smoothing constant (default=0)
##     score   => $func,          ##-- scoring function ("f"|"fm"|"mi"|"ld") : default="f"
##     kbest   => $k,             ##-- return only $k best collocates per date (slice) : default=-1:all
##     cutoff  => $cutoff,        ##-- minimum score
##     ##
##     ##-- profiling and debugging parameters
##     strings => $bool,          ##-- do/don't stringify (default=do)
##     fill    => $bool,          ##-- if true, returned multi-profile will have null profiles inserted for missing slices
##    )
##  + sets default %opts and wraps $coldb->relation($rel)->profile($coldb, %opts)
sub profile {
  my ($coldb,$rel,%opts) = @_;

  ##-- defaults
  $opts{query}     = (grep {defined($_)} @opts{qw(query q lemma lem l)})[0] // '';
  $opts{date}    //= '';
  $opts{slide}   //= 1;
  $opts{groupby} ||= join(',', map {quotemeta($_)} @{$coldb->attrs});
  $opts{score}   //= 'f';
  $opts{eps}     //= 0;
  $opts{kbest}   //= -1;
  $opts{cutoff}  //= '';
  $opts{strings} //= 1;
  $opts{fill}    //= 0;

  ##-- debug
  $coldb->vlog($coldb->{logRequest},
	       "profile("
	       .join(', ',
		     map {"$_->[0]='".($_->[1]//'')."'"}
		     ([rel=>$rel],
		      [query=>$opts{query}],
		      [groupby=>UNIVERSAL::isa($opts{groupby},'ARRAY') ? join(',', @{$opts{groupby}}) : $opts{groupby}],
		      (map {[$_=>$opts{$_}]} qw(date slice score eps kbest cutoff)),
		     ))
	       .")");

  ##-- relation
  my ($reldb);
  if (!defined($reldb=$coldb->relation($rel||'cof'))) {
    $coldb->logwarn($coldb->{error}="profile(): unknown relation '".($rel//'-undef-')."'");
    return undef;
  }

  ##-- delegate
  return $reldb->profile($coldb,%opts);
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $coldb->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + %opts:
##    (
##     ##-- selection parameters
##     (a|b)?query => $query,       ##-- target query as for parseRequest()
##     (a|b)?date  => $date1,       ##-- string or array or range "MIN-MAX" (inclusive) : default=all
##     ##
##     ##-- aggregation parameters
##     groupby     => $groupby,     ##-- string or array "ATTR1[:HAVING1] ...": default=$coldb->attrs; see groupby() method
##     (a|b)?slice => $slice,       ##-- date slice (default=1, 0 for global profile)
##     ##
##     ##-- scoring and trimming parameters
##     eps     => $eps,           ##-- smoothing constant (default=0)
##     score   => $func,          ##-- scoring function ("f"|"fm"|"mi"|"ld") : default="f"
##     kbest   => $k,             ##-- return only $k best collocates per date (slice) : default=-1:all
##     cutoff  => $cutoff,        ##-- minimum score
##     ##
##     ##-- profiling and debugging parameters
##     strings => $bool,          ##-- do/don't stringify (default=do)
##    )
##  + sets default %opts and wraps $coldb->relation($rel)->compare($coldb, %opts)
BEGIN { *diff = \&compare; }
sub compare {
  my ($coldb,$rel,%opts) = @_;
  $rel //= 'cof';

  ##-- defaults and '[ab]OPTION' parsing
  foreach my $ab (qw(a b)) {
    $opts{"${ab}query"} = ((grep {defined($_)} @opts{map {"${ab}$_"} qw(query q lemma lem l)}),
			   (grep {defined($_)} @opts{qw(query q lemma lem l)})
			  )[0]//'';
  }
  foreach my $a (qw(date slice)) {
    $opts{"a$a"} = ((map {defined($opts{"a$_"}) ? $opts{"a$_"} : qw()} @{$ATTR_RALIAS{$a}}),
		    (map {defined($opts{$_})    ? $opts{$_}    : qw()} @{$ATTR_RALIAS{$a}}),
		   )[0]//'';
    $opts{"b$a"} = ((map {defined($opts{"b$_"}) ? $opts{"b$_"} : qw()} @{$ATTR_RALIAS{$a}}),
		    (map {defined($opts{$_})    ? $opts{$_}    : qw()} @{$ATTR_RALIAS{$a}}),
		   )[0]//'';
  }
  delete @opts{keys %ATTR_ALIAS};

  ##-- debug
  $coldb->vlog($coldb->{logRequest},
	       "compare("
	       .join(', ',
		     map {"$_->[0]='".($_->[1]//'')."'"}
		     ([rel=>$rel],
		      (map {["a$_"=>$opts{"a$_"}]} (qw(query date slice))),
		      (map {["b$_"=>$opts{"b$_"}]} (qw(query date slice))),
		      [groupby=>(UNIVERSAL::isa($opts{groupby},'ARRAY') ? join(',',@{$opts{groupby}}) : $opts{groupby})],
		      (map {[$_=>$opts{$_}]} qw(score eps kbest cutoff)),
		     ))
	       .")");

  ##-- relation
  my ($reldb);
  if (!defined($reldb=$coldb->relation($rel||'cof'))) {
    $coldb->logwarn($coldb->{error}="profile(): unknown relation '".($rel//'-undef-')."'");
    return undef;
  }

  ##-- delegate
  return $reldb->compare($coldb,%opts);
}

##==============================================================================
## Footer
1;

__END__




