## -*- Mode: CPerl -*-
## File: DiaColloDB.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, top-level

package DiaColloDB;
use DiaColloDB::Client;
use DiaColloDB::Logger;
use DiaColloDB::EnumFile;
use DiaColloDB::EnumFile::Identity;
use DiaColloDB::EnumFile::FixedLen;
use DiaColloDB::EnumFile::FixedMap;
use DiaColloDB::EnumFile::MMap;
use DiaColloDB::EnumFile::Tied;
use DiaColloDB::MultiMapFile;
use DiaColloDB::PackedFile;
use DiaColloDB::Relation;
use DiaColloDB::Relation::Unigrams;
use DiaColloDB::Relation::Cofreqs;
use DiaColloDB::Relation::DDC;
#use DiaColloDB::Relation::Vsem;
use DiaColloDB::Profile;
use DiaColloDB::Profile::Multi;
use DiaColloDB::Profile::MultiDiff;
use DiaColloDB::Corpus;
use DiaColloDB::Persistent;
use DiaColloDB::Utils qw(:math :fcntl :json :sort :pack :regex :file :si :run :env);
use DiaColloDB::Timer;
use DDC::XS; ##-- for query parsing
use Tie::File::Indexed::JSON; ##-- for vsem training
use Fcntl;
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = "0.08.001";
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

## $LBAD_DEFAULT
##  + default negative lemma regex for document parsing
our $LBAD_DEFAULT   = undef;

## $VSMGOOD_DEFAULT
##  + default positive meta-field regex for document parsing (vsem only)
##  + don't use qr// here, since Storable doesn't like pre-compiled Regexps
our $VSMGOOD_DEFAULT = q/^(?:author|title|basename|collection|flags|textClass)$/;

## $VSMBAD_DEFAULT
##  + default negative meta-field regex for document parsing (vsem only)
##  + don't use qr// here, since Storable doesn't like pre-compiled Regexps
our $VSMBAD_DEFAULT = q/_$/;

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
#our $MMCLASS = 'DiaColloDB::MultiMapFile::MMap'; ##-- TODO

## %VSOPTS : vsem: default options for DiaColloDB::Relation::Vsem->new()
our %VSOPTS = (
	       minFreq=>10,       ##-- minimum total term-frequency for model inclusion
	       minDocFreq=>5,     ##-- minimim "doc-frequency" (#/docs per term) for model inclusion
	       smoothf=>1,        ##-- smoothing constant
	       #saveMem=>1, 	  ##-- slower but memory-friendlier compilation
	       vtype=>'float',    ##-- store compiled values as 32-bit floats
	       itype=>'long',     ##-- store compiled indices as 32-bit integers
	      );

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
##    #bos => $bos,        ##-- special string to use for BOS, undef or empty for none (default=undef) DISABLED
##    #eos => $eos,        ##-- special string to use for EOS, undef or empty for none (default=undef) DISABLED
##    pack_id => $fmt,    ##-- pack-format for IDs (default='N')
##    pack_f  => $fmt,    ##-- pack-format for frequencies (default='N')
##    pack_date => $fmt,  ##-- pack-format for dates (default='n')
##    pack_off => $fmt,   ##-- pack-format for file offsets (default='N')
##    pack_len => $len,   ##-- pack-format for string lengths (default='n')
##    dmax  => $dmax,     ##-- maximum distance for collocation-frequencies and implicit ddc near() queries (default=5)
##    cfmin => $cfmin,    ##-- minimum co-occurrence frequency for Cofreqs and ddc queries (default=2)
##    tfmin => $tfmin,    ##-- minimum global term-frequency WITHOUT date component (default=2)
##    fmin_${a} => $fmin, ##-- minimum independent frequency for value of attribute ${a} (default=undef:no-min)
##    keeptmp => $bool,   ##-- keep temporary files? (default=0)
##    index_vsem => $bool,##-- vsem: create/use vector-semantic index? (default=undef: if available)
##    index_cof  => $bool,##-- cof: create/use co-frequency index (default=1)
##    vbreak => $vbreak,  ##-- vsem: use break-type $break for vsem index (default=undef: files)
##    vsopts => \%vsopts, ##-- vsem: options for DocClassify::Mapper->new(); default={} (all inherited from %VSOPTS)
##    vbnmin => $vbnmin,  ##-- vsem: minimum number of content-tokens in signature-blocks (default=8)
##                        ##   + for kern (n:%sigs,%toks): 1:0%,0%, 2:5.1%,0.5%, 4:18%,1.6%, 5:22%,2.3%, 8:34%,4.6%, 10:40%,6.5%, 16:54%,12.8%
##    vbnmax => $vbnmax,  ##-- vsem: maximum number of content-tokens in signature-blocks (default=1024)
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
##    vsmgood => $regex,  ##-- positive filter regex for metadata attributes (vsem only)
##    vsmbade => $regex,  ##-- negative filter regex for metadata attributes (vsem only)
##    ##
##    ##-- logging
##    logOpen => $level,        ##-- log-level for open/close (default='info')
##    logCreate => $level,      ##-- log-level for create messages (default='info')
##    logCorpusFile => $level,  ##-- log-level for corpus file-parsing (default='info')
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
##    #${a}2w   => $a2w,     ##-- attribute multimap: $a2w : ($dbdir/${a}_2w.*) : $ai=>@wis  : N=>N*
##    pack_x$a => $fmt      ##-- pack format: extract attribute-id $ai from a packed tuple-string $xs ; $ai=unpack($coldb->{"pack_x$a"},$xs)
##    ##
##    ##-- tuple data (+dates)
##    xenum  => $xenum,     ##-- enum: tuples ($dbdir/xenum.*) : [@ais,$di]<=>$xi : N*n<=>N
##    pack_x => $fmt,       ##-- symbol pack-format for $xenum : "${pack_id}[Nattrs]${pack_date}"
##    xdmin => $xdmin,      ##-- minimum date
##    xdmax => $xdmax,      ##-- maximum date
##    ##
##    ##-- relation data
##    xf    => $xf,       ##-- ug: $xi => $f($xi) : N=>N
##    cof   => $cof,      ##-- cf: [$xi1,$xi2] => $f12
##    ddc   => $ddc,      ##-- ddc client relation
##    vsem  => $vsem,     ##-- vector-space semantic model
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
		      tfmin => 2,
		      #keeptmp => 0,
		      index_vsem => undef,
		      index_cof => 1,
		      vbreak => undef,
		      vsopts => {},
		      vbnmin => 8,
		      vbnmax => 'inf',

		      ##-- filters
		      pgood => $PGOOD_DEFAULT,
		      pbad  => $PBAD_DEFAULT,
		      wgood => $WGOOD_DEFAULT,
		      wbad  => $WBAD_DEFAULT,
		      lgood => $LGOOD_DEFAULT,
		      lbad  => $LBAD_DEFAULT,
		      vsmgood => $VSMGOOD_DEFAULT,
		      vsmbad  => $VSMBAD_DEFAULT,

		      ##-- logging
		      logOpen => 'info',
		      logCreate => 'info',
		      logCorpusFile => 'info',
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

		      ##-- tuples (+dates)
		      #xenum  => undef, #$XECLASS::FixedLen->new(pack_i=>$coldb->{pack_id}, pack_s=>$coldb->{pack_x}),
		      #pack_x => 'Nn',

		      ##-- relations
		      #xf    => undef, #DiaColloDB::Relation::Unigrams->new(packas=>$coldb->{pack_f}),
		      #cof   => undef, #DiaColloDB::Relation::Cofreqs->new(pack_f=>$pack_f, pack_i=>$pack_i, dmax=>$dmax, fmin=>$cfmin),
		      #ddc   => undef, #DiaColloDB::Relation::DDC->new(),
		      #vsem  => undef, #DiaColloDB::Relation::Vsem->new(),

		      @_,	##-- user arguments
		     },
		     ref($that)||$that);
  $coldb->{class}  = ref($coldb);
  $coldb->{pack_w} = $coldb->{pack_id};
  $coldb->{pack_x} = $coldb->{pack_w} . $coldb->{pack_date};
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
  #@$coldb{keys %opts} = values %opts; ##-- clobber options after loadHeader()
  $dbdir //= $coldb->{dbdir};
  $dbdir =~ s{/$}{};
  $coldb->close() if ($coldb->opened);
  $coldb->{dbdir} = $dbdir;
  my $flags = fcflags($opts{flags} // $coldb->{flags});
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
  @$coldb{keys %opts} = values %opts; ##-- clobber header options with user-supplied values

  ##-- open: vsem: require
  $coldb->{index_vsem} = 0 if (!-r "$dbdir/vsem.hdr");
  if ($coldb->{index_vsem}) {
    if (!require "DiaColloDB/Relation/Vsem.pm") {
      $coldb->logwarn("open(): require failed for DiaColloDB/Relation/Vsem.pm ; vector-space modelling disabled", ($@ ? "\n: $@" : ''));
      $coldb->{index_vsem} = 0;
    }
  }

  ##-- open: common options
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = (%efopts, pack_l=>$coldb->{pack_id});

  ##-- open: attributes
  my $attrs = $coldb->{attrs} = $coldb->attrs(undef,['l']);

  ##-- open: by attribute
  my $axat = 0;
  foreach my $attr (@$attrs) {
    ##-- open: ${attr}*
    my $abase = (-r "$dbdir/${attr}_enum.hdr" ? "$dbdir/${attr}_" : "$dbdir/${attr}"); ##-- v0.03-compatibility hack
    $coldb->{"${attr}enum"} = $ECLASS->new(base=>"${abase}enum", %efopts)
      or $coldb->logconfess("open(): failed to open enum ${abase}enum.*: $!");
    $coldb->{"${attr}2x"} = $MMCLASS->new(base=>"${abase}2x", %mmopts)
      or $coldb->logconfess("open(): failed to open expansion multimap ${abase}2x.*: $!");
    $coldb->{"pack_x$attr"} //= "\@${axat}$coldb->{pack_id}";
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
      next if (!defined($d = unpack($pack_xdate,$_))); ##-- strangeness: getting only 9-bytes in $_ for 10-byte values in file and toArray(): why?!
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
  if ($coldb->{index_cof}//1) {
    $coldb->{cof} = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
						       pack_i=>$coldb->{pack_id}, pack_f=>$coldb->{pack_f},
						       dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
						      )
      or $coldb->logconfess("open(): failed to open co-frequency file $dbdir/cof.*: $!");
  }

  ##-- open: ddc (undef if ddcServer isn't set in ddc.hdr or $coldb)
  $coldb->{ddc} = DiaColloDB::Relation::DDC->new(-r "$dbdir/ddc.hdr" ? (base=>"$dbdir/ddc") : qw())->fromDB($coldb)
    // 'DiaColloDB::Relation::DDC';

  ##-- open: vsem (if available)
  if ($coldb->{index_vsem}) {
    $coldb->{vsopts}     //= {};
    $coldb->{vsopts}{$_} //= $VSOPTS{$_} foreach (keys %VSOPTS); ##-- vsem: default options
    $coldb->{vsem} = DiaColloDB::Relation::Vsem->new((-r "$dbdir/vsem.hdr" ? (base=>"$dbdir/vsem") : qw()),
						     %{$coldb->{vsopts}}
						    );
  }

  ##-- all done
  return $coldb;
}


## @dbkeys = $coldb->dbkeys()
sub dbkeys {
  return (
	  (ref($_[0]) ? (map {($_."enum",$_."2x")} @{$_[0]->attrs}) : qw()),
	  qw(xenum xf cof vsem),
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
##  + returns list of dist files for this db
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
		  'doc.flags'      => [qw(doc.flags flags)],
		  ##
		  date  => [map {(uc($_),ucfirst($_),$_)} qw(date d)],
		  slice => [map {(uc($_),ucfirst($_),$_)} qw(dslice slice sl ds s)],
		 );
  %ATTR_ALIAS = (map {my $attr=$_; map {($_=>$attr)} @{$ATTR_RALIAS{$attr}}} keys %ATTR_RALIAS);
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
  my ($that,$attr) = @_;
  $attr = $that->attrName($attr//'');
  return $ATTR_TITLE{$attr} if (exists($ATTR_TITLE{$attr}));
  $attr =~ s/^(?:doc|meta)\.//;
  return $attr;
}

## $acbexpr = $CLASS_OR_OBJECT->attrCountBy($attr_or_alias,$matchid=0)
sub attrCountBy {
  my ($that,$attr,$matchid) = @_;
  $attr = $that->attrName($attr//'');
  if (exists($ATTR_CBEXPR{$attr})) {
    ##-- aliased attribute
    return $ATTR_CBEXPR{$attr};
  }
  if ($attr =~ /^doc\.(.*)$/) {
    ##-- document attribute ("doc.ATTR" convention)
    return DDC::XS::CQCountKeyExprBibl->new($1);
  } else {
    ##-- token attribute
    return DDC::XS::CQCountKeyExprToken->new($attr, ($matchid||0), 0);
  }
}

## $aquery_or_filter_or_undef = $CLASS_OR_OBJECT->attrQuery($attr_or_alias,$cquery)
##  + returns a CQuery or CQFilter object for condition $cquery on $attr_or_alias
sub attrQuery {
  my ($that,$attr,$cquery) = @_;
  $attr = $that->attrName( $attr // ($cquery ? $cquery->getIndexName : undef) // '' );
  if ($attr =~ /^doc\./) {
    ##-- document attribute ("doc.ATTR" convention)
    return $that->query2filter($attr,$cquery);
  }
  ##-- token condition (use literal $cquery)
  return $cquery;
}

## \@attrdata = $coldb->attrData()
## \@attrdata = $coldb->attrData(\@attrs=$coldb->attrs)
##  + get attribute data for \@attrs
##  + return @attrdata = ({a=>$attr, i=>$i, enum=>$aenum, pack_x=>$pack_xa, a2x=>$a2x, ...})
sub attrData {
  my ($coldb,$attrs) = @_;
  $attrs //= $coldb->attrs;
  my ($attr);
  return [map {
    $attr = $coldb->attrName($attrs->[$_]);
    {i=>$_, a=>$attr, enum=>$coldb->{"${attr}enum"}, pack_x=>$coldb->{"pack_x$attr"}, a2x=>$coldb->{"${attr}2x"}}
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

  ##-- initialize: vsem
  $coldb->{index_vsem} //= 1;
  if ($coldb->{index_vsem}) {
    if (!require "DiaColloDB/Relation/Vsem.pm") {
      $coldb->logwarn("create(): require failed for DiaColloDB/Relation/Vsem.pm ; vector-space modelling disabled", ($@ ? "\n: $@" : ''));
      $coldb->{index_vsem} = 0;
    } else {
      $coldb->info("vector-space modelling via DiaColloDB::Relation::Vsem enabled.");
    }
  }

  ##-- initialize: attributes
  my $attrs = $coldb->{attrs} = [map {$coldb->attrName($_)} @{$coldb->attrs(undef,['l'])}];

  ##-- pack-formats
  my $pack_id    = $coldb->{pack_id};
  my $pack_date  = $coldb->{pack_date};
  my $pack_f     = $coldb->{pack_f};
  my $pack_off   = $coldb->{pack_off};
  my $pack_len   = $coldb->{pack_len};
  my $pack_w     = $coldb->{pack_w} = $pack_id."[".scalar(@$attrs)."]";
  my $pack_x     = $coldb->{pack_x} = $pack_w.$pack_date;

  ##-- initialize: common flags
  my %efopts = (flags=>$flags, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my %mmopts = (%efopts, pack_l=>$coldb->{pack_id});

  ##-- initialize: attribute enums
  my $aconf = [];  ##-- [{a=>$attr, i=>$i, enum=>$aenum, pack_x=>$pack_xa, s2i=>\%s2i, ns=>$nstrings, ?i2j=>$vec, ...}, ]
  my $axpos = 0;
  my ($attr,$ac);
  foreach (0..$#$attrs) {
    push(@$aconf,$ac={i=>$_, a=>($attr=$attrs->[$_])});
    $ac->{enum}   = $coldb->{"${attr}enum"} = $ECLASS->new(%efopts);
    $ac->{pack_x} = $coldb->{"pack_x$attr"} = '@'.$axpos.$pack_id;
    $ac->{s2i}    = $ac->{enum}{s2i};
    $ac->{ma}     = $1 if ($attr =~ /^(?:meta|doc)\.(.*)$/);
    $axpos       += packsize($pack_id);
  }
  my @aconfm = grep { defined($_->{ma})} @$aconf; ##-- meta-attributes
  my @aconfw = grep {!defined($_->{ma})} @$aconf; ##-- token-attributes

  ##-- initialize: tuple enum (+dates)
  my $xenum = $coldb->{xenum} = $XECLASS->new(%efopts, pack_s=>$pack_x);
  my $xs2i  = $xenum->{s2i};
  my $nx    = 0;

  ##-- initialize: corpus token-list (temporary)
  ##  + 1 token/line, blank lines ~ EOS, token lines ~ "$a0i $a1i ... $aNi $date"
  my $atokfile =  "$dbdir/atokens.dat";
  CORE::open(my $atokfh, ">$atokfile")
    or $coldb->logconfess("$0: open failed for $atokfile: $!");

  ##-- initialize: vsem: doc-data array (temporary)
  my ($doctmpa);
  my $doctmpn = 0; ##-- current size of @$doctmpa
  my $index_vsem = $coldb->{index_vsem};
  if ($index_vsem) {
    $doctmpa = $coldb->{doctmpa} = [];
    tie(@$doctmpa, 'Tie::File::Indexed::JSON', "$dbdir/doctmp.a", mode=>'rw', temp=>!$coldb->{keeptmp}, pack_o=>'J', pack_l=>'J')
      or $coldb->logconfess("create(): could not tie temporary doc-data array to $dbdir/doctmp.a: $!");
  }
  my $vbnmin = $coldb->{vbnmin} = max2(($coldb->{vbnmin}//0),1);
  my $vbnmax = $coldb->{vbnmax} = min2(($coldb->{vbnmax}//'inf'),'inf');
  my $vbreak = ($coldb->{vbreak} // '#file');
  $vbreak    = "#$vbreak" if ($vbreak !~ /^#/);
  $coldb->{vbreak} = $vbreak;

  ##-- initialize: filter regexes
  my $pgood = $coldb->{pgood} ? qr{$coldb->{pgood}} : undef;
  my $pbad  = $coldb->{pbad}  ? qr{$coldb->{pbad}}  : undef;
  my $wgood = $coldb->{wgood} ? qr{$coldb->{wgood}} : undef;
  my $wbad  = $coldb->{wbad}  ? qr{$coldb->{wbad}}  : undef;
  my $lgood = $coldb->{lgood} ? qr{$coldb->{lgood}} : undef;
  my $lbad  = $coldb->{lbad}  ? qr{$coldb->{lbad}}  : undef;

  ##-- initialize: logging
  my $nfiles   = $corpus->size();
  my $logFileN = $coldb->{logCorpusFileN} // max2(1,int($nfiles/20));

  ##-- initialize: enums, date-range
  $coldb->vlog($coldb->{logCreate},"create(): processing $nfiles corpus file(s)");
  my ($xdmin,$xdmax) = ('inf','-inf');
  my ($doc, $date,$tok,$w,$p,$l,@ais,$aistr,$x,$xi, $wx,@sigs,$sig,$sign, $filei, $last_was_eos);
  for ($corpus->ibegin(); $corpus->iok; $corpus->inext) {
    $coldb->vlog($coldb->{logCorpusFile}, sprintf("create(): processing files [%3.0f%%]: %s", 100*($filei-1)/$nfiles, $corpus->ifile))
      if ($logFileN && ($filei++ % $logFileN)==0);
    $doc  = $corpus->idocument();
    $date = $doc->{date};

    ##-- initalize vsem signatures
    @sigs = qw();
    $sig  = {}; ##-- ($wi => $count, ...)
    $sign = 0;

    ##-- get date-range
    $xdmin = $date if ($date < $xdmin);
    $xdmax = $date if ($date > $xdmax);

    ##-- get meta-attributes
    @ais = qw();
    $ais[$_->{i}] = ($_->{s2i}{$doc->{meta}{$_->{ma}}} //= ++$_->{ns}) foreach (@aconfm);

    ##-- iterate over tokens, populating initial attribute-enums and writing $atokfile
    $last_was_eos = 1;
    foreach $tok (@{$doc->{tokens}}) {
      if (ref($tok)) {
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
	$aistr        = join(' ',@ais);

	#$x  = pack($pack_x,@ais,$date);
	#$xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x}));

	$atokfh->print("$aistr $date\n");
	$last_was_eos = 0;

	##-- extract signature for vsem
	if ($index_vsem) {
	  ++$sig->{$aistr};
	  ++$sign;
	}
      }
      elsif (!defined($tok) && !$last_was_eos) {
	##-- eos
	$atokfh->print("\n");
	$last_was_eos = 1;
      }
      elsif (defined($tok) && $tok eq $vbreak && $sign >= $vbnmin && $sign <= $vbnmax) {
	##-- break:vsem
	push(@sigs,$sig);
	$sig  = {};
	$sign = 0;
      }
    }

    ##-- store doc-data (for vsem)
    if ($doctmpa) {
      push(@sigs,$sig) if ($sign >= $vbnmin && $sign <= $vbnmax);
      push(@$doctmpa, {
		       sigs => \@sigs,
		       id   => $doctmpn++,
		       (map {($_=>$doc->{$_})} qw(meta date label)),
		      })
	if (@sigs);
    }
  }
  ##-- store date-range
  @$coldb{qw(xdmin xdmax)} = ($xdmin,$xdmax);

  ##-- close temporary attribute-token file(s)
  CORE::close($atokfh)
      or $corpus->logconfess("create(): failed to close temporary token storage file '$atokfile': $!");

  ##-- filter: by attribute frequency
  my $ibits = packsize($pack_id)*8;
  my $ibad  = unpack($pack_id,pack($pack_id,-1));
  foreach $ac (@$aconf) {
    my $afmin = $coldb->{"fmin_".$ac->{a}} // 0;
    next if ($afmin <= 0);
    $coldb->vlog($coldb->{logCreate}, "create(): building attribute frequency filter (fmin_$ac->{a}=$afmin)");

    ##-- filter: by attribute frequency: setup re-numbering map $ac->{i2j}
    $ac->{i2j} = '';
    my $i2j    = \$ac->{i2j};
    vec($$i2j, $ac->{ns}, $ibits) = $ibad;

    ##-- filter: by attribute frequency: populate $ac->{i2j} and update $ac->{s2i}
    env_push(LC_ALL=>'C');
    my $ai1   = $ac->{i}+1;
    my $cmdfh = opencmd("cut -d\" \" -f$ai1 $atokfile | sort -n | uniq -c |")
      or $coldb->logconfess("create(): failed to open pipe from sort for attribute frequency filter (fmin_$ac->{a}=$afmin)");
    my ($f,$i);
    my $nj   = 0;
    while (defined($_=<$cmdfh>)) {
      chomp;
      ($f,$i) = split(' ',$_,2);
      vec($$i2j, $i, $ibits) = ($f >= $afmin ? ++$nj : $ibad) if ($i);
    }
    $cmdfh->close();
    env_pop();

    my $nabad = $ac->{ns} - $nj;
    my $pabad = $ac->{ns} ? sprintf("%.2f%%", 100*$nabad/$ac->{ns}) : 'nan%';
    $coldb->vlog($coldb->{logCreate}, "create(): filter (fmin_$ac->{a}=$afmin) pruning $nabad of $ac->{ns} attribute value type(s) ($pabad)");

    my $s2i  = $ac->{s2i};
    delete @$s2i{grep {vec($$i2j, $s2i->{$_}, $ibits)==$ibad} keys %$s2i};
    $s2i->{$_} = vec($$i2j, $s2i->{$_}, $ibits) foreach (keys %$s2i);
    $ac->{ns} = $nj;
  }

  ##-- filter: terms: populate $ws2w (map IDs)
  my $tfmin = $coldb->{tfmin}//0;
  my $ws2w  = undef; ##-- join(' ',@ais) => pack($pack_w,i2j(@ais)) : includes attribute-id re-mappings
  if ($tfmin > 0 || grep {defined($_->{i2j})} @$aconf) {
    $coldb->vlog($coldb->{logCreate}, "create(): populating global term enum (tfmin=$tfmin)");
    my @ai2j  = map {defined($_->{i2j}) ? \$_->{i2j} : undef} @$aconf;
    my @ai2ji = grep {defined($ai2j[$_])} (0..$#ai2j);
    my $na        = scalar(@$attrs);
    my ($nw0,$nw) = (0,0);
    my ($f);
    env_push(LC_ALL=>'C');
    my $cmdfh = opencmd("cut -d \" \" -f -$na $atokfile | sort -n | uniq -c |")
      or $coldb->logconfess("create(): failed to open pipe from sort for global term filter");
  FILTER_WTUPLES:
    while (defined($_=<$cmdfh>)) {
      chomp;
      ++$nw0;
      ($f,$aistr) = split(' ',$_,2);
      next if (!$aistr || $f < $tfmin);
      @ais = split(' ',$aistr,$na);
      foreach (@ai2ji) {
	##-- apply attribute-wise re-mappings
	$ais[$_] = vec(${$ai2j[$_]}, ($ais[$_]//0), $ibits);
	next FILTER_WTUPLES if ($ais[$_] == $ibad);
      }
      $ws2w->{$aistr} = pack($pack_w,@ais);
      ++$nw;
    }
    $cmdfh->close();
    env_pop();

    my $nwbad = $nw0 - $nw;
    my $pwbad = $nw0 ? sprintf("%.2f%%", 100*$nwbad/$nw0) : 'nan%';
    $coldb->vlog($coldb->{logCreate}, "create(): will prune $nwbad of $nw0 term tuple type(s) ($pwbad)");
  }

  ##-- compile: apply filters & assign term-ids
  $coldb->vlog($coldb->{logCreate}, "create(): filtering corpus tokens & assigning term-IDs");
  my $tokfile =  "$dbdir/tokens.dat";
  CORE::open(my $tokfh, ">$tokfile")
      or $coldb->logconfess("$0: open failed for $tokfile: $!");
  CORE::open($atokfh, "<$atokfile")
      or $coldb->logconfess("$0: re-open failed for $atokfile: $!");
  my $len_w = packsize($pack_w);
  $nx       = 0;
  my ($ntok_in,$ntok_out) = (0,0);
  while (defined($_=<$atokfh>)) {
    chomp;
    if ($_) {
      ++$ntok_in;
      if (defined($ws2w)) {
	$date = $1 if (s/ ([0-9]+)$//);
	next if (!defined($w=$ws2w->{$_}));
	$x = $w.pack($pack_date,$date);
      } else {
	$x = pack($pack_x,split(' ',$_));
      }
      $xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x}));
      $tokfh->print($xi,"\n");
      ++$ntok_out;
    }
    else {
      $tokfh->print("\n");
    }
  }
  CORE::close($atokfh)
      or $coldb->logconfess("create(): failed to temporary attribute-token-file $atokfile: $!");
  CORE::close($tokfh)
      or $coldb->logconfess("create(): failed to temporary token-file $tokfile: $!");
  my $ptokbad = $ntok_in ? sprintf("%.2f%%",100*($ntok_in-$ntok_out)/$ntok_in) : 'nan%';
  $coldb->vlog($coldb->{logCreate}, "create(): assigned $nx term+date tuple-IDs to $ntok_out of $ntok_in tokens (pruned $ptokbad)");

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

    ##-- compile: by attribute: expansion multimaps (+dates)
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
  if ($coldb->{index_cof}//1) {
    $coldb->info("creating co-frequency index $dbdir/cof.* [dmax=$coldb->{dmax}, fmin=$coldb->{cfmin}]");
    my $cof = $coldb->{cof} = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
								 pack_i=>$pack_id, pack_f=>$pack_f,
								 dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
								 keeptmp=>$coldb->{keeptmp},
								)
      or $coldb->logconfess("create(): failed to create co-frequency index $dbdir/cof.*: $!");
    $cof->create($coldb, $tokfile)
      or $coldb->logconfess("create(): failed to create co-frequency index: $!");
  } else {
    $coldb->info("NOT creating co-frequency index $dbdir/cof.*; set index_cof=1 to enable");
  }

  ##-- create vector-space model (if requested & available)
  if ($coldb->{index_vsem}) {
    $coldb->info("creating vector-space model $dbdir/vsem* [vbreak=$vbreak]");
    $coldb->{vsopts}     //= {};
    $coldb->{vsopts}{$_} //= $VSOPTS{$_} foreach (keys %VSOPTS); ##-- vsem: default options
    $coldb->{vsem} = DiaColloDB::Relation::Vsem->create($coldb, undef, base=>"$dbdir/vsem");
  } else {
    $coldb->info("NOT creating vector-space model, 'vsem' profiling relation disabled");
  }

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
  if (!$coldb->{keeptmp}) {
    CORE::unlink($tokfile)
	or $coldb->logwarn("create(): clould not remove temporary file '$tokfile': $!");
    CORE::unlink($atokfile)
	or $coldb->logwarn("create(): clould not remove temporary file '$atokfile': $!");
  }
  !$doctmpa
    or !tied(@$doctmpa)
    or untie(@$doctmpa)
    or $coldb->logwarn("create(): could untie temporary doc-data array $dbdir/doctmp.a: $!");
  delete $coldb->{doctmpa};

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

  ##-- union: attribute enums; also sets $db->{"_union_${a}i2u"} for each attribute $attr
  ##   + $db->{"${a}i2u"} is a PackedFile temporary in $dbdir/"${a}_i2u.tmp${argi}"
  my ($ac,$attr,$aenum,$as2i,$argi);
  my $adata = $coldb->attrData($attrs);
  foreach $ac (@$adata) {
    $coldb->vlog($coldb->{logCreate}, "union(): creating attribute enum $dbdir/$ac->{a}_enum.*");
    $attr  = $ac->{a};
    $aenum = $coldb->{"${attr}enum"} = $ac->{enum} = $ECLASS->new(%efopts);
    $as2i  = $aenum->{s2i};
    foreach $argi (0..$#dbargs) {
      ##-- enum union: guts
      $db        = $dbargs[$argi];
      my $dbenum = $db->{"${attr}enum"};
      $coldb->vlog($coldb->{logCreate}, "union(): processing $dbenum->{base}.*");
      $aenum->addEnum($dbenum);
      $db->{"_union_argi"}    = $argi;
      $db->{"_union_${attr}i2u"} = (DiaColloDB::PackedFile
				 ->new(file=>"$dbdir/${attr}_i2u.tmp${argi}", flags=>'rw', packas=>$coldb->{pack_id})
				 ->fromArray( [@$as2i{$dbenum ? @{$dbenum->toArray} : ''}] ))
	or $coldb->logconfess("union(): failed to create temporary $dbdir/${attr}_i2u.tmp${argi}");
      $db->{"_union_${attr}i2u"}->flush();
    }
    $aenum->save("$dbdir/${attr}_enum")
      or $coldb->logconfess("union(): failed to save attribute enum $dbdir/${attr}_enum: $!");
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
  if ($coldb->{index_cof}//1) {
    $coldb->vlog($coldb->{logCreate}, "union(): creating co-frequency index $dbdir/cof.* [fmin=$coldb->{cfmin}]");
    $coldb->{cof} = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", flags=>$flags,
						       pack_i=>$pack_id, pack_f=>$pack_f,
						       dmax=>$coldb->{dmax}, fmin=>$coldb->{cfmin},
						       keeptmp=>$coldb->{keeptmp},
						      )
      or $coldb->logconfess("create(): failed to open co-frequency index $dbdir/cof.*: $!");
    $coldb->{cof}->union($coldb, [map {[@$_{qw(cof _union_xi2u)}]} @dbargs])
      or $coldb->logconfess("union(): could not populate co-frequency index $dbdir/cof.*: $!");
  } else {
    $coldb->vlog($coldb->{logCreate}, "union(): NOT creating co-frequency index $dbdir/cof.*; set index_cof=1 to enable");
  }

  ##-- vsem: populate
  my $db_vsem            = !grep {!$_->{index_vsem}} @dbargs;
  $coldb->{index_vsem} //= $db_vsem;
  if ($coldb->{index_vsem} && $db_vsem) {
    $coldb->vlog($coldb->{logCreate}, "union(): creating vector-space index $dbdir/vsem.*");
    my $vsopts0            = $dbargs[0]{vsem}{vsopts};
    $coldb->{vsopts}     //= {};
    $coldb->{vsopts}     //= $vsopts0->{$_} foreach (keys %$vsopts0); ##-- vsem: inherit options
    $coldb->{vsopts}{$_} //= $VSOPTS{$_}    foreach (keys %VSOPTS);   ##-- vsem: default options
    $coldb->{vsem} = DiaColloDB::Relation::Vsem->union($coldb, \@dbargs,
						       base => "$dbdir/vsem",
						       flags => $flags,
						       keeptmp => $coldb->{keeptmp},
						       %{$coldb->{vsopts}},
						      )
      or $coldb->logconfess("create(): failed to populate vector-space index $dbdir/vsem.*: $!");
  } else {
    $coldb->vlog($coldb->{logCreate}, "union(): NOT creating vector-space index $dbdir/vsem.*; set index_vsem=1 on all argument DBs to enable");
  }

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
  return (qw(attrs), grep {!ref($_[0]{$_}) && $_ !~ m{^(?:dbdir$|flags$|perms$|info$|vsopts$|log)}} keys %{$_[0]});
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
	       #relations => [$coldb->relations],
	       relations => { map {($_=>$coldb->{$_}->dbinfo($coldb))} $coldb->relations },

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
  elsif ($rel =~ m/^(?:v|sem|tdm|tfidf)/) {
    return 'vsem';
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
## @relnames = $coldb->relations()
##  + gets list of defined relations
sub relations {
  return grep {UNIVERSAL::isa(ref($_[0]{$_}),'DiaColloDB::Relation')} keys %{$_[0]};
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
    next if (($dfilter && !$dfilter->($d)) || $d < $coldb->{xdmin} || $d > $coldb->{xdmax});
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
  eval { $q=$coldb->qcompiler->ParseQuery($qstr); };
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
    my ($attr,$areq,$aq);
    foreach (@$areqs) {
      if (UNIVERSAL::isa($_,'ARRAY')) {
	##-- compat: attribute request: ARRAY
	($attr,$areq) = @$_;
      } elsif (UNIVERSAL::isa($_,'HASH')) {
	##-- compat: attribute request: HASH
	($attr,$areq) = %$_;
      } else {
	##-- compat: attribute request: STRING (native)
	($attr,$areq) = m{^(${attrre})[:=](${valre})$} ? ($1,$2) : ($_,undef);
	$attr =~ s/\\(.)/$1/g;
	$areq =~ s/\\(.)/$1/g if (defined($areq));
      }

      ##-- compat: parse defaults
      ($attr,$areq) = ('',$attr)   if (defined($defaultIndex) && !defined($areq));
      $attr = $defaultIndex//'' if (($attr//'') eq '');
      $attr =~ s/^\$//;

      if (UNIVERSAL::isa($areq,'DDC::XS::CQuery')) {
	##-- compat: value: ddc query object
	$aq = $areq;
	$aq->setIndexName($attr) if ($aq->can('setIndexName') && $attr ne '');
      }
      elsif (UNIVERSAL::isa($areq,'ARRAY')) {
	##-- compat: value: array --> CQTokSet @{VAL1,...,VALN}
	$aq = DDC::XS::CQTokSet->new($attr, '', $areq);
      }
      elsif (UNIVERSAL::isa($areq,'RegExp') || (!$opts{ddcmode} && $areq && $areq =~ m{^${regre}$})) {
	##-- compat: value: regex --> CQTokRegex /REGEX/
	my $re = regex($areq)."";
	$re    =~ s{^\(\?\^\:(.*)\)$}{$1};
	$aq = DDC::XS::CQTokRegex->new($attr, $re);
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
		  ? DDC::XS::CQTokAny->new($attr,'*')
		  : DDC::XS::CQTokExact->new($attr,$vals->[0]))
	       : DDC::XS::CQTokSet->new($attr,($areq//''),$vals));
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
  my ($q,$attr,$aq);
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

      my $attr = $q->getIndexName || $default;
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
      push(@$areqs, [$attr,$aq]);
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
    $attr = $coldb->attrName($_->[0]);
    if (!$opts{allowUnknown} && !$coldb->hasAttr($attr)) {
      $warnsub->("unsupported attribute '".($_->[0]//'(undef)')."' in native $logas request");
      0
    } else {
      $_->[0] = $attr;
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
##     xenum => $xenum,    ##-- enum to use for \&x2g and \&g2s (default: $coldb->{xenum})
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
  my $xenum = $opts{xenum} // $coldb->{xenum};
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
## $cqfilter = $coldb->query2filter($attr,$cquery,%opts)
##  + converts a CQToken to a CQFilter, for ddc parsing
##  + %opts:
##     logas => $logas,   ##-- log-prefix for warnings
sub query2filter {
  my ($coldb,$attr,$q,%opts) = @_;
  return undef if (!defined($q));
  my $logas = $opts{logas} || 'query2filter';

  ##-- document attribute ("doc.ATTR" convention)
  my $field = $coldb->attrName( $attr // $q->getIndexName );
  $field = $1 if ($field =~ /^doc\.(.*)$/);
  if (UNIVERSAL::isa($q, 'DDC::XS::CQTokAny')) {
    return undef;
  } elsif (UNIVERSAL::isa($q, 'DDC::XS::CQTokExact') || UNIVERSAL::isa($q, 'DDC::XS::CQTokInfl')) {
    return DDC::XS::CQFHasField->new($field, $q->getValue, $q->getNegated);
  } elsif (UNIVERSAL::isa($q, 'DDC::XS::CQTokSet') || UNIVERSAL::isa($q, 'DDC::XS::CQTokSetInfl')) {
    return DDC::XS::CQFHasFieldSet->new($field, $q->getValues, $q->getNegated);
  } elsif (UNIVERSAL::isa($q, 'DDC::XS::CQTokRegex')) {
    return DDC::XS::CQFHasFieldRegex->new($field, $q->getValue, $q->getNegated);
  } elsif (UNIVERSAL::isa($q, 'DDC::XS::CQTokPrefix')) {
    return DDC::XS::CQFHasFieldPrefix->new($field, $q->getValue, $q->getNegated);
  } elsif (UNIVERSAL::isa($q, 'DDC::XS::CQTokSuffix')) {
    return DDC::XS::CQFHasFieldSuffix->new($field, $q->getValue, $q->getNegated);
  } elsif (UNIVERSAL::isa($q, 'DDC::XS::CQTokInfix')) {
    return DDC::XS::CQFHasFieldInfix->new($field, $q->getValue, $q->getNegated);
  } else {
    $coldb->logconfess("can't handle metadata restriction of type ", ref($q), " in $logas request: \`", $q->toString, "'");
  }
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
  if (!ref($req) && $req =~ m{^\s*(?:\#by)?\s*\[(.*)\]\s*$}) {
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
    my ($filter);
    foreach (@$gbreq) {
      push(@$gbexprs, $coldb->attrCountBy($_->[0], 2));
      if ($_->[0] =~ /^doc\./) {
	##-- document attribute ("doc.ATTR" convention)
	push(@$gbfilters, $filter) if (defined($filter=$coldb->query2filter($_->[0], $_->[1])));
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
##     score   => $func,          ##-- scoring function (f|fm|lf|lfm|mi|ld) : default="f"
##     kbest   => $k,             ##-- return only $k best collocates per date (slice) : default=-1:all
##     cutoff  => $cutoff,        ##-- minimum score
##     global  => $bool,          ##-- trim profiles globally (vs. locally for each date-slice?) (default=0)
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
  $opts{slice}   //= 1;
  $opts{groupby} ||= join(',', map {quotemeta($_)} @{$coldb->attrs});
  $opts{score}   //= 'f';
  $opts{eps}     //= 0;
  $opts{kbest}   //= -1;
  $opts{cutoff}  //= '';
  $opts{global}  //= 0;
  $opts{strings} //= 1;
  $opts{fill}    //= 0;

  ##-- debug
  $coldb->vlog($coldb->{logRequest},
	       "profile("
	       .join(', ',
		     map {"$_->[0]='".quotemeta($_->[1]//'')."'"}
		     ([rel=>$rel],
		      [query=>$opts{query}],
		      [groupby=>UNIVERSAL::isa($opts{groupby},'ARRAY') ? join(',', @{$opts{groupby}}) : $opts{groupby}],
		      (map {[$_=>$opts{$_}]} qw(date slice score eps kbest cutoff global)),
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
##     score   => $func,          ##-- scoring function (f|fm|lf|lfm|mi|ld) : default="f"
##     kbest   => $k,             ##-- return only $k best collocates per date (slice) : default=-1:all
##     cutoff  => $cutoff,        ##-- minimum score (UNUSED for comparison profiles)
##     global  => $bool,          ##-- trim profiles globally (vs. locally for each date-slice?) (default=0)
##     diff    => $diff,          ##-- low-level score-diff operation (diff|adiff|sum|min|max|avg|havg); default='adiff'
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
  foreach my $attr (qw(date slice)) {
    $opts{"a$attr"} = ((map {defined($opts{"a$_"}) ? $opts{"a$_"} : qw()} @{$ATTR_RALIAS{$attr}}),
		       (map {defined($opts{$_})    ? $opts{$_}    : qw()} @{$ATTR_RALIAS{$attr}}),
		      )[0]//'';
    $opts{"b$attr"} = ((map {defined($opts{"b$_"}) ? $opts{"b$_"} : qw()} @{$ATTR_RALIAS{$attr}}),
		       (map {defined($opts{$_})    ? $opts{$_}    : qw()} @{$ATTR_RALIAS{$attr}}),
		      )[0]//'';
  }
  delete @opts{keys %ATTR_ALIAS};

  ##-- common defaults
  $opts{groupby} ||= join(',', map {quotemeta($_)} @{$coldb->attrs});
  $opts{score}   //= 'f';
  $opts{eps}     //= 0;
  $opts{kbest}   //= -1;
  $opts{cutoff}  //= '';
  $opts{global}  //= 0;
  $opts{diff}    //= 'adiff';
  $opts{strings} //= 1;

  ##-- debug
  $coldb->vlog($coldb->{logRequest},
	       "compare("
	       .join(', ',
		     map {"$_->[0]=".quotemeta($_->[1]//'')."'"}
		     ([rel=>$rel],
		      (map {["a$_"=>$opts{"a$_"}]} (qw(query date slice))),
		      (map {["b$_"=>$opts{"b$_"}]} (qw(query date slice))),
		      [groupby=>(UNIVERSAL::isa($opts{groupby},'ARRAY') ? join(',',@{$opts{groupby}}) : $opts{groupby})],
		      (map {[$_=>$opts{$_}]} qw(score eps kbest cutoff global diff)),
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
