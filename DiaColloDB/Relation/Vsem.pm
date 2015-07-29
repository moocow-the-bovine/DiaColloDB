## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model (via DocClassify)

package DiaColloDB::Relation::Vsem;
use DiaColloDB::Relation;
use DiaColloDB::Relation::Vsem::Query;
use DiaColloDB::Utils qw(:fcntl :file :math :json :list :pdl);
use DocClassify;
use DocClassify::Mapper::Train;
use File::Path qw(make_path remove_tree);
use PDL;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Relation);

##==============================================================================
## Constructors etc.

## $vs = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##   ##-- user options
##   base   => $basename,   ##-- relation basename
##   flags  => $flags,      ##-- i/o flags (default: 'r')
##   dcopts => \%dcopts,    ##-- options for DocClassify::Mapper->new()
##   dcio   => \%dcio,      ##-- options for DocClassify::Mapper->saveDir()
##   mgood  => $regex,      ##-- positive filter regex for metadata attributes
##   mbad   => $regex,      ##-- negative filter regex for metadata attributes
##   ##
##   ##-- guts: aux term-tuples ($NA:number of term-attributes, $NT:number of term-tuples)
##   attrs  => \@attrs,       ##-- known term attributes
##   tvals  => $tvals,        ##-- pdl($NA,$NT) : [$apos,$ti] => $avali_at_term_ti
##   tsorti => $tsorti,       ##-- pdl($NT,$NA) : [,($apos)]  => $tvals->slice("($apos),")->qsorti
##   tpos   => \%a2pos,       ##-- term-attribute positions: $apos=$a2pos{$aname}
##   ##
##   ##-- guts: aux: metadata ($NM:number of metas-attributes, $NC:number of cats (source files))
##   meta => \@mattrs         ##-- known metadata attributes
##   meta_e_${ATTR} => $enum, ##-- metadata-attribute enum
##   mvals => $mvals,         ##-- pdl($NM,$NC) : [$mpos,$ci] => $mvali_at_ci
##   msorti => $msorti,       ##-- pdl($NC,$NM) : [,($mpos)]  => $mvals->slice("($mpos),")->qsorti
##   mpos  => \%m2pos,        ##-- meta-attribute positions: $mpos=$m2pos{$mattr}
##   ##
##   ##-- guts: mapper
##   dcmap  => $dcmap,      ##-- underlying DocClassify::Mapper object
##   #...
##   )
sub new {
  my $that = shift;
  my $vs   = $that->SUPER::new(
			       flags => 'r',
			       mgood => $DiaColloDB::VSMGOOD_DEFAULT,
			       mbad  => $DiaColloDB::VSMBAD_DEFAULT,
			       dcopts => {}, ##-- inherited from $coldb->{vsopts}
			       dcio   => {verboseIO=>0,saveSvdUS=>1,mmap=>1}, ##-- I/O opts for DocClassify
			       meta  => [],
			       attrs => [],
			       dcmap => undef,
			       @_
			      );
  return $vs->open() if ($vs->{base});
  return $vs;
}

##==============================================================================
## API: disk usage

## @files = $obj->diskFiles()
##  + returns disk storage files, used by du() and timestamp()
sub diskFiles {
  return ("$_[0]{base}.hdr", glob("$_[0]{base}_*"));
}

##==============================================================================
## Persistent API: header

## @keys = $obj->headerKeys()
##  + keys to save as header; default implementation returns all keys of all non-references
sub headerKeys {
  my $obj = shift;
  return (qw(dcopts meta attrs), $obj->SUPER::headerKeys);
}


##==============================================================================
## API: open/close

## $vs_or_undef = $vs->open($base)
## $vs_or_undef = $vs->open($base,$flags)
## $vs_or_undef = $vs->open()
sub open {
  my ($vs,$base,$flags) = @_;
  $base  //= $vs->{base};
  $flags //= $vs->{flags};
  $vs->close() if ($vs->opened);
  $vs->{base}  = $base;
  $vs->{flags} = $flags = fcflags($flags);

  if (fcread($flags) && !fctrunc($flags)) {
    $vs->loadHeader()
      or $vs->logconess("failed to load header from '$vs->{base}.hdr': $!");
  }

  ##-- open maybe create directory
  my $vsdir = "$vs->{base}.d";
  if (!-d $vsdir) {
    $vs->logconfess("open(): no such directory '$vsdir'") if (!fccreat($flags));
    make_path($vsdir)
      or $vs->logconfess("open(): could not create relation directory '$vsdir': $!");
  }

  ##-- load: mapper
  my %ioopts = %{$vs->{dcio}//{}};
  my $map = $vs->{dcmap} = DocClassify::Mapper->loadDir("$vsdir/map.d", %ioopts)
    or $vs->logconfess("open(): failed to load mapper data from $vsdir/map.d: $!");

  ##-- load: aux data: piddles
  foreach (qw(tvals tsorti mvals msorti d2c c2d c2date)) {
    defined($vs->{$_}=$map->readPdlFile("$vsdir/$_.pdl", %ioopts))
      or $vs->logconfess("open(): failed to load piddle data from $vsdir/$_.pdl: $!");
  }

  ##-- load: metadata: enums
  my %efopts = (flags=>$vs->{flags}); #, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}
  foreach my $mattr (@{$vs->{meta}}) {
    $vs->{"meta_e_$mattr"} = $DiaColloDB::ECLASS->new(base=>"$vsdir/meta_e_$mattr", %efopts)
      or $vs->logconfess("open(): failed to open metadata enum $vsdir/meta_e_$mattr: $!");
  }

  return $vs;
}

## $vs_or_undef = $vs->close()
sub close {
  my $vs = shift;
  if ($vs->opened && fcwrite($vs->{flags})) {
    $vs->saveHeader() or return undef;
#   $vs->{dcmap}->saveDir("$vs->{base}_map.d", %{$vs->{dcio}//{}})
#     or $vs->logconfess("close(): failed to save mapper data to $vs->{base}_map.d: $!");
  }
  undef $vs->{base};
  undef $vs->{dcmap};
  return $vs;
}

## $bool = $obj->opened()
sub opened {
  my $vs = shift;
  return defined($vs->{dcmap}) && $vs->{dcmap}->compiled();
}

##==============================================================================
## Relation API: create

## $vs = $CLASS_OR_OBJECT->create($coldb,$tokdat_file,%opts)
##  + populates current database from $tokdat_file,
##    a tt-style text file containing 1 token-id perl line with optional blank lines
##  + %opts: clobber %$vs, also:
##    (
##     size=>$size,  ##-- set initial size
##    )
sub create {
  my ($vs,$coldb,$datfile,%opts) = @_;

  ##-- create/clobber
  $vs = $vs->new() if (!ref($vs));
  @$vs{keys %opts} = values %opts;
  $vs->{$_} = $coldb->{"vs$_"} foreach (grep {exists $coldb->{"vs$_"}} qw(mgood mbad));

  ##-- sanity check(s)
  my $doctmpd = "$coldb->{dbdir}/doctmp.d";
  my $doctmpf = "$coldb->{dbdir}/doctmp.files";
  my $base   = $vs->{base};
  $vs->logconfess("create(): no source document directory '$doctmpd'") if (!-d $doctmpd);
  $vs->logconfess("create(): no source document file-list '$doctmpf'") if (!-f $doctmpf);
  $vs->logconfess("create(): no 'base' key defined") if (!$base);

  ##-- initialize: output directory
  my $vsdir = "$base.d";
  $vsdir =~ s{/$}{};
  !-d $vsdir
    or remove_tree($vsdir)
      or $vs->logconfess("create(): could not remove stale $vsdir: $!");
  make_path($vsdir)
    or $vs->logconfess("create(): could not create Vsem directory $vsdir: $!");

  ##-- create dcmap
  $vs->{dcopts} //= {};
  @{$vs->{dcopts}}{keys %{$coldb->{vsopts}//{}}} = values %{$coldb->{vsopts}//{}};
  my $map = $vs->{dcmap} = DocClassify::Mapper->new( %{$vs->{dcopts}} )
    or $vs->logconfess("create(): failed to create DocClassify::Mapper object");
  my $mverbose = $map->{verbose};
  $map->{verbose} = min2($mverbose,1);

  ##-- initialize: file-list
  CORE::open(my $doctmpfh, "<$doctmpf")
      or $vs->logconfess("create(): could not open source document file-list $doctmpf: $!");
  my @docfiles = map {chomp; $_} <$doctmpfh>;
  CORE::close($doctmpfh);

  ##-- initialize: logging
  my $logCreate = 'trace';
  my $nfiles    = scalar(@docfiles);
  my $logFileN  = $coldb->{logCorpusFileN} // max2(1,int($nfiles/10));

  ##-- initialize: metadata
  my %meta = qw(); ##-- ( $meta_attr => {n=>$nkeys, s2i=>\%s2i, vals=>$pdl}, ... )
  my $mgood = $vs->{mgood} ? qr{$vs->{mgood}} : undef;
  my $mbad  = $vs->{mbad}  ? qr{$vs->{mbad}}  : undef;

  ##-- simulate $map->trainCorpus()
  $vs->vlog($logCreate, "create(): simulating trainCorpus() [N=$nfiles]");
  my $NC     = $nfiles;
  my $c2date = $vs->{c2date} = zeroes(ushort, $NC);
  my $json   = DiaColloDB::Utils->jsonxs();
  my ($filei,$docfile,$doc,$doclabel,$docid);
  my ($mattr,$mval,$mdata,$mvali,$mvals);
  my ($sig,$sigi,$dcdoc);
  foreach $docfile (@docfiles) {
    $vs->vlog($coldb->{logCorpusFile}, sprintf("create(): processing signatures [%3d%%]: %s", 100*($filei-1)/$nfiles, $docfile))
      if ($logFileN && ($filei++ % $logFileN)==0);

    $doc      = DiaColloDB::Utils::loadJsonFile($docfile,json=>$json);
    $doclabel = $doc->{meta}{basename} // $doc->{meta}{file_} // $doc->{label} // $docfile;
    $docid    = $doc->{id} // ++$docid;
    $sigi     = 0;
    $c2date->set($docid,$doc->{date});

    ##-- parse metadata
    while (($mattr,$mval) = each %{$doc->{meta}//{}}) {
      next if ((defined($mgood) && $mattr !~ $mgood) || (defined($mbad) && $mattr =~ $mbad));
      $mdata = $meta{$mattr} = {n=>1, s2i=>{''=>0}, vals=>zeroes(long,$NC)} if (!defined($mdata=$meta{$mattr}));
      $mvali = ($mdata->{s2i}{$mval} //= $mdata->{n}++);
      $mdata->{vals}->set($docid,$mvali);
    }

    ##-- create temporary DocClassify::Document objects for each embedded signature
    foreach $sig (@{$doc->{sigs}}) {
      $dcdoc = bless({
		      label=>$doclabel."#".($sigi++),
		      cats=>[{id=>$docid,deg=>0,name=>$doclabel}],
		      sig =>bless({tf=>$sig},'DocClassify::Signature'),
		     }, 'DocClassify::Document');
      $map->trainDocument($dcdoc);
    }
  }

  ##-- compile mapper
  $vs->vlog($logCreate, "create(): compiling ", ref($map), " object");
  $map->compile()
    or $vs->logconfess("create(): failed to compile ", ref($map), " object");

  ##-- create: aux: common variables
  my ($tmp);
  my %ioopts = %{$vs->{dcio}//{}};

  ##-- create: aux: tenum
  my $NT = $map->{tenum}->size;
  my $NA = scalar(@{$coldb->{attrs}});
  $vs->vlog($logCreate, "create(): creating term-attribute pseudo-enum (NA=$NA x NT=$NT)");
  my $pack_t = $coldb->{pack_w};
  my $ti2s   = $map->{tenum}{id2sym};
  my $tvals  = $vs->{tvals} = zeroes(long, $NA,$NT); ##-- [$apos,$ti] => $avali_at_term_ti
  foreach (0..$#$ti2s) {
    ($tmp=$tvals->slice(",($_)")) .= [unpack($pack_t,$ti2s->[$_])] if (defined($_));
  }
  ##
  #$vs->vlog($logCreate, "create(): creating term-attribute sort-indices (NA=$NA x NT=$NT)");
  my $tsorti = $vs->{tsorti} = zeroes(long, $NT,$NA); ##-- [,($apos)] => $tvals->slice("($apos),")->qsorti
  foreach (0..($NA-1)) {
    $tvals->slice("($_),")->qsorti($tsorti->slice(",($_)"));
  }
  ##
  $vs->{attrs} = $coldb->{attrs}; ##-- save local copy of attributes

  ##-- create: aux: d2c: [$di] => $ci
  my $ND  = $map->{denum}->size();
  $vs->vlog($logCreate, "create(): creating doc<->category translation piddles (ND=$ND, NC=$NC)");
  my $d2c = $vs->{d2c} = zeroes(long, $ND);
  ($tmp=$d2c->index($map->{dcm}->_whichND->slice("(0),"))) .= $map->{dcm}->_whichND->slice("(1),");

  ##-- create: aux: c2d (2,$NC): [0,$ci] => $di_off, [1,$ci] => $di_len
  my ($c2d_n,$c2d_vals) = $d2c->rle();
  my $c2d_which = $c2d_n->which;
  $c2d_n    = $c2d_n->index($c2d_which);
  $c2d_vals = $c2d_vals->index($c2d_which);
  my $c2d_off = $c2d_n->append(0)->rotate(1)->slice("0:-2")->cumusumover;
  my $c2d     = $vs->{c2d} = zeroes(long,2,$NC);
  ($tmp=$c2d->slice("(0),")->index($c2d_vals)) .= $c2d_off;
  ($tmp=$c2d->slice("(1),")->index($c2d_vals)) .= $c2d_n;
  ##--
  ##-- create: aux: c2d : CCS ($ND,$NC): [$ci,$di] => 1 iff $di \in $ci
  #my $c2d = $vs->{c2d} = PDL::CCS::Nd->newFromWhich($d2c->cat($d2c->xvals)->xchg(0,1), ones(byte,$ND));


  ##-- create: aux: c2date: [$ci] => $date -- NOTHING TO DO HERE: populated in training loop above

  ##-- create: aux: metadata attributes
  @{$vs->{meta}} = sort keys %meta;
  my %efopts     = (flags=>$vs->{flags}, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my $NM         = scalar @{$vs->{meta}};
  $mvals         = $vs->{mvals} = zeroes(long,$NM,$NC); ##-- [$mpos,$ci] => $mvali_at_ci
  my ($menum);
  foreach (0..($NM-1)) {
    $vs->vlog($logCreate, "create(): creating metadata enum for attribute '$vs->{meta}[$_]'");
    $mattr = $vs->{meta}[$_];
    $mdata = $meta{$mattr};
    $menum = $vs->{"meta_e_$mattr"} = $DiaColloDB::ECLASS->new(%efopts);
    ($tmp=$mvals->slice("($_),")) .= $mdata->{vals};
    $menum->fromHash($mdata->{s2i})
      or $vs->logconfess("create(): failed to create metadata enum for attribute '$mattr': $!");
    $menum->save("$vsdir/meta_e_$mattr")
      or $vs->logconfess("create(): failed to save metadata enum $vsdir/meta_e_$mattr: $!");
  }
  ##
  $vs->vlog($logCreate, "create(): creating metadata sort-indices (NM=$NM x NC=$NC)");
  my $msorti = $vs->{msorti} = zeroes(long, $NC,$NM); ##-- [,($mi)] => $mvals->slice("($mi),")->qsorti
  foreach (0..($NM-1)) {
    $mvals->slice("($_),")->qsorti($msorti->slice(",($_)"));
  }

  ##-- tweak mapper piddles
  $vs->vlog($logCreate, "create(): tweaking mapper piddles");
  $map->{dcm}->inplace->convert(byte) if (defined($map->{dcm}));

  ##-- tweak mapper enums
  $vs->vlog($logCreate, "create(): creating dummy mapper identity-enums");
  $map->{$_} = $vs->idEnum($map->{$_}) foreach (qw(gcenum lcenum tenum denum));

  ##-- save
  $vs->vlog($logCreate, "create(): saving to $base*");

  ##-- save: header
  $vs->saveHeader()
    or $vs->logconfess("create(): failed to save header data: $!");

  ##-- save: mapper
  $map->saveDir("$vsdir/map.d", %ioopts)
    or $vs->logconfess("create(): failed to save mapper data to ${vsdir}/map.d: $!");

  ##-- save: aux data: piddles
  foreach (qw(tvals tsorti mvals msorti d2c c2d c2date)) {
    $map->writePdlFile($vs->{$_}, "$vsdir/$_.pdl", %ioopts)
      or $vs->logconfss("create(): failed to save auxilliary piddle $vsdir/$_.pdl: $!");
  }


  ##-- return
  return $vs;
}

##----------------------------------------------------------------------
## create: utils

##--------------------------------------------------------------
## $idEnum = PACKAGE->idEnum($mudlEnum)
##  + creates a shadow identity MUDL::Enum for $mudlEnum
sub idEnum {
  shift if (UNIVERSAL::isa($_[0],__PACKAGE__));
  my $menum = shift;
  my ($i2s,$s2i) = DiaColloDB::EnumFile::Identity->tiepair(size=>$menum->size,dirty=>1);
  return MUDL::Enum->new(id2sym=>$i2s,sym2id=>$s2i);
}

##==============================================================================
## Relation API: union

## $vs = CLASS_OR_OBJECT->union($coldb, \@pairs, %opts)
##  + merge multiple co-frequency indices into new object
##  + @pairs : array of pairs ([$vs,\@xi2u],...)
##    of unigram-objects $vs and tuple-id maps \@xi2u for $vs
##  + %opts: clobber %$vs
##  + implicitly flushes the new index
sub union {
  my ($vs,$coldb,$pairs,%opts) = @_;

  ##-- create/clobber
  $vs = $vs->new() if (!ref($vs));
  @$vs{keys %opts} = values %opts;

  $vs->logconfess("union(): not yet implemented");
  return $vs;
}

##==============================================================================
## Relation API: profile

## $mprf = $rel->profile($coldb, %opts)
## + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
## + %opts: as for DiaColloDB::Relation::profile()
sub profile {
  my ($vs,$coldb,%opts) = @_;

  ##-- common variables
  my $logProfile = $coldb->{logProfile};
  my $map = $vs->{dcmap};

  ##-- sanity checks / fixes
  $vs->{attrs} = $coldb->{attrs} if (!@{$vs->{attrs}//[]});

  ##-- parse query
  my $groupby = $vs->groupby($coldb, $opts{groupby}, relax=>0);
  #my $groupby= $coldb->groupby($opts{groupby}, relax=>1);  ##-- TODO: make "real" groupby-clause here
  #my ($gbexprs,$gbrestr,$gbfilters) = $coldb->parseGroupBy($opts{groupby}, %opts); ##-- DDC for restrictions
  ##
  my $q = $coldb->parseQuery($opts{query}, logas=>'query', default=>'', ddcmode=>1);
  my ($qo);
  $q->setOptions($qo=DDC::XS::CQueryOptions->new) if (!defined($qo=$q->getOptions));
  #$qo->setFilters([@{$qo->getFilters}, @$gbfilters]) if (@$gbfilters);

  ##-- evaluate query components
  my %vqopts = (%opts,coldb=>$coldb,vsem=>$vs);
  my $vq = DiaColloDB::Relation::Vsem::Query->new($q)->evaluate(%vqopts);

  ##-- parse and apply date-request
  my ($dfilter,$dslo,$dshi,$dlo,$dhi) = $coldb->parseDateRequest(@opts{qw(date slice fill)},1);
  $vs->vlog($coldb->{logProfile}, "profile(): query vector: dates");
  $vq->restrictByDate($dlo,$dhi,%vqopts);

  ##-- construct query: common
  my ($qvec); ##-- query-vector: ($svdR,1) : [$ri] => $x
  my ($ti,$ci) = @$vq{qw(ti ci)};
  my $prf = DiaColloDB::Profile->new(N=>0,score=>'vsim',vsim=>{});

  ##-- construct query: sanity checks: null vectors
  if ($opts{fill}) {
    return $prf if ((defined($ti) && !$ti->nelem) || (defined($ci) && $ci->nelem));
  } elsif (defined($ti) && !$ti->nelem) {
    $vs->logconfess($coldb->{error}="no index term(s) matched user query \`$opts{query}'");
  } elsif (defined($ci) && !$ci->nelem) {
    $vs->logconfess($coldb->{error}="no index document(s) matched user query \`$opts{query}'");
  }

  ##-- construct query: dispatch
  if (defined($ti) && defined($ci)) {
    ##-- both term- and document-conditions
    $vs->vlog($coldb->{logProfile}, "profile(): query vector: xsubset");
    my $q_c2d     = $vs->{c2d}->dice_axis(1,$ci);
    my $di        = $q_c2d->slice("(1),")->rldseq($q_c2d->slice("(0),"))->qsort;
    my $q_tdm     = $map->{tdm}->xsubset2d($ti,$di);
    return $prf if ($q_tdm->allmissing); ##-- empty subset
    $q_tdm = $q_tdm->sumover->dummy(0,1)->make_physically_indexed;
    #$vs->vlog($coldb->{logProfile}, "profile(): query vector: xsubset: svdapply");
    $qvec = $map->{svd}->apply1($q_tdm)->xchg(0,1);
  }
  elsif (defined($ti)) {
    ##-- term-conditions only: slice from $svd->{v}
    $vs->vlog($coldb->{logProfile}, "profile(): query vector: terms");
    my $xtm = $map->{svd}{v};
    $qvec   = $xtm->dice_axis(1,$ti);
    if ($ti->nelem > 1) {
      $qvec = $qvec->xchg(0,1)->sumover->dummy(1,1) / $ti->nelem;
    }
  }
  elsif (defined($ci)) {
    ##-- doc-conditions only: slice from $map->{xcm}
    $vs->vlog($coldb->{logProfile}, "profile(): query vector: cats");
    my $xcm = $map->{xcm};
    $qvec   = $xcm->dice_axis(1,$ci);
    if ($ci->nelem > 1) {
      $qvec = $qvec->xchg(0,1)->sumover->dummy(1,1) / $ci->nelem;
    }
  }

  ##-- map to & extract k-best terms (TODO: maybe map to cats by querying {xcm} instead of {xtm}~{svd}{v} ?)
  $vs->vlog($coldb->{logProfile}, "profile(): extracting k-nearest neighbors (terms)");
  my $dist = $map->qdistance($qvec,
			     (defined($groupby->{ghaving})
			      ? $map->{svd}{v}->dice_axis(1,$groupby->{ghaving})
			      : $map->{svd}{v}));

  ##-- groupby: aggregate
  my ($g_keys,$g_dist) = $groupby->{gaggr}->($dist);

  ##-- groupby: k-best items
  my $g_sorti = $g_dist->qsorti;
  my $g_besti = $g_sorti->slice("0:".($opts{kbest} < $g_sorti->nelem ? ($opts{kbest}-1) : "-1"));

  ##-- convert distance to similarity (simple linear method; range=[-1:1])
  my $g_sim = (1-$g_dist);

  ##-- construct profile [TODO: do we need to simulate f1, f12, etc?  do we need a special profile subclass?]
  $vs->vlog($coldb->{logProfile}, "profile(): constructing output profile");
  %{$prf->{vsim}} = map { ($g_keys->at($_)=>$g_sim->at($_)) } $g_besti->list;

  ##-- TODO: construct qinfo

  ##-- TEST: stringify
  $vs->vlog($logProfile, "profile(): stringify");
  my $mp = DiaColloDB::Profile::Multi->new(profiles=>[$prf], titles=>$groupby->{titles}, qinfo=>'TODO');
  $mp->stringify($groupby->{g2s}) if ($opts{strings});

  return $mp;
}

##==============================================================================
## Profile: Utils: domain sizes

## $NT = $vs->nTerms()
##  + gets number of terms
sub nTerms {
  return $_[0]{dcmap}{tenum}->size;
}

## $ND = $vs->nDocs()
##  + returns number of documents (breaks)
BEGIN { *nBreaks = \&nDocs; }
sub nDocs {
  return $_[0]{dcmap}{denum}->size;
}

## $NC = $vs->nFiles()
##  + returns number of categories (original source files)
BEGIN { *nCategories = *nCats = \&nFiles; }
sub nFiles {
  return $_[0]{dcmap}{lcenum}->size;
}

##==============================================================================
## Profile: Utils: attribute positioning

## \%tpos = $vs->tpos()
##  $tpos = $vs->tpos($tattr)
##  + get or build term-attribute position lookup hash
sub tpos {
  $_[0]{tpos} //= { (map {($_[0]{attrs}[$_]=>$_)} (0..$#{$_[0]{attrs}})) };
  return @_>1 ? $_[0]{tpos}{$_[1]} : $_[0]{tpos};
}

## \%mpos = $vs->mpos()
## $mpos  = $vs->mpos($mattr)
##  + get or build meta-attribute position lookup hash
sub mpos {
  $_[0]{mpos} //= { (map {($_[0]{meta}[$_]=>$_)} (0..$#{$_[0]{meta}})) };
  return @_>1 ? $_[0]{mpos}{$_[1]} : $_[0]{mpos};
}

##==============================================================================
## Profile: Utils: query parsing & evaluation

## $idPdl = $vs->idpdl($idPdl)
## $idPdl = $vs->idpdl(\@ids)
## $idPdl = $vs->idpdl($ids)
sub idpdl {
  shift if (UNIVERSAL::isa($_[0],__PACKAGE__));
  my $ids = shift;
  return null->long   if (!defined($ids));
  $ids = [$ids] if (!ref($ids));
  $ids = pdl(long,$ids) if (!UNIVERSAL::isa($ids,'PDL'));
  return $ids;
}

## $tupleIds = $vs->tupleIds($attrType, $attrName, $valIdsPdl)
## $tupleIds = $vs->tupleIds($attrType, $attrName, \@valIds)
## $tupleIds = $vs->tupleIds($attrType, $attrName, $valId)
sub tupleIds {
  my ($vs,$typ,$attr,$valids) = @_;
  $valids = $valids=$vs->idpdl($valids);

  ##-- check for empty value-set
  if ($valids->nelem == 0) {
    return null->long;
  }

  ##-- non-empty: get base data
  my $apos = $vs->can("${typ}pos")->($vs,$attr);
  my $vals = $vs->{"${typ}vals"}->slice("($apos),");

  ##-- check for singleton value-set & maybe do simple linear search
  if ($valids->nelem == 1) {
    return ($vals==$valids)->which;
  }

  ##-- nontrivial value-set: do vsearch lookup
  my $sorti   = $vs->{"${typ}sorti"}->slice(",($apos)");
  my $vals_qs = $vals->index($sorti);
  my $i0      = $valids->vsearch($vals_qs);
  my $i0_mask = ($vals_qs->index($i0) == $valids);
  $i0         = $i0->where($i0_mask);
  my $ilen    = ($valids->where($i0_mask)+1)->vsearch($vals_qs);
  $ilen      -= $i0;
  $ilen->slice("-1")->lclip(1) if ($ilen->nelem); ##-- hack for bogus 0-length at final element
  my $iseq    = $ilen->rldseq($i0);
  return $sorti->index($iseq)->qsort;
}

## $ti = $vs->termIds($tattrName, $valIdsPDL)
## $ti = $vs->termIds($tattrName, \@valIds)
## $ti = $vs->termIds($tattrName, $valId)
sub termIds {
  return $_[0]->tupleIds('t',@_[1..$#_]);
}

## $ci = $vs->catIds($mattrName, $valIdsPDL)
## $ci = $vs->catIds($mattrName, \@valIds)
## $ci = $vs->catIds($mattrName, $valId)
sub catIds {
  return $_[0]->tupleIds('m',@_[1..$#_]);
}

## $bool = $vs->hasMeta($attr)
##  + returns true iff $vs supports metadata attribute $attr
sub hasMeta {
  return defined($_[0]->mpos($_[1]));
}

## $enum_or_undef = metaEnum($mattr)
##  + returns metadata attribute enum for $attr
sub metaEnum {
  my ($vs,$attr) = @_;
  return undef if (!$vs->hasMeta($attr));
  return $vs->{"meta_e_$attr"};
}

##~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
## \%groupby = $vs->groupby($coldb, $groupby_request, %opts)
## \%groupby = $vs->groupby($coldb, \%groupby,        %opts)
##  + modified version of DiaColloDB::groupby() suitable for pdl-ized Vsem relation
##  + $grouby_request : see DiaColloDB::parseRequest()
##  + returns a HASH-ref:
##    (
##     ##-- OLD: equivalent to DiaColloDB::groupby() return values
##     req => $request,    ##-- save request
##     areqs => \@areqs,   ##-- parsed attribute requests ([$attr,$ahaving],...)
##     attrs => \@attrs,   ##-- like $coldb->attrs($groupby_request), modulo "having" parts
##     titles => \@titles, ##-- like map {$coldb->attrTitle($_)} @attrs
##     ##
##     ##-- REMOVED: not constructed for Vsem::groupby()
##     #x2g => \&x2g,       ##-- group-id extraction code suitable for e.g. DiaColloDB::Relation::Cofreqs::profile(groupby=>\&x2g)
##     #g2s => \&g2s,       ##-- stringification object suitable for DiaColloDB::Profile::stringify() [CODE,enum, or undef]
##     ##
##     ##-- NEW: equivalent to DiaColloDB::groupby() return values
##     ghaving => $ghaving, ##-- pdl ($NHavingOk) : term indices $ti s.t. $ti matches groupby "having" requests
##     gaggr   => \&gaggr,  ##-- code: ($gkeys,$gdist) = gaggr($dist) : where $dist is diced to $ghaving on dim(1)
##     g2s     => \&g2s,    ##-- stringification object suitable for DiaColloDB::Profile::stringify() [CODE,enum, or undef]
##     ##
##     ##-- NEW: pdl utilties
##     #gv    => $gv,       ##-- pdl ($NG): [$gvi] => $gi : group-id enumeration
##     #gn    => $gn,       ##-- pdl ($NG): [$gvi] => $n  : number of terms in group
##    )
##  + %opts:
##     warn  => $level,    ##-- log-level for unknown attributes (default: 'warn')
##     relax => $bool,     ##-- allow unsupported attributes (default=0)
sub groupby {
  my ($vs,$coldb,$gbreq,%opts) = @_;
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

  ##-- get "having"-clause matches
  my $ghaving = undef;
  foreach (grep {$_->[1] && !UNIVERSAL::isa($_->[1],'DDC::XS::CQTokAny')} @$gbareqs) {
    my $avalids = $coldb->enumIds($coldb->{"$_->[0]enum"}, $_->[1], logLevel=>$coldb->{logProfile}, logPrefix=>"groupby(): fetch filter ids: $_->[0]");
    my $ahaving = $vs->termIds($_->[0], $avalids);
    $ghaving    = DiaColloDB::Utils::_intersect_p($ghaving,$ahaving);
  }
  $gb->{ghaving} = $ghaving;

  ##-- get pdl-ized group-aggregation and -stringification objects
  my ($g_keys); ##-- pdl ($NHavingOk): [$hvi] => $gi : term-ids for generic aggregation by $ghaving or raw term-id
  if (@{luniq($gbattrs)} == @{luniq($coldb->{attrs})}) {
    ##-- project all attributes: t2g: use native term-ids
    #$gb->{t2g} = sub { return $_[0]; };

    ##-- project all attribute: aggregate by term-identity (i.e. don't)
    $gb->{gaggr} = sub {
      return (defined($ghaving)
	      ? ($ghaving,$_[0])
	      : (sequence(long,$_[0]->nelem),$_[0]));
    };

    ##-- project all attributes: g2s: stringification
    my $tvals  = $vs->{tvals};
    my @tenums = map {$coldb->{"${_}enum"}} @{$vs->{attrs}};
    my @gbpos  = map {$vs->tpos($_)} @$gbattrs;
    $gb->{g2s} = sub {
      return join("\t", map {$tenums[$_]->i2s($tvals->at($_,$_[0]))//''} @gbpos);
    };
  }
  elsif (@$gbattrs == 1) {
    ##-- project single attribute: t2g: use native attribute-ids
    my $gpos   = $vs->tpos($gbattrs->[0]);
    #$gb->{t2g} = sub { return $vs->{tvals}->slice("($gpos),")->index($_[0]); };

    ##-- project single attribute: gaggr: aggregate by native attribute-ids
    $g_keys = $vs->{tvals}->slice("($gpos),");

    ##-- project single attribute: g2s: stringification
    my $gbenum = $coldb->{$gbattrs->[0]."enum"};
    $gb->{g2s} = sub { return $gbenum->i2s($_[0]); };
  }
  else {
    ##-- project multiple attributes: t2g: create local vector-enum
    my $gpos   = [map {$vs->tpos($_)} @$gbattrs];
    my $gvecs  = $vs->{tvals}->dice_axis(0,$gpos);
    my $gsorti = $gvecs->vv_qsortveci;
    my $gvids  = zeroes(long, $gvecs->dim(1));
    $gvecs->dice_axis(1,$gsorti)->enumvecg($gvids->index($gsorti));
    #$gb->{t2g} = sub { return $gvids->index($_[0]); };

    ##-- project multiple attributes: gaggr: aggregate by local vector-enum
    $g_keys = $gvids;

    ##-- project multiple attributes: g2s: stringification
    my @tenums = map {$coldb->{"${_}enum"}} @{$vs->{attrs}};
    $gb->{g2s} = sub {
      return join("\t", map {$tenums[$gpos->[$_]]->i2s($gvecs->at($_,$_[0]))//''} (0..$#$gpos));
    };
  }

  ##-- aggregation: generic
  if (!defined($gb->{gaggr})) {
    $g_keys      = $g_keys->index($ghaving) if (defined($ghaving));
    my ($gv,$gn) = $g_keys->valcounts;
    my $g_vids   = $g_keys->vsearch($gv);
    $gb->{gaggr} = sub {
      my $dist = shift;
      my $g_vdist = zeroes($dist->type,$gv->nelem);
      $dist->indadd($g_vids, $g_vdist);
      $g_vdist /= $gn;
      return ($gv,$g_vdist);
    };
  }

  return $gb;
}


##==============================================================================
## Relation API: default: query info

## \%qinfo = $rel->qinfo($coldb, %opts)
##  + get query-info hash for profile administrivia (ddc hit links)
##  + %opts: as for profile(), additionally:
##    (
##     qreqs => \@qreqs,      ##-- as returned by $coldb->parseRequest($opts{query})
##     gbreq => \%groupby,    ##-- as returned by $coldb->groupby($opts{groupby})
##    )
##  + TODO

##==============================================================================
## Footer
1;

__END__
