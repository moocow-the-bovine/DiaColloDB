## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model (via DocClassify)

package DiaColloDB::Relation::Vsem;
use DiaColloDB::Relation;
use DiaColloDB::Relation::Vsem::Query;
use DiaColloDB::Utils qw(:fcntl :file :math :json);
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
##   ##-- guts: metadata
##   meta => \@attrs,         ##-- known metadata attributes
##   meta_e_${ATTR} => $enum, ##-- metadata-attribute enum
##   meta_v_${ATTR} => $pdl,  ##-- metadata-attribute pdl ($NC) : [$ci] => $val_id s.t. ATTR(docid($ci)) = $enum->i2s($val_id)
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
  return ('dcopts', 'meta', $obj->SUPER::headerKeys);
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
  foreach (qw(t2w w2t d2c c2d c2date)) {
    defined($vs->{$_}=$map->readPdlFile("$vsdir/$_.pdl", %ioopts))
      or $vs->logconfess("open(): failed to load piddle data from $vsdir/$_.pdl: $!");
  }

  ##-- load: metadata
  my %efopts = (flags=>$vs->{flags}); #, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}
  foreach my $mattr (@{$vs->{meta}}) {
    $vs->{"meta_e_$mattr"} = $DiaColloDB::ECLASS->new(base=>"$vsdir/meta_e_$mattr", %efopts)
      or $vs->logconfess("open(): failed to open metadata enum $vsdir/meta_e_$mattr: $!");
    defined($vs->{"meta_v_$mattr"} = $map->readPdlFile("$vsdir/meta_v_$mattr.pdl",%ioopts))
      or $vs->logconfess("open(): failed to load metadata values from $vsdir/meta_v_$mattr.pdl: $!");
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
  my $docdir = "$coldb->{dbdir}/docdata.d";
  my $base   = $vs->{base};
  $vs->logconfess("create(): no source document directory '$docdir'") if (!-d $docdir);
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

  ##-- initialize: logging
  my $logCreate = 'trace';
  my @docfiles  = glob("$docdir/*.json");
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

  ##-- create: aux: t2w,w2t: term-translation pdls
  my %ioopts = %{$vs->{dcio}//{}};
  $vs->vlog($logCreate, "create(): creating term-translation piddles");
  my ($tmp);
  my $t2w  = $vs->{t2w} = pdl(long, $map->{tenum}{id2sym});            ##-- pdl($NW_dc) : [$wi_map] => $wi_coldb
  (my $w2t = $vs->{w2t} = zeroes(long, $coldb->{wenum}->size)) .= -1;
  ($tmp=$w2t->index($t2w)) .= $t2w->xvals;

  ##-- create: aux: d2c: [$di] => $ci
  $vs->vlog($logCreate, "create(): creating doc<->category translation piddles");
  my $ND  = $map->{denum}->size();
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
  my %efopts = (flags=>$vs->{flags}, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my ($menum);
  foreach $mattr (@{$vs->{meta}}) {
    $vs->vlog($logCreate, "create(): creating metadata map for attribute '$mattr'");
    $mdata = $meta{$mattr};
    $menum = $vs->{"meta_e_$mattr"} = $DiaColloDB::ECLASS->new(%efopts);
    $mvals = $vs->{"meta_v_$mattr"} = $mdata->{vals};
    $menum->fromHash($mdata->{s2i})
      or $vs->logconfess("create(): failed to create metadata enum for attribute '$mattr': $!");
    $menum->save("$vsdir/meta_e_$mattr")
      or $vs->logconfess("create(): failed to save metadata enum $vsdir/meta_e_$mattr: $!");
    $map->writePdlFile($mvals, "$vsdir/meta_v_${mattr}.pdl", %ioopts)
      or $vs->logconfess("create(): failed to save metadata value-piddle $vsdir/meta_v_$mattr: $!");
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
  foreach (qw(t2w w2t d2c c2d c2date)) {
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

  ##-- parse query
  my ($gbexprs,$gbrestr,$gbfilters) = $coldb->parseGroupBy($opts{groupby}, %opts); ##-- for restrictions
  my $groupby= $coldb->groupby($opts{groupby}, xenum=>$coldb->{wenum}, relax=>1);  ##-- TODO: make "real" groupby-clause here
  my $q = $coldb->parseQuery($opts{query}, logas=>'query', default=>'', ddcmode=>1);
  my ($qo);
  $q->setOptions($qo=DDC::XS::CQueryOptions->new) if (!defined($qo=$q->getOptions));
  $qo->setFilters([@{$qo->getFilters}, @$gbfilters]) if (@$gbfilters);

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
  } else {
    $vs->logconfess("no index term(s) found for user query \`$opts{query}'") if (defined($ti) && !$ti->nelem);
    $vs->logconfess("no index document(s) found for user query \`$opts{query}'") if (defined($ci) && !$ci->nelem);
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

  ##-- TEST: map to & extract k-best terms
  $vs->vlog($coldb->{logProfile}, "profile(): TEST: getting k-nearest neighbors");
  my $dist = $map->qdistance($qvec, $map->{svd}{v});
  my $sim  = (2-$dist);
  $sim->inplace->divide(2,$sim,0);
  my $simi  = $sim->qsorti->slice("-1:0");
  ##
  ##-- TODO: aggregate based on groupby request; maybe query {xcm} instead
  ##
  my $best_ti = $simi->slice("0:".($opts{kbest} < $simi->nelem ? ($opts{kbest}-1) : "-1"));
  my $best_wi = $vs->{t2w}->index($best_ti);
  my $best_sim = $sim->index($best_ti);

  ##-- TEST: construct profile [TODO: do we need to simulate f1, f12, etc?  do we need a special profile subclass?]
  $vs->vlog($coldb->{logProfile}, "profile(): TEST: constructing output profile");
  my ($gi);
  my $x2g = $groupby->{x2g};
  %{$prf->{vsim}} = map {
    $gi = $best_wi->at($_);
    $gi = $x2g->($gi) if (defined($x2g));
    ($gi => $best_sim->at($_))
  } (0..($best_wi->nelem-1));

  ##-- TODO: construct qinfo

  ##-- TEST: stringify
  $vs->vlog($logProfile, "profile(): stringify");
  my $mp = DiaColloDB::Profile::Multi->new(profiles=>[$prf], titles=>$groupby->{titles}, qinfo=>'TODO');
  $mp->stringify($groupby->{g2s}) if ($opts{strings});


  return $mp;
}

##==============================================================================
## Profile: Utils: query parsing

## $wi_map = $vs->w2t($wi_coldb)
##   + maps an index-piddle $wi_coldb over $coldb->{wenum} to an index-piddle $wi_map over $vs->{dcmap}{wenum}
sub w2t {
  my ($vs,$wi) = @_;
  my $ti = $vs->{w2t}->index($wi);
  return $ti->where($ti>=0);
}

## $wi_coldb = $vs->t2w($ti_map)
##   + maps an index-piddle $wi_map over $vs->{dcmap}{wenum} to an index-piddle $wi_coldb over $coldb->{wenum}
sub t2w {
  my ($vs,$ti) = @_;
  return $vs->{t2w}->index($ti);
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
