## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model (via DocClassify)

package DiaColloDB::Relation::Vsem;
use DiaColloDB::Relation;
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
			       dcopts => {}, ##-- inherited from $coldb->{vsopts}
			       dcio   => {verboseIO=>0,mmap=>1}, ##-- I/O opts for DocClassify
			       flags => 'r',
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
  foreach (qw(map2w w2map d2c c2date)) {
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
  $map->{verbose} = 1;

  ##-- initialize: logging
  my $logCreate = $coldb->{logCreate};
  my @docfiles  = glob("$docdir/*.json");
  my $nfiles    = scalar(@docfiles);
  my $logFileN  = $coldb->{logCorpusFileN} // max2(1,int($nfiles/10));

  ##-- simulate $map->trainCorpus()
  $vs->vlog($logCreate, "create(): simulating trainCorpus() [N=$nfiles]");
  my $NC     = $nfiles;
  my $c2date = $vs->{c2date} = zeroes(ushort, $NC);
  my $json   = DiaColloDB::Utils->jsonxs();
  my ($filei,$docfile,$doc,$doclabel,$docid);
  my %meta = qw(); ##-- ( $meta_attr => {n=>$nkeys, s2i=>\%s2i, vals=>$pdl}, ... )
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
      next if ($mattr =~ /_$/);
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

  ##-- create: aux: map2w,w2map: term-translation pdls
  my %ioopts = %{$vs->{dcio}//{}};
  $vs->vlog($logCreate, "create(): creating term-translation piddles");
  my ($tmp);
  my $map2w  = $vs->{map2w} = pdl(long, $map->{tenum}{id2sym});    ##-- pdl($NW_dc) : [$wi_map] => $wi_coldb
  (my $w2map = $vs->{w2map} = zeroes(long, $coldb->{wenum}->size)) .= -1;
  ($tmp=$w2map->index($map2w)) .= $map2w->xvals;

  ##-- create: aux: d2c: [$di] => $ci
  $vs->vlog($logCreate, "create(): creating doc-category piddle");
  my $ND  = $map->{denum}->size();
  my $d2c = $vs->{d2c} = zeroes(long, $ND);
  ($tmp=$d2c->index($map->{dcm}->_whichND->slice("(0),"))) .= $map->{dcm}->_whichND->slice("(1),");

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
  foreach (qw(map2w w2map d2c c2date)) {
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
## Relation API: default: profiling

## $prf = $vs->subprofile(\@xids, %opts)
##  + get frequency profile for @xids (db must be opened)
##  + %opts:
##     groupby => \&gbsub,  ##-- key-extractor $key2_or_undef = $gbsub->($i2)
##  + TODO

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
