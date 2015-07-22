## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model (via DocClassify)

package DiaColloDB::Relation::Vsem;
use DiaColloDB::Relation;
use DiaColloDB::Utils qw(:fcntl :file :math :json);
use DocClassify;
use DocClassify::Mapper::Train;
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
##   ##-- guts
##   dcmap  => $dcmap,      ##-- underlying DocClassify::Mapper object
##   )
sub new {
  my $that = shift;
  my $vs   = $that->SUPER::new(
			       dcopts => {}, ##-- inherited from $coldb->{vsopts}
			       dcio   => {verboseIO=>0,mmap=>1}, ##-- I/O opts for DocClassify
			       flags => 'r',
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
  return ('dcopts', $obj->SUPER::headerKeys);
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

  $vs->{dcmap} = DocClassify::Mapper->loadDir("${base}_map.d", %{$vs->{dcio}//{}})
    or $vs->logconfess("open(): failed to load mapper data from ${base}_map.d: $!");

  #$vs->logconfess("open(): not yet implemented");

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
  $vs->logconfess("create(): no source document directory '$docdir'") if (!-d $docdir);
  $vs->logconfess("create(): no 'base' key defined") if (!$vs->{base});

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
  my $json = DiaColloDB::Utils->jsonxs();
  my ($filei,$docfile,$doc,$doclabel,$docid, $sig,$sigi,$dcdoc);
  foreach $docfile (@docfiles) {
    $vs->vlog($coldb->{logCorpusFile}, sprintf("create(): processing signatures [%3d%%]: %s", 100*($filei-1)/$nfiles, $docfile))
      if ($logFileN && ($filei++ % $logFileN)==0);

    $doc      = DiaColloDB::Utils::loadJsonFile($docfile,json=>$json);
    $doclabel = $doc->{meta}{basename} // $doc->{meta}{file_} // $doc->{label} // $docfile;
    $docid    = $doc->{id} // ++$docid;
    $sigi     = 0;
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

  ##-- save
  $vs->vlog($logCreate, "create(): saving to $vs->{base}*");
  $vs->saveHeader()
    or $vs->logconfess("create(): failed to save header data: $!");
  $map->saveDir("$vs->{base}_map.d", %{$vs->{dcio}//{}})
    or $vs->logconfess("create(): failed to save mapper data to $vs->{base}_map.d: $!");

  ##-- 

  return $vs;
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
