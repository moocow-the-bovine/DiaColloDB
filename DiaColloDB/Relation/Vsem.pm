## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model (via DocClassify)

package DiaColloDB::Relation::Vsem;
use DiaColloDB::Relation;
use DiaColloDB::Utils qw(:sort :env :run :pack :file);
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
##   ##-- PackedFile: user options
##   base   => $basename,   ##-- relation basename
##   ##
##   ##-- guts
##   dcmap  => $dcmap,      ##-- underlying DocClassify::Mapper object
##   )
sub new {
  my $that = shift;
  my $vs   = $that->DiaColloDB::PackedFile::new(
						dcmap => undef,
						@_
					       );
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
## Persistent API: open/close: TODO

## $bool = $obj->opened()
sub opened {
  return $_[0]{dcmap} && $_[0]{dcmap}->compiled();
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

  ##-- TODO: CONTINUE HERE

  ##-- ensure openend
  $vs->opened
    or $vs->open()
      or $vs->logconfess("create(): failed to open vector-space semantic model: $!");

  ##-- done
  $vs->logconfess("create(): not yet implemented");
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
