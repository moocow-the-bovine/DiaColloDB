## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, relation API (abstract & utilities)

package DiaColloDB::Relation;
use DiaColloDB::Persistent;
use DiaColloDB::Profile;
use DiaColloDB::Profile::Multi;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Persistent);

##==============================================================================
## Constructors etc.

## $rel = CLASS_OR_OBJECT->new(%args)
## + %args, object structure: see subclases
sub new {
  my ($that,%args) = @_;
  return bless({ %args }, ref($that)||$that);
}

##==============================================================================
## Relation API: create

## $rel = $CLASS_OR_OBJECT->create($coldb, $tokdat_file, %opts)
##  + populates current database from $tokdat_file,
##    a tt-style text file containing 1 token-id perl line with optional blank lines
##  + %opts: clobber %$rel
sub create {
  my ($rel,$coldb,$datfile,%opts) = @_;
  $rel->logconfess("create(): abstract method called");
}

##==============================================================================
## Relation API: union

## $rel = $CLASS_OR_OBJECT->union($coldb, \@pairs, %opts)
##  + merge multiple co-frequency indices into new object
##  + @pairs : array of pairs ([$argrel,\@xi2u],...)
##    of relation-objects $argrel and tuple-id maps \@xi2u for $rel
##  + %opts: clobber %$rel
##  + implicitly flushes the new index
sub union {
  my ($rel,$coldb, $pairs,%opts) = @_;
  $rel->logconfess("union(): abstract method called");
}

##==============================================================================
## Relation API: profiling & comparison: top-level

##--------------------------------------------------------------
## Relation API: profile

## $mprf = $rel->profile($coldb, %opts)
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
##  + default implementation calls $rel->subprofile() for every requested date-slice
##    and collects the result in a DiaColloDB::Profile::Multi object
##  + default values for %opts should be set by higher-level call, e.g. DiaColloDB::profile()
sub profile {
  my ($reldb,$coldb,%opts) = @_;

  ##-- common variables
  my $logProfile = $coldb->{logProfile};

  ##-- variables: by attribute
  my $groupby= $coldb->groupby($opts{groupby});
  my $attrs  = $coldb->attrs();
  my $adata  = $coldb->attrData($attrs);
  my $a2data = {map {($_->{a}=>$_)} @$adata};
  my $areqs  = $coldb->parseRequest($opts{query}, logas=>'query', default=>$attrs->[0]);
  foreach (@$areqs) {
    $a2data->{$_->[0]}{req} = $_->[1];
  }

  ##-- sanity check(s)
  if (!@$areqs) {
    $reldb->logwarn($coldb->{error}="profile(): no target attributes specified (supported attributes: ".join(' ',@{$coldb->attrs}).")");
    return undef;
  }
  if (!@{$groupby->{attrs}}) {
    $reldb->logconfess($coldb->{error}="profile(): cannot profile with empty groupby clause");
    return undef;
  }

  ##-- prepare: get target IDs (by attribute)
  my ($ac);
  foreach $ac (grep {($_->{req}//'') ne ''} @$adata) {
    $ac->{reqids} = $coldb->enumIds($ac->{enum},$ac->{req},logLevel=>$logProfile,logPrefix=>"profile(): get target $ac->{a}-values");
    if (!@{$ac->{reqids}}) {
      $reldb->logwarn($coldb->{error}="profile(): no $ac->{a}-attribute values match user query '$ac->{req}'");
      return undef;
    }
  }

  ##-- prepare: get tuple-ids (by attribute)
  $reldb->vlog($logProfile, "profile(): get target tuple IDs");
  my $xiset = undef;
  foreach $ac (grep {$_->{reqids}} @$adata) {
    my $axiset = {};
    @$axiset{map {@{$ac->{a2x}->fetch($_)}} @{$ac->{reqids}}} = qw();
    if (!$xiset) {
      $xiset = $axiset;
    } else {
      delete @$xiset{grep {!exists $axiset->{$_}} keys %$xiset};
    }
  }
  my $xis = [sort {$a<=>$b} keys %$xiset];
  if ($coldb->{maxExpand}>0 && @$xis > $coldb->{maxExpand}) {
    $reldb->logwarn("profile(): Warning: target set exceeds max expansion size (",scalar(@$xis)." > $coldb->{maxExpand}): truncating");
    $#$xis = $coldb->{maxExpand}-1;
  }

  ##-- prepare: parse and filter tuples
  $reldb->vlog($logProfile, "profile(): parse and filter target tuples (date=$opts{date}, slice=$opts{slice}, fill=$opts{fill})");
  my $d2xis = $coldb->xidsByDate($xis, @opts{qw(date slice fill)});

  ##-- profile: get relation profiles (by date-slice)
  $reldb->vlog($logProfile, "profile(): get frequency profile(s)");
  my @dprfs  = qw();
  my ($d,$prf);
  foreach $d (sort {$a<=>$b} keys %$d2xis) {
    $prf = $reldb->subprofile($d2xis->{$d}, groupby=>$groupby->{x2g});
    $prf->compile($opts{score}, eps=>$opts{eps});
    $prf->trim(kbest=>$opts{kbest}, cutoff=>$opts{cutoff});
    $prf = $prf->stringify($groupby->{g2s}) if ($opts{strings});
    $prf->{label}  = $d;
    $prf->{titles} = $groupby->{titles};
    push(@dprfs, $prf);
  }

  return DiaColloDB::Profile::Multi->new(profiles=>\@dprfs, titles=>$groupby->{titles});
}

##--------------------------------------------------------------
## Relation API: comparison (diff)

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
##  + default implementation wraps profile() method
##  + default values for %opts should be set by higher-level call, e.g. DiaColloDB::compare()
BEGIN { *diff = \&compare; }
sub compare {
  my ($reldb,$coldb,%opts) = @_;

  ##-- common variables
  my $logProfile = $coldb->{logProfile};
  my $groupby    = $coldb->groupby($opts{groupby} || [@{$coldb->attrs}]);
  my %aopts      = map {($_=>$opts{"a$_"})} (qw(query date slice));
  my %bopts      = map {($_=>$opts{"b$_"})} (qw(query date slice));
  my %popts      = (kbest=>-1,cutoff=>'',strings=>0,fill=>1, groupby=>$groupby);

  ##-- get profiles to compare
  my $mpa = $reldb->profile($coldb,%opts, %aopts,%popts) or return undef;
  my $mpb = $reldb->profile($coldb,%opts, %bopts,%popts) or return undef;

  ##-- alignment and trimming
  $reldb->vlog($logProfile, "compare(): align and trim");
  my $ppairs = DiaColloDB::Profile::MultiDiff->align($mpa,$mpb);
  my %trim   = (kbest=>($opts{kbest}//-1), cutoff=>($opts{cutoff}//''));
  my ($pa,$pb,%pkeys);
  foreach (@$ppairs) {
    ($pa,$pb) = @$_;
    %pkeys = map {($_=>undef)} (($pa ? @{$pa->which(%trim)} : qw()), ($pb ? @{$pb->which(%trim)} : qw()));
    $pa->trim(keep=>\%pkeys);
    $pb->trim(keep=>\%pkeys);
  }

  ##-- diff and stringification
  $reldb->vlog($logProfile, "compare(): diff and stringification");
  my $diff = DiaColloDB::Profile::MultiDiff->new($mpa,$mpb,titles=>$groupby->{titles});
  $diff->trim(kbesta=>$opts{kbest});
  $diff->stringify($groupby->{g2s}) if ($opts{strings}//1);

  return $diff;
}



##==============================================================================
## Relation API: default: subprofile()

## $prf = $rel->profile(\@xids, %opts)
##  + get frequency profile for @xids (db must be opened)
##  + %opts:
##     groupby => \&gbsub,  ##-- key-extractor $key2_or_undef = $gbsub->($i2)
sub subprofile {
  my ($rel,$ids,%opts) = @_;
  $rel->logconfess("subprofile(): abstract method called");
}

##==============================================================================
## Footer
1;

__END__



