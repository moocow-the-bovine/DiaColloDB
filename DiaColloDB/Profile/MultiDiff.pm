## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Profile::MultiDiff.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, co-frequency profile diffs, by date


package DiaColloDB::Profile::MultiDiff;
use DiaColloDB::Profile::Multi;
use DiaColloDB::Profile::Diff;
use DiaColloDB::Utils qw(:html);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Profile::Multi);

##==============================================================================
## Constructors etc.

## $mpd = CLASS_OR_OBJECT->new(%args)
## $mpd = CLASS_OR_OBJECT->new($mp1,$mp2,%args)
## + %args, object structure:
##   (
##    data => \%key2prf,   ##-- ($date => $profile, ...) : profiles by date
##   )
sub new {
  my $that = shift;
  my $mp1  = UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile::Multi') ? shift : undef;
  my $mp2  = UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile::Multi') ? shift : undef;
  my $mpd  = $that->SUPER::new(@_);
  return $mpd->populate($mp1,$mp2) if ($mp1 && $mp2);
  return $mpd;
}


## $mp2 = $mp->clone()
## $mp2 = $mp->clone($keep_compiled)
##  + clones %$mp
##  + if $keep_score is true, compiled data is cloned too
##  + INHERITED from DiaColloDB::Profile::Multi

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: JSON
##  + INHERITED from DiaCollocDB::Persistent

##--------------------------------------------------------------
## I/O: Text

## $bool = $obj->saveTextFile($filename_or_handle, %opts)
##  + wraps saveTextFh(); INHERITED from DiaCollocDB::Persistent

## $bool = $mp->saveTextFh($fh)
##  + save text representation to a filehandle (guts)
##  + INHERITED from DiaCollocDB::Profile::Multi

##--------------------------------------------------------------
## I/O: HTML

## $bool = $mp->saveHtmlFile($filename_or_handle, %opts)
##  + %opts:
##    (
##     table  => $bool,     ##-- include <table>..</table> ? (default=1)
##     body   => $bool,     ##-- include <html><body>..</html></body> ? (default=1)
##     header => $bool,     ##-- include header-row? (default=1)
##    )
##  + TODO
sub saveHtmlFile {
  my ($mp,$file,%opts) = @_;
  $mp->logconfess("saveHtmlFile(): not yet implemented");
}

##==============================================================================
## Compilation

##  @ppairs = $CLASS_OR_OBJECT->align($mp1,$mp2)
## \@ppairs = $CLASS_OR_OBJECT->align($mp1,$mp2)
##  + aligns subprofile-pairs from $mp1 and $mp2
##  + subprofiles are aligned cyclically, in stored order
##    - this lets you compare e.g. a global profile with a sliced one by
##      something like PDL's "implicit threading"
sub align {
  my ($that,$mpa,$mpb) = @_;
  my $psa = $mpa->{profiles};
  my $psb = $mpb->{profiles};
  my $null = @$psa && @$psb ? undef : DiaColloDB::Profile->new();
  my @pairs = map {
    [($psa->[$_ % @$psa]//$null), ($psb->[$_ % @$psb]//$null)]
  } (0..($#$psa > $#$psb ? $#$psa : $#$psb));
  return wantarray ? @pairs : \@pairs;
}

## $mpd = $mpd->populate($mp1,$mp2)
##  + populates multi-diff by subtracting $mp1 sub-profile scores from $mp1
##  + uses $mpd->align() to align sub-profiles
sub populate {
  my ($mpd,$mpa,$mpb) = @_;
  @{$mpd->{profiles}} = map {
    DiaColloDB::Profile::Diff->new($_->[0],$_->[1])
  } @{$mpd->align($mpa,$mpb)};
  return $mpd;
}

## $mp_or_undef = $mp->compile($func)
##  + compile all sub-profiles for score-function $func, one of qw(f mi ld); default='f'
##  + INHERITED from DiaColloDB::Profile::Multi

## $mp = $mp->uncompile()
##  + un-compiles all scores for $mp
##  + INHERITED from DiaColloDB::Profile::Multi

## $mp_or_undef = $mp->trim(%opts)
##  + calls $prf->trim(%opts) for each sub-profile $prf
##  + INHERITED from DiaColloDB::Profile::Multi

## $mp = $mp->stringify( $obj)
## $mp = $mp->stringify(\@key2str)
## $mp = $mp->stringify(\&key2str)
## $mp = $mp->stringify(\%key2str)
##  + stringifies multi-profile (destructive) via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
##  + INHERITED from DiaColloDB::Profile::Multi

##==============================================================================
## Binary operations

## $mp = $mp->_add($mp2,%opts)
##  + adds $mp2 frequency data to $mp (destructive)
##  + implicitly un-compiles sub-profiles
##  + %opts: passed to Profile::_add()
##  + INHERITED but probably useless

## $mp3 = $mp1->add($mp2,%opts)
##  + returns sum of $mp1 and $mp2 frequency data (destructive)
##  + %opts: passed to Profile::_add()
##  + INHERITED but probably useless

## $diff = $mp1->diff($mp2)
##  + returns score-diff of $mp1 and $mp2 frequency data (destructive)
##  + INHERITED but probably useless

##==============================================================================
## Package DiaColloDB::Profile::Multi::Diff : alias
package DiaColloDB::Profile::Multi::Diff;
our @ISA = qw(DiaColloDB::Profile::MultiDiff);


##==============================================================================
## Footer
1;

__END__
