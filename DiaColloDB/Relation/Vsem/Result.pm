## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem::Result.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model: pre-profiles

package DiaColloDB::Relation::Vsem::Result;
use DiaColloDB::Utils qw(:pdl);
use PDL;
use PDL::VectorValued;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

our $SIM_MIN = -1;

##==============================================================================
## Constructors etc.

## $vr = CLASS_OR_OBJECT->new(%args)
## $vr = CLASS_OR_OBJECT->new($g_keys,$g_sim,%args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Profile-like attributes
##    label => $label,         ##-- profile label (default=undef)
##    ##-- subprofile guts
##    g_keys => $g_keys,       ##-- pdl ($NGKeys) : group-keys
##    g_sim  => $g_sim,        ##-- pdl ($NGKeys) : group-similarities [-1:1]
##    ##
##   )
sub new {
  my $that = shift;
  my $g_keys = ref($_[0]) ? shift : undef;
  my $g_sim = ref($_[0]) ? shift : undef;
  return bless({
		#label=>undef,
		g_keys=>$g_keys,
		g_sim=>$g_sim,
		@_
	       }, ref($that)||$that);
}

##==============================================================================
## API: convert to DiaColloDB::Profile

## $bool = $vr->empty()
##  + returns true iff result is empty
sub empty {
  return !defined($_[0]{g_keys}) || $_[0]{g_keys}->nelem==0;
}

## $prf = $vr->toProfile(%opts)
##  + convert to a DiaColloDB::Profile object
##  + %opts are passed to DiaColloDB::Profile->new()
sub toProfile {
  my ($vr,%opts) = @_;
  my ($g_keys,$g_sim) = @$vr{qw(g_keys g_sim)};
  my $vsim = {};
  %$vsim = (map {($g_keys->at($_)=>$g_sim->at($_)) } (0..($g_keys->nelem-1))) if (!$vr->empty);
  return DiaColloDB::Profile->new(N=>0, label=>$vr->{label}, %opts, score=>'vsim', vsim=>$vsim);
}

##==============================================================================
## API: trimming

## $g_keys_good = $vr->gwhich(%opts)
##  + %opts:
##    (
##     cutoff => $cutoff,  ##-- retain only items with $prf->{$prf->{score}}{$item} >= $cutoff
##     kbest  => $kbest,   ##-- retain only $kbest items
##     kbesta => $kbest,   ##-- retain only $kbest items by absolute value
##    )
sub gwhich {
  my ($vr,%opts) = @_;

  ##-- common variables
  my $g_sim = $vr->{g_sim};
  my $igood  = undef;

  ##-- sanity checks
  return null->long if ($vr->empty);

  ##-- gwhich: by explicit cutoff
  if (($opts{cutoff}//'') ne '') {
    my $ibad = ($g_sim < $opts{cutoff})->which;
    (my $tmp=$g_sim->index($ibad)) .= $SIM_MIN;
    $igood = _setdiff_p($igood,$ibad,$g_sim->nelem);
  }

  ##-- gwhich: k-best
  my $kbest;
  if (defined($kbest=$opts{kbest}) && $kbest > 0) {
    my $g_sorti = $g_sim->qsorti->slice("-1:0");
    my $g_besti = $g_sorti->slice("0:".($kbest < $g_sorti->nelem ? ($kbest-1) : "-1"));
    $igood = _intersect_p($igood,$g_besti);
  }

  ##-- gwhich: k-best (absolute values)
  my $kbesta;
  if (defined($kbesta=$opts{kbesta}) && $kbesta > 0) {
    my $g_sorti = $g_sim->abs->qsorti->slice("-1:0");
    my $g_besti = $g_sorti->slice("0:".($kbesta < $g_sorti->nelem ? ($kbesta-1) : "-1"));
    $igood = _intersect_p($igood,$g_besti);
  }

  ##-- which: return
  return defined($igood) ? $vr->{g_keys}->index($igood) : $vr->{g_keys};
}

## $vr = $vr->gtrim(%opts)
##  + %opts: as for gwhich(), also:
##    (
##     keep => $gwhich   ##-- pdl of group-vals to keep; overrides other options if present (undef:all)
##    )
sub gtrim {
  my ($vr,%opts) = @_;

  ##-- sanity checks
  return $vr if ($vr->empty);

  ##-- get target items
  my $keep = exists($opts{keep}) ? $opts{keep} : $vr->gwhich(%opts);
  return $vr if (!defined($keep));

  ##-- perform trimming
  my $g_keys  = $vr->{g_keys};
  my $k_keyi  = $keep->vsearch($g_keys);
  my $k_keepi = $k_keyi->where( $g_keys->index($k_keyi)==$keep );

  $vr->{g_keys} = $g_keys->index($k_keepi);
  $vr->{g_sim}  = $vr->{g_sim}->index($k_keepi);

  return $vr;
}

##==============================================================================
## Algebraic operations

## $vr_avg = CLASS_OR_OBJECT->averageOver(\@vrs,%opts)
##  + get average distance over multiple profiles
##  + %opts are passed to CLASS_OR_OBJECT->new()
sub averageOver {
  my ($that,$vrs,%opts) = @_;
  my $vr = $that->new(%opts);

  ##-- step 1: get keys
  my $g_keys = undef;
  $g_keys = _union_p($g_keys,$_->{g_keys}) foreach (@$vrs);
  return $vr if (!defined($g_keys) || $g_keys->nelem==0);

  ##-- step 2: get distances
  my $g_sim = zeroes(double,$g_keys->nelem);
  foreach (@$vrs) {
    if (!defined($_) || $_->empty) {
      $g_sim += $SIM_MIN;
      next;
    }
    my $keyi = $g_keys->vsearch($_->{g_keys});
    my $mask = ($g_keys == $_->{g_keys}->index($keyi));
    if ($mask->all) {
      $g_sim += $_->{g_sim}->index($keyi);
    } else {
      $g_sim->where( $mask) += $_->{g_sim}->index($keyi->where($mask));
      $g_sim->where(!$mask) += $SIM_MIN;
    }
  }
  $g_sim /= scalar(@$vrs);

  @$vr{qw(g_keys g_sim)} = ($g_keys,$g_sim);
  return $vr;
}

##==============================================================================
## Footer
1;

__END__
