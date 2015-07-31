## -*- Mode: CPerl -*-
## File: DiaColloDB::Profile::Pdl.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, result profile: pdl-ized (e.g. for DiaColloDB::Relation::Vsem)

package DiaColloDB::Profile::Pdl;
use DiaColloDB::Profile;
use DiaColloDB::Utils qw(:pdl :list);
use PDL;
use PDL::VectorValued;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Profile);

## $MISSING : default value for missing keys
our $MISSING = -1;

##==============================================================================
## Constructors etc.

## $pprf = CLASS_OR_OBJECT->new(%args)
## $pprf = CLASS_OR_OBJECT->new($gkeys,$gvals,%args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Profile attributes
##    label => $label,       ##-- profile label (default=undef)
##    score => $score,       ##-- output score function name (for toProfile() method)
##    ##
##    ##-- NEW for DiaColloDB::Profile::Pdl
##    gkeys => $gkeys,       ##-- pdl ($NGKeys) : group-keys
##    gvals => $gvals,       ##-- pdl ($NGKeys) : group-similarities [-1:1]
##    missing => $missing,   ##-- missing value (default=$VAL_NONE)
##    ##
##   )
sub new {
  my $that = shift;
  my $gkeys = ref($_[0]) ? shift : undef;
  my $gvals = ref($_[0]) ? shift : undef;
  return bless({
		#label=>undef,
		gkeys=>$gkeys,
		gvals=>$gvals,
		missing=>$MISSING,
		@_
	       }, ref($that)||$that);
}


## $pprf2 = $pprf->shadow()
##  + shadows profile with identical keys but all-missing values
sub shadow {
  my $pprf = shift;
  return ref($pprf)->new(%$pprf) if ($pprf->empty);

  my $gvals = zeroes($pprf->{gvals}->type,1);
  $gvals   .= $pprf->missing;
  return ref($pprf)->new(%$pprf, gkeys=>$pprf->{keys}, gvals=>$gvals);
}

##==============================================================================
## API: Basic Access

## $missing = $pprf->missing()
## $missing = $pprf->missing($missing)
##  + get/set missing value
sub missing {
  my $that = shift;
  if (!ref($that)) {
    $MISSING = $_[0] if (@_);
    return $MISSING;
  }
  $that->{missing} = $_[0] if (@_);
  return $that->{missing};
}

## $bool = $pprf->empty()
##  + returns true iff result is empty
sub empty {
  return !defined($_[0]{gkeys}) || $_[0]{gkeys}->nelem==0;
}

## $prf = $pprf->toProfile(%opts)
##  + convert to a DiaColloDB::Profile object
##  + %opts are passed to DiaColloDB::Profile->new()
##  + additionally, 'score' sets the score-key to use (default=$pprf->{score} or 'vsim')
sub toProfile {
  my ($pprf,%opts) = @_;
  my ($gkeys,$gvals) = @$pprf{qw(gkeys gvals)};
  my $score = $opts{score} // $pprf->{score} // 'vsim';
  my $vals  = {};
  %$vals = (map {($gkeys->at($_)=>$gvals->at($_)) } (0..($gkeys->nelem-1))) if (!$pprf->empty);
  return DiaColloDB::Profile->new(N=>0, label=>$pprf->{label}, %opts, score=>$score, $score=>$vals);
}

##==============================================================================
## API: trimming

##----------------------------------------------------------------------
## $gkeys_good = $pprf->gwhich(%opts)
##  + %opts:
##    (
##     cutoff => $cutoff,  ##-- retain only items with $prf->{$prf->{score}}{$item} >= $cutoff
##     kbest  => $kbest,   ##-- retain only $kbest items
##     kbesta => $kbest,   ##-- retain only $kbest items by absolute value
##    )
sub gwhich {
  my ($pprf,%opts) = @_;

  ##-- common variables
  my $gvals = $pprf->{gvals};
  my $igood  = undef;

  ##-- sanity checks
  return null->long if ($pprf->empty);

  ##-- gwhich: by explicit cutoff
  if (($opts{cutoff}//'') ne '') {
    my $ibad = ($gvals < $opts{cutoff})->which;
    (my $tmp=$gvals->index($ibad)) .= $pprf->missing;
    $igood = _setdiff_p($igood,$ibad,$gvals->nelem);
  }

  ##-- gwhich: k-best
  my $kbest;
  if (defined($kbest=$opts{kbest}) && $kbest > 0) {
    my $gsorti = $gvals->qsorti->slice("-1:0");
    my $gbesti = $gsorti->slice("0:".($kbest < $gsorti->nelem ? ($kbest-1) : "-1"));
    $igood = _intersect_p($igood,$gbesti);
  }

  ##-- gwhich: k-best (absolute values)
  my $kbesta;
  if (defined($kbesta=$opts{kbesta}) && $kbesta > 0) {
    my $gsorti = $gvals->abs->qsorti->slice("-1:0");
    my $gbesti = $gsorti->slice("0:".($kbesta < $gsorti->nelem ? ($kbesta-1) : "-1"));
    $igood = _intersect_p($igood,$gbesti);
  }

  ##-- which: return
  return defined($igood) ? $pprf->{gkeys}->index($igood) : $pprf->{gkeys};
}

##----------------------------------------------------------------------
## $pprf = $pprf->gtrim(%opts)
##  + %opts: as for gwhich(), also:
##    (
##     keep => $gwhich   ##-- pdl of group-vals to keep; overrides other options if present (undef:all)
##    )
sub gtrim {
  my ($pprf,%opts) = @_;

  ##-- sanity checks
  return $pprf if ($pprf->empty);

  ##-- get target items
  my $keep = exists($opts{keep}) ? $opts{keep} : $pprf->gwhich(%opts);
  return $pprf if (!defined($keep));

  ##-- perform trimming
  my $gkeys  = $pprf->{gkeys};
  my $k_keyi  = $keep->vsearch($gkeys);
  my $k_keepi = $k_keyi->where( $gkeys->index($k_keyi)==$keep );

  $pprf->{gkeys} = $gkeys->index($k_keepi);
  $pprf->{gvals} = $pprf->{gvals}->index($k_keepi);

  return $pprf;
}

##==============================================================================
## Algebraic operations

## $pprf_avg = CLASS_OR_OBJECT->averageOver(\@pprfs,%opts)
##  + get average distance over multiple profiles
##  + %opts are passed to CLASS_OR_OBJECT->new()
sub averageOver {
  my ($that,$pprfs,%opts) = @_;

  ##-- common
  my $pprf = $that->new(%opts);
  my ($tmp);

  ##-- step 1: get keys
  #  my $nkeys = pdl(long,[map {$_->{gkeys}->nelem} grep {!$_->empty} @$pprfs])->sum;
  #  my $off   = 0;
  #  my $gkeys = zeroes(long, $nkeys);
  #  foreach (grep {!$_->empty} @$pprfs) {
  #    ($tmp=$gkeys->slice("$off:".($_->{gkeys}->nelem-1))) .= $_->{gkeys};
  #  }
  #  $gkeys = $gkeys->uniq;
  ##--
  my $gkeys = undef;
  $gkeys = _union_p($gkeys,$_->{gkeys}->qsort) foreach (grep {!$_->empty} @$pprfs);
  return $pprf if ($gkeys->nelem==0);

  ##-- step 2: get score-values
  my $gvals = zeroes(double,$gkeys->nelem);
  foreach (@$pprfs) {
    if (!defined($_) || $_->empty) {
      $gvals += ($_ ? $_->missing : $that->missing);
      next;
    }
    my $keyi = $gkeys->vsearch($_->{gkeys});
    my $mask = ($gkeys == $_->{gkeys}->index($keyi));
    if ($mask->all) {
      $gvals += $_->{gvals}->index($keyi);
    } else {
      $gvals->where( $mask) += $_->{gvals}->index($keyi->where($mask));
      $gvals->where(!$mask) += $_->missing;
    }
  }
  $gvals /= scalar(@$pprfs);

  @$pprf{qw(gkeys gvals)} = ($gkeys,$gvals);
  return $pprf;
}

##==============================================================================
## Footer
1;

__END__
