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
##    gkeys => $gkeys,       ##-- pdl ($NGKeys) : group-keys (+sorted)
##    gvals => $gvals,       ##-- pdl ($NGKeys) : group-similarities [-1:1]
##    gN    => $gN,          ##-- pdl (1)       : token total
##    gf1   => $gf1,         ##-- pdl (1)       : item1 frequency
##    gf2   => $gf2,         ##-- pdl ($NGKeys) : item2 frequencies
##    gf12  => $gf12,        ##-- pdl ($NGKeys) : (item1,item2) joint frequencies (undef->ignored)
##    missing => $missing,   ##-- missing value (default=$VAL_NONE)
##    ##
##   )
sub new {
  my $that = shift;
  my $gkeys = ref($_[0]) ? shift : undef;
  my $gvals = ref($_[0]) ? shift : undef;
  return bless({
		#label=>undef,
		(defined($gkeys) ? (gkeys=>$gkeys) : qw()),
		(defined($gvals) ? (gvals=>$gvals) : qw()),
		missing=>$MISSING,
		@_
	       }, ref($that)||$that);
}


## $pprf2 = $pprf->shadow()
##  + shadows profile with identical keys but all-missing score-values
sub shadow {
  my $pprf = shift;
  return ref($pprf)->new(%$pprf) if ($pprf->empty);

  my $gvals = zeroes($pprf->{gvals}->type,1);
  $gvals   .= $pprf->missing if ($pprf->missing);
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
  return DiaColloDB::Profile->new(label=>"$pprf->{label}",
				  N=>0,
				  %opts,
				  $pprf->profileScalar('N',$pprf->{N}),
				  $pprf->profileScalar('f1',$pprf->{f1}),
				  $pprf->profileHash('f2',$pprf->{f2}),
				  $pprf->profileHash('f12',$pprf->{f12}),
				  score=>"$score",
				  $pprf->profileHash($score,$gvals),
				 );
}

## ($label=>$val_sclr) = $pprf->profileScalar($pdl_or_scalar)
##  + gets a profile scalar e.g. for N~$N, f1~$gf1
##  + just returns empty list if $gvals is undefined
sub profileScalar {
  my ($pprf,$label,$val) = @_;
  return qw() if (!defined($val));
  return ($label=>(UNIVERSAL::isa($val,'PDL') ? $val->sclr : $val));
}

## ($label=>\%profileHash) = $pprf->profileHash($label,$gvals)
##  + gets a $gkeys-labelled profile hash e.g. for score~$gvals, f2~$gf2, g12~$gf12
##  + just returns empty list if $gvals is undefined
sub profileHash {
  my ($pprf,$label,$gvals) = @_;
  return qw() if (!defined($gvals));
  my $gkeys = $pprf->{gkeys};
  my $vals  = {};
  %$vals = (map {($gkeys->at($_)=>$gvals->at($_)) } (0..($gkeys->nelem-1))) if (!$pprf->empty);
  $_ = "NA" foreach (grep {$_!=$_} values %$vals); ##-- tweak "nan" values for jQuery-parseability
  return ($label=>$vals);
}

##==============================================================================
## API: trimming

##----------------------------------------------------------------------
## $gkeys_good = $pprf->gwhich(%opts)
##  + returned key-list is sorted
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
  return null->ccs_indx if ($pprf->empty);

  ##-- gwhich: by explicit cutoff
  if (($opts{cutoff}//'') ne '') {
    my $ibad = ($gvals < $opts{cutoff})->which;
    if ( (my $missing=$pprf->{missing}) ) {
      (my $tmp=$gvals->index($ibad)) .= $missing;
    }
    $igood = _setdiff_p($igood,$ibad,$gvals->nelem);
  }

  ##-- gwhich: k-best
  my $kbest;
  if (defined($kbest=$opts{kbest}) && $kbest > 0) {
    my $gsorti = $gvals->qsorti->slice("-1:0");
    my $gbesti = $gsorti->slice("0:".($kbest < $gsorti->nelem ? ($kbest-1) : "-1"));
    $gbesti->inplace->qsort;
    $igood = _intersect_p($igood,$gbesti);
  }

  ##-- gwhich: k-best (absolute values)
  my $kbesta;
  if (defined($kbesta=$opts{kbesta}) && $kbesta > 0) {
    my $gsorti = $gvals->abs->qsorti->slice("-1:0");
    my $gbesti = $gsorti->slice("0:".($kbesta < $gsorti->nelem ? ($kbesta-1) : "-1"));
    $gbesti->inplace->qsort;
    $igood = _intersect_p($igood,$gbesti);
  }

  ##-- which: return
  return defined($igood) ? $pprf->{gkeys}->index($igood)->qsort : $pprf->{gkeys};
}

##----------------------------------------------------------------------
## $pprf = $pprf->gtrim(%opts)
##  + %opts: as for gwhich(), also:
##    (
##     keep => $gwhich   ##-- pdl of group-vals to keep (+sorted); overrides other options if present (undef:all)
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

  $pprf->{gkeys} = $gkeys->index($k_keepi); #->qsort
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
  my $valt  = undef;
  foreach (grep {!$_->empty} @$pprfs) {
    $gkeys = _union_p($gkeys,$_->{gkeys});
    $valt  = $_->{gvals}->type if (!defined($valt) || $_->{gvals}->type > $valt);
  }
  return $pprf if ($gkeys->nelem==0);

  ##-- step 2: get score-values
  my $gvals = zeroes(($valt//double),$gkeys->nelem);
  foreach (@$pprfs) {
    if (!defined($_) || $_->empty) {
      my $missing = ($_ ? $_->missing : $that->missing);
      $gvals += $missing if ($missing);
      next;
    }
    my $keyi = $gkeys->vsearch($_->{gkeys});
    my $mask = ($gkeys == $_->{gkeys}->index($keyi));
    if ($mask->all) {
      $gvals += $_->{gvals}->index($keyi);
    } else {
      $gvals->where( $mask) += $_->{gvals}->index($keyi->where($mask));
      $gvals->where(!$mask) += $_->missing if ($_->missing);
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
