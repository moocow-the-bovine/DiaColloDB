## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem::Result.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model: pre-profiles

package DiaColloDB::Relation::Vsem::Result;
use DiaColloDB::Utils qw(:pdl :list);
use DiaColloDB::Profile::Diff;
use PDL;
use PDL::VectorValued;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

our $VAL_NONE = -1;

##==============================================================================
## Constructors etc.

## $vr = CLASS_OR_OBJECT->new(%args)
## $vr = CLASS_OR_OBJECT->new($g_keys,$g_vals,%args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Profile-like attributes
##    label => $label,         ##-- profile label (default=undef)
##    ##-- subprofile guts
##    g_keys => $g_keys,       ##-- pdl ($NGKeys) : group-keys
##    g_vals => $g_vals,       ##-- pdl ($NGKeys) : group-similarities [-1:1]
##    ##
##   )
sub new {
  my $that = shift;
  my $g_keys = ref($_[0]) ? shift : undef;
  my $g_vals = ref($_[0]) ? shift : undef;
  return bless({
		#label=>undef,
		g_keys=>$g_keys,
		g_vals=>$g_vals,
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
  my ($g_keys,$g_vals) = @$vr{qw(g_keys g_vals)};
  my $vsim = {};
  %$vsim = (map {($g_keys->at($_)=>$g_vals->at($_)) } (0..($g_keys->nelem-1))) if (!$vr->empty);
  return DiaColloDB::Profile->new(N=>0, label=>$vr->{label}, %opts, score=>'vsim', vsim=>$vsim);
}

## $ccs = $vr->toCCS()
##  + returns a PDL::CCS::Nd object for $vr
sub toCCS {
  my $vr=shift;
  return PDL::CCS::Nd->newFromWhich(@$vr{qw(g_keys g_vals)}, missing=>$VAL_MIN);
}

##==============================================================================
## API: trimming

##----------------------------------------------------------------------
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
  my $g_vals = $vr->{g_vals};
  my $igood  = undef;

  ##-- sanity checks
  return null->long if ($vr->empty);

  ##-- gwhich: by explicit cutoff
  if (($opts{cutoff}//'') ne '') {
    my $ibad = ($g_vals < $opts{cutoff})->which;
    (my $tmp=$g_vals->index($ibad)) .= $VAL_NONE;
    $igood = _setdiff_p($igood,$ibad,$g_vals->nelem);
  }

  ##-- gwhich: k-best
  my $kbest;
  if (defined($kbest=$opts{kbest}) && $kbest > 0) {
    my $g_sorti = $g_vals->qsorti->slice("-1:0");
    my $g_besti = $g_sorti->slice("0:".($kbest < $g_sorti->nelem ? ($kbest-1) : "-1"));
    $igood = _intersect_p($igood,$g_besti);
  }

  ##-- gwhich: k-best (absolute values)
  my $kbesta;
  if (defined($kbesta=$opts{kbesta}) && $kbesta > 0) {
    my $g_sorti = $g_vals->abs->qsorti->slice("-1:0");
    my $g_besti = $g_sorti->slice("0:".($kbesta < $g_sorti->nelem ? ($kbesta-1) : "-1"));
    $igood = _intersect_p($igood,$g_besti);
  }

  ##-- which: return
  return defined($igood) ? $vr->{g_keys}->index($igood) : $vr->{g_keys};
}

##----------------------------------------------------------------------
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
  $vr->{g_vals} = $vr->{g_vals}->index($k_keepi);

  return $vr;
}

##----------------------------------------------------------------------
## \@vrs = CLASS_OR_OBJECT->trimPairs(\@vrs,%opts)
##  + %opts: as for DiaColloDB::Profile::Multi::trim(), including 'global' and 'diff' options
##  + adapted from DiaColloDB::Profile::MultiDiff::trimPairs()
sub trimPairs {
  my ($that,$ppairs,%opts) = @_;

  ##-- defaults
  $opts{kbest}  //= -1;
  $opts{cutoff} //= '';
  $opts{global} //= 0;
  $opts{diff}   //= 'adiff';

  if ($opts{global}) {
    ##-- trim globally
    my $gpa = $that->averageOver(xluniq([map {$_->[0]} @$ppairs]), eps=>$opts{eps});
    my $gpb = $that->averageOver(xluniq([map {$_->[0]} @$ppairs]), eps=>$opts{eps});
    my $keep;
    if (DiaColloDB::Profile::Diff->diffpretrim($opts{diff})) {
      ##-- pre-trim (global)
      $keep = _union_p($gpa->gwhich(%opts), $gpb->gwhich(%opts));
      $gpa->gtrim(keep=>$keep);
      $gpb->gtrim(keep=>$keep);
    }

    my $gdiff = $that->diff($gpa,$gbp, diff=>$opts{diff});
    my $keep  = $gdiff->gwhich( DiaColloDB::Profile::Diff->diffkbest($opts{diff})=>$opts{kbest} );
    $_->gtrim(keep=>$keep) foreach (grep {$_} map {@$_} @$ppairs);
  }
  elsif (DiaColloDB::Profile::Diff->diffpretrim($opts{diff})) {
    ##-- (pre-)trim locally
    my ($pa,$pb,$pdiff,$keep);
    foreach (@$ppairs) {
      ($pa,$pb) = @$_;
      next if (!defined($pdiff = $that->diff($pa,$pb,diff=>$opts{diff})));
      $keep = $pdiff->trim(keep=>$keep);
      $pa->gtrim(keep=>$keep) if ($pa);
      $pb->gtrim(keep=>$keep) if ($pb);
    }
 }

  return $ppairs;
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
  my $g_vals = zeroes(double,$g_keys->nelem);
  foreach (@$vrs) {
    if (!defined($_) || $_->empty) {
      $g_vals += $SIM_MIN;
      next;
    }
    my $keyi = $g_keys->vsearch($_->{g_keys});
    my $mask = ($g_keys == $_->{g_keys}->index($keyi));
    if ($mask->all) {
      $g_vals += $_->{g_vals}->index($keyi);
    } else {
      $g_vals->where( $mask) += $_->{g_vals}->index($keyi->where($mask));
      $g_vals->where(!$mask) += $VAL_NONE;
    }
  }
  $g_vals /= scalar(@$vrs);

  @$vr{qw(g_keys g_vals)} = ($g_keys,$g_vals);
  return $vr;
}

## $vr_diff = CLASS->diff($vr1,$vr2,%opts)
##  + %opts: passed to ref($vr1)->new()
##    (
##     diff=>$diff,  ##-- diff-op as for DiaColloDB::Profile::Diff::diffop(); default='adiff'
##    )
sub diff {
  my ($that,$vr1,$vr2,%opts) = @_;
  my $diffop  = DiaColloDB::Profile::Diff->diffop($opts{diff});
  my $diffsub = $that->diffsub($diffop);

  ##-- edge cases / sanity checks
  my ($dkeys,$dvals,$tmp);
  if (!defined($vr1)) {
    ##-- diff(A,\empty)
    return undef if (!defined($vr2));
    $dkeys = $vr2->{g_keys};
    $dvals = zeroes($vr2->{g_vals}->type, 2,$vr2->{g_vals}->nelem);
    ($tmp=$dvals->slice("(0),")) .= $VAL_NONE;
    ($tmp=$dvals->slice("(1),")) .= $vr2->{g_vals};
  }
  elsif (!defined($vr2)) {
    ##-- diff(\empty,B)
    $dkeys = $vr1->{g_keys};
    $dvals = zeroes($vr1->{g_vals}->type, 2,$vr1->{g_vals}->nelem);
    ($tmp=$dvals->slice("(0),")) .= $vr1->{g_vals};
    ($tmp=$dvals->slice("(1),")) .= $VAL_NONE;
  }
  else {
    ##-- non-trivial diff(A,B)
    my ($keys1,$vals1,$keys2,$vals2) = map {@$_{qw(g_keys g_vals)}} ($vr1,$vr2);
    if ($keys1->nelem==$keys2->nelem && all($keys1==$keys2)) {
      ##-- optimize for co-indexed keys
      $dkeys = $keys1;
      $dvals = $vals1->cat($vals2)->xchg(0,1);
    } else {
      ##-- keys are not co-indexable: use vsearch
      $dkeys     = $keys1->setops('OR',$keys2);
      my $keys1i = $keys1->vsearch($dkeys);
      my $keys2i = $keys2->vsearch($dkeys);
      my $keys1m = ($dkeys->index($keys1i) == $keys1);
      my $keys2m = ($dkeys->index($keys2i) == $keys2);

      $dvals = zeroes($vals1->type, 2,$keys->nelem) + $VAL_NONE;
      ($tmp=$dvals->slice("(0),")->index($keys1i->where($keys1m))) .= $vals1->where($keys1m);
      ($tmp=$dvals->slice("(1),")->index($keys2i->where($keys2m))) .= $vals2->where($keys2m);
    }
  }

  my $diff = $diffsub->($dvals);
  return ref($vr1)->new($dkeys,$diff, %opts);
}

##----------------------------------------------------------------------
## Compilation: diff-ops

## \&FUNC = $vr->diffsub()
## \&FUNC = $CLASS_OR_OBJECT->diffsub($opNameOrAlias)
##  + gets low-level binary diff operation for diff-operation $opNameOrAlias (default=$dprf->{diff})
##  + wraps DiaColloDB::Profile::Diff::diffsub
sub diffsub {
  return DiaColloDB::Profile::Diff::diffsub(@_);
}

##-- diffsubs: called as DIFFSUB($dvals)
## + $dvals: pdl (2,$NG) : [0]=>$aval, [1]=>$bval
BEGIN { *diffop_adiff = \&diffop_diff; }
sub diffop_diff  { return $_[0]->slice("(0),")-$_[0]->slice("(1),"); }
sub diffop_sum   { return $_[0]->sumover; }
sub diffop_min   { return $_[0]->minimum; }
sub diffop_max   { return $_[0]->maximum; }
sub diffop_avg   { return $_[0]->average; }

#sub diffop_havg  { return $_[0]<=0 || $_[1]<=0 ? 0 : 2.0/(1.0/$_[0] + 1.0/$_[1]); }
##--
#our $havg_eps = 0.1;
#sub diffop_havg  { return 2.0/(1.0/($_[0]+$havg_eps) + 1.0/($_[1]+$havg_eps)) - $havg_eps; }
##--
#sub diffop_havg0  { return $_[0]<=0 || $_[1]<=0 ? 0 : (2*$_[0]*$_[1])/($_[0]+$_[1]); }
#sub diffop_havg   { return diffop_avg(diffop_havg0(@_),diffop_avg(@_)); }
##--
sub diffop_havg {
  my $dvals = $_[0]-$VAL_NONE;   ##-- work on range [0:2] ($VAL_NONE)
  my $havg  = $dvals->prodover;
  $havg    *= 2;
  $havg    /= $dvals->sumover;
  $havg->inplace->setnantobad->inplace->setbadtoval(0);
  $havg    += $dvals->average;
  $havg    /= 2;
  $havg    += $VAL_NONE;         ##-- convert back to range [-1:1] ($VAL_NONE)
  return $havg;
}

sub nthRoot {
  my ($p,$n) = @_;
  my $root   = $p->abs;
  $root    **= (1/$n);
  (my $tmp=$root->where($p < 0)) *= -1;
  return $root;
}
#sub diffop_gavg0 { return nthRoot($_[0]*$_[1], 2); }
#sub diffop_gavg  { return diffop_avg(diffop_gavg0(@_),diffop_avg(@_)); }
sub diffop_gavg {
  my $dvals = shift;
  my $gavg  = nthRoot($gavg->prodover, 2);
  $gavg    += $dvals->average;
  $gavg    /= 2;
  return $gavg;
}

sub diffop_lavg {
  my $dvals = shift->qsort;
  my $delta = 1-$dvals->slice("(0),");
  (my $tmp=$delta->where($dvals->slice("(0),") >= 1)) .= 0;
  $dvals += $delta->slice("*1,");
  my $lavg = $dvals->prodover;
  $lavg->inplace->log;
  $lavg /= 2.0;
  $lavg->inplace->exp;
  $lavg -= $delta;
  return $lavg;
}


##==============================================================================
## Footer
1;

__END__
