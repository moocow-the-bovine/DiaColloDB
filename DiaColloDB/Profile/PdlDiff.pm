## -*- Mode: CPerl -*-
## File: DiaColloDB::Profile::PdlDiff.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, comparison profile: pdl-ized (e.g. for DiaColloDB::Relation::Vsem)

package DiaColloDB::Profile::PdlDiff;
use DiaColloDB::Profile::Pdl;
use DiaColloDB::Profile::Diff;
use DiaColloDB::Utils qw(:pdl :list);
use PDL;
use PDL::VectorValued;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Profile::Pdl DiaColloDB::Profile::Diff);

##==============================================================================
## Constructors etc.

## $dpprf = CLASS_OR_OBJECT->new(%args)
## $dpprf = CLASS_OR_OBJECT->new($pprf1,$pprf2,%args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Profile-like attributes
##    label => $label,       ##-- profile label (default=undef)
##    score => $score,       ##-- output score function name (for toProfile() method)
##    ##
##    ##-- DiaColloDB::Profile::Diff-like attributes
##    prf1 => $pprf1,        ##-- 1st operand: DiaColloDB::Profile::Pdl object
##    prf2 => $pprf1,        ##-- 2nd operand: DiaColloDB::Profile::Pdl object
##    diff  => $diff,        ##-- low-level diff operation name
##    ##
##    ##-- DiaColloDB::Profile::Pdl attributes
##    gkeys => $gkeys,       ##-- pdl ($NGKeys) : group-keys
##    gvals => $gvals,       ##-- pdl ($NGKeys) : group-similarities [-1:1]
##    missing => $missing,   ##-- missing value (default=$VAL_NONE)
##   )
sub new {
  my $that = shift;
  my $prf1 = !defined($_[0]) || UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile::Pdl') ? shift : undef;
  my $prf2 = !defined($_[0]) || UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile::Pdl') ? shift : undef;
  my %opts = @_;
  my $dprf = $that->SUPER::new(
			       prf1=>$prf1,
			       prf2=>$prf2,
			       diff=>'adiff',
			       %opts,
			      );
  return $dprf->populate() if ($dprf->{prf1} && $dprf->{prf2});
  return $dprf;
}

##==============================================================================
## API: Basic Access

## $bool = $dpprf->empty()
##  + returns true iff both operands are empty
sub empty {
  return DiaColloDB::Profile::Diff::empty(@_);
}

BEGIN {
  *toDiff = \&toProfile;
}
## $dprf = $dpprf->toProfile(%opts)
##  + convert to a DiaColloDB::Profile:Diff object
##  + %opts are passed to DiaColloDB::Profile::Diff->new()
##  + additionally, 'score' sets the score-key to use (default=$pprf->{score} or 'vsim')
sub toProfile {
  my ($dpprf,%opts) = @_;

  my $prf1 = defined($dpprf->{prf1}) ? $dpprf->{prf1}->toProfile : undef;
  my $prf2 = defined($dpprf->{prf2}) ? $dpprf->{prf2}->toProfile : undef;

  my ($gkeys,$gvals) = @$dpprf{qw(gkeys gvals)};
  my $score = $opts{score} // $dpprf->{score} // 'vsim';
  my $vals  = {};
  %$vals = (map {($gkeys->at($_)=>$gvals->at($_)) } (0..($gkeys->nelem-1))) if (!$dpprf->empty);
  return DiaColloDB::Profile::Diff->new(label=>$dpprf->{label}, diff=>$dpprf->{diff}, %opts, prf1=>$prf1, prf2=>$prf2, score=>$score, $score=>$vals);
}

##==============================================================================
## API: trimming

##----------------------------------------------------------------------
## \@pprfs = CLASS_OR_OBJECT->trimPairs(\@pprfs,%opts)
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
    if (DiaColloDB::Profile::Diff->diffpretrim($opts{diff})) {
      ##-- pre-trim (global)
      my $keep = _union_p($gpa->gwhich(%opts), $gpb->gwhich(%opts));
      $gpa->gtrim(keep=>$keep);
      $gpb->gtrim(keep=>$keep);
    }

    my $gdiff = $that->diff($gpa,$gpb, diff=>$opts{diff});
    my $keep  = $gdiff->gwhich( DiaColloDB::Profile::Diff->diffkbest($opts{diff})=>$opts{kbest} );
    $_->gtrim(keep=>$keep) foreach (grep {$_} map {@$_} @$ppairs);
  }
  elsif (DiaColloDB::Profile::Diff->diffpretrim($opts{diff})) {
    ##-- (pre-)trim locally
    my ($pa,$pb,$pdiff,$keep);
    foreach (@$ppairs) {
      ($pa,$pb) = @$_;
      $keep = _union_p(($pa ? $pa->gwhich(%opts)->qsort : undef), ($pb ? $pb->gwhich(%opts)->qsort : undef));
      $pa->gtrim(keep=>$keep) if (defined($keep) && $pa);
      $pb->gtrim(keep=>$keep) if (defined($keep) && $pb);
    }
 }

  return $ppairs;
}

##==============================================================================
## Compilation: guts

## $dpprf = $dpprf->populate()
## $dpprf = $dpprf->populate($pprf1,$pprf2)
##  + populates diff-profile by applying the selected diff-operation on aligned operand scores
sub populate {
  my ($dpprf,$pprf1,$pprf2,%opts) = @_;
  $pprf1  //= $dpprf->{prf1};
  $pprf2  //= $dpprf->{prf2};
  $pprf1    = $pprf2->shadow() if (!$pprf1 &&  $pprf2);
  $pprf2    = $pprf1->shadow() if ( $pprf1 && !$pprf2);
  @$dpprf{qw(prf1 prf2)} = ($pprf1,$pprf2);
  $dpprf->{label} = $pprf1->label() . "-" . $pprf2->label();

  ##-- get diff-sub (pdl-ized)
  $dpprf->{diff} //= $dpprf->diffop();
  my $diffsub      = $dpprf->diffsub();

  my ($dkeys,$dvals,$tmp);
  my ($keys1,$keys2) = map {$_->{gkeys} // null->long} ($pprf1,$pprf2);
  my ($vals1,$vals2) = map {$_->{gvals} // null->double} ($pprf1,$pprf2);
  if ($keys1->nelem==$keys2->nelem && all($keys1==$keys2)) {
    ##-- optimize for co-indexed keys
    $dkeys = $keys1;
    $dvals = $vals1->cat($vals2)->xchg(0,1);
  } else {
    ##-- keys are not co-indexable: use vsearch
    #$dkeys     = $keys1->setops('OR',$keys2);
    $dkeys     = _union_p($keys1->qsort, $keys2->qsort);
    my $keys1i = $keys1->vsearch($dkeys);
    my $keys2i = $keys2->vsearch($dkeys);
    my $keys1m = ($dkeys->index($keys1i) == $keys1);
    my $keys2m = ($dkeys->index($keys2i) == $keys2);

    $dvals = zeroes($vals1->type, 2,$dkeys->nelem) + $dpprf->missing;
    ($tmp=$dvals->slice("(0),")->index($keys1i->where($keys1m))) .= $vals1->where($keys1m);
    ($tmp=$dvals->slice("(1),")->index($keys2i->where($keys2m))) .= $vals2->where($keys2m);
  }

  $dpprf->{gkeys} = $dkeys;
  $dpprf->{gvals} = $diffsub->($dvals);
  return $dpprf;
}

##----------------------------------------------------------------------
## $pprf = $pprf->gtrim(%opts)
##  + %opts: as for DiaCollo::Profile::Pdl::gtrim()
##  + also trims sub-profiles
sub gtrim {
  my ($dpprf,%opts) = @_;
  $dpprf->SUPER::gtrim(%opts);
  $dpprf->{prf1}->gtrim(keep=>$dpprf->{gkeys}) if ($dpprf->{prf1});
  $dpprf->{prf2}->gtrim(keep=>$dpprf->{gkeys}) if ($dpprf->{prf2});
  return $dpprf;
}


##----------------------------------------------------------------------
## Compilation: diff-ops

## \&FUNC = $pprf->diffsub()
## \&FUNC = $CLASS_OR_OBJECT->diffsub($opNameOrAlias)
##  + gets low-level binary diff operation for diff-operation $opNameOrAlias (default=$dprf->{diff})
##  + wraps DiaColloDB::Profile::Diff::diffsub
sub diffsub {
  return DiaColloDB::Profile::Diff::diffsub(@_)->($_[0]);
}

##-- diffsubs: called as
## + DIFFSUB_CLOSURE = $dpprf->DIFFSUB()
## + $diff_vals = DIFFSUB_CLOSURE($dvals)
## + $dvals: pdl (2,$NG) : [0]=>$aval, [1]=>$bval
BEGIN { *diffop_adiff = \&diffop_diff; }
sub diffop_diff  { return sub { $_[0]->slice("(0),")-$_[0]->slice("(1),"); }; }
sub diffop_sum   { return sub { $_[0]->sumover; }; }
sub diffop_min   { return sub { $_[0]->minimum; }; }
sub diffop_max   { return sub { $_[0]->maximum; }; }
sub diffop_avg   { return sub { $_[0]->average; }; }

sub diffop_havg {
  my $that = shift;
  return sub {
    my $dvals = $_[0]-$that->missing;   ##-- work on range [0:2] ($MISSING)
    my $havg  = $dvals->prodover;
    $havg    *= 2;
    $havg    /= $dvals->sumover;
    $havg->inplace->setnantobad->inplace->setbadtoval(0);
    $havg    += $dvals->average;
    $havg    /= 2;
    $havg    += $that->missing;         ##-- convert back to range [-1:1] ($MISSING)
    return $havg;
  };
}

## $sqrt = ssqrt($x)
##  + signed square-root is negative wherever $x < 0
sub ssqrt {
  my $x = shift;
  my $sqrt = $x->abs;
  $sqrt->inplace->sqrt;
  $sqrt->where($x < 0) *= -1;
  return $sqrt;
}
sub diffop_gavg {
  return sub {
    my $dvals = shift;
    my $gavg  = ssqrt($dvals->prodover);
    $gavg    += $dvals->average;
    $gavg    /= 2;
    return $gavg;
  };
}

sub diffop_lavg {
  return sub {
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
  };
}


##==============================================================================
## Footer
1;

__END__
