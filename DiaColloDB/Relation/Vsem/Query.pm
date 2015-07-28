## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem::Query.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model: query hacks

package DiaColloDB::Relation::Vsem::Query;
use DDC::XS;
use PDL;
use PDL::VectorValued;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## Constructors etc.

## $vq = CLASS_OR_OBJECT->new(%args)
## $vq = CLASS_OR_OBJECT->new($cquery)
## + %args, object structure:
##   (
##    ##-- ddc query guts
##    cq => $cquery,      ##-- underlying DDC::XS::CQuery object
##    ti => $ti_pdl,      ##-- selected term-indices (undef: all)
##    ci => $ci_pdl,      ##-- selected cat-indices (undef: all)
##   )
sub new {
  my $that = shift;
  my $cq   = (@_%2)==1 ? shift : undef;
  return bless({
		cq=>$cq,
		ti=>undef,
		ci=>undef,
		@_
	       }, ref($that)||$that);
}

##==============================================================================
## API: evaluation

## $vq_or_undef = $vq->evaluate(%opts)
##  + calls $vq->{cq}->__dcvs_eval($vq,%opts), and optionally $vq->evaluateOptions()
##  + %opts: as for DiaColloDB::profile(), also
##    (
##     coldb => $coldb,   ##-- DiaColloDB context (for enums)
##     vsem  => $vsem,    ##-- DiaColloDB::Relation::Vsem context (for meta-enums)
##    )
sub evaluate {
  my $vq = shift;
  return undef if (!defined($vq->{cq}));
  return undef if (!defined($vq->{cq}->__dcvs_eval($vq,@_)));
  return $vq->evaluateOptions(@_);
  return $vq;
}

## $vq_or_undef = $vq->evaluateOptions(%opts)
##  + merges underlying CQueryOptions restrictions into $vq piddles
##  + %opts: as for DiaColloDB::Relation::Vsem::Query::evaluate()
sub evaluateOptions {
  my $vq = shift;
  return $vq if (!$vq->{cq}->can('getOptions') || !defined(my $qo=$vq->{cq}->getOptions));
  return $qo->__dcvs_eval($vq,@_);
}

## $vq = $vq->restrictByDate($dlo_or_undef, $dhi_or_undef, %opts)
##  + %opts: as for DiaColloDB::Relation::Vsem::Query::evaluate()
sub restrictByDate {
  my ($vq,$dlo,$dhi,%opts) = @_;
  if (defined($dlo) || defined($dhi)) {
    my $c_date = defined($vq->{ci}) ? $opts{vsem}{c2date}->index($vq->{ci}) : $opts{vsem}{c2date};
    my ($c_mask);
    if (defined($dlo)) {
      $c_mask  = ($c_date>=$dlo);
      $c_mask &= ($c_date<=$dhi) if (defined($dhi));
    } else {
      $c_mask  = ($c_date<=$dhi);
    }
    $vq->{ci} = defined($vq->{ci}) ? $vq->{ci}->where($c_mask) : $c_mask->which;
  }
  return $vq;
}

##==============================================================================
## Utils: set operations

## $vq = $vq->_intersect($vq2)
##   + destructive intersection
sub _intersect {
  my ($vq,$vq2) = @_;
  $vq->{ti} = _intersect_p($vq->{ti},$vq2->{ti});
  $vq->{ci} = _intersect_p($vq->{ci},$vq2->{ci});
  return $vq;
}

## $i = _intersect_p($p1,$p2)
##  + intersection of 2 piddles; undef is treated as the universal set
##  + argument piddles MUST be sorted in ascending order
sub _intersect_p {
  return (defined($_[0])
	  ? (defined($_[1])
	     ? v_intersect($_[0],$_[1]) ##-- v_intersect is 1.5-3x faster than PDL::Primitive::intersect()
	     : $_[0])
	  : $_[1]);
}

## $vq = $vq->_union($vq2)
##   + destructive union
sub _union {
  my ($vq,$vq2) = @_;
  $vq->{ti} = _union_p($vq->{ti},$vq2->{ti});
  $vq->{ci} = _union_p($vq->{ci},$vq2->{ci});
  return $vq;
}

## $i = _union_p($p1,$p2)
##  + union of 2 piddles; undef is treated as the universal set
##  + argument piddles MUST be sorted in ascending order
sub _union_p {
  return (defined($_[0])
	  ? (defined($_[1])
	     ? v_union($_[0],$_[1])  ##-- v_union is 1.5-3x faster than PDL::Primitive::setops($a,'OR',$b)
	     : $_[0])
	  : $_[1]);
}



##==============================================================================
## Wrappers: DDC::XS::Object
##  + each supported DDC::XS::CQuery or DDC::XS::CQFilter subclass gets its
##    API extended by method(s):
##      __dcvs_eval($vq,%opts) : evaluate query

BEGIN {
  ##-- enable logging for DDC::XS::Object
  push(@DDC::XS::Object::ISA, 'DiaColloDB::Logger') if (!UNIVERSAL::isa('DDC::XS::Object', 'DiaColloDB::Logger'));
}

##======================================================================
## Wrappers: DDC::XS::CQuery

##----------------------------------------------------------------------
## $vq = $DDC_XS_OBJECT->__dcvs_eval($vq,%opts)
##  + evaluates DDC::XS::Object (CQuery or CQFilter) $cquery
##  + returns a new DiaColloDB::Relation::Vsem::Query object representing the evalution, or undef on failure
##  + %opts: as for DiaColloDB::Relation::Vsem::Query::evaluate()
sub DDC::XS::Object::__dcvs_eval {
  my ($cq,$vq,%opts) = @_;
  $vq->logconfess("unsupported query expression of type ", ref($cq), " (", $cq->toString, ")");
}

##----------------------------------------------------------------------
## $vq_or_undef = $CQToken->__dcvs_init($vq,%opts)
##  + checks+sets $CQToken->IndexName
sub DDC::XS::CQToken::__dcvs_init {
  my ($cq,$vq,%opts) = @_;
  my $aname = $opts{coldb}->attrName($cq->getIndexName || $opts{coldb}{attrs}[0]);
  $vq->logconfess("unsupported token-attribute \`$aname' in ", ref($cq), " expression (", $cq->toString, ")") if (!$opts{coldb}->hasAttr($aname));
  $cq->setIndexName($aname);
  return $vq;
}

## \%adata = $CQToken->__dcvs_attr($vq,%opts)
##   + gets attribute-data for $CQToken->getIndexName()
sub DDC::XS::CQToken::__dcvs_attr {
  my ($cq,$vq,%opts) = @_;
  return $opts{coldb}->attrData([$cq->getIndexName])->[0];
}

##----------------------------------------------------------------------
## $vq = $CQTokExact->__dcvs_eval($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokExact::__dcvs_eval {
  my ($cq,$vq,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->__dcvs_attr($vq,%opts);
  my $ai = $attr->{enum}->s2i($cq->getValue);
  my $wi = pdl(long, $attr->{a2w}->fetch($ai));
  my $ti = $vq->{ti} = $opts{vsem}->w2t($wi);
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQTokInfl->__dcvs_eval($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokInfl::__dcvs_eval {
  my ($cq,$vq,%opts) = @_;
  $vq->logwarn("ignoring non-trivial expansion chain in ", ref($cq), " expression (", $cq->toString, ")")
    if (@{$cq->getExpanders//[]});
  return DDC::XS::CQTokExact::__dcvs_eval(@_);
}

##----------------------------------------------------------------------
## $vq = $CQTokRegex->__dcvs_eval($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokRegex::__dcvs_eval {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_eval(@_) if (ref($cq) ne 'DDC::XS::CQTokRegex');
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->__dcvs_attr($vq,%opts);
  my $ais  = $attr->{enum}->re2i($cq->getValue);
  my $wis  = pdl(long, [map {$attr->{a2w}->fetch($_)} @$ais]);
  my $ti   = $vq->{ti} = $opts{vsem}->w2t($wis);
  $ti      = $ti->where($ti>=0);
  return $vq;
}


##----------------------------------------------------------------------
## $vq = $OBJECT->__dcvs_eval($vq,%opts)
##  + ....

##======================================================================
## Wrappers: DDC::XS::CQFilter

##----------------------------------------------------------------------
## $vq = $CQFilter->__dcvs_ignore($vq,%opts)
##  + convenience wrapper: ignore with warning
sub DDC::XS::CQFilter::__dcvs_ignore {
  my ($cq,$vq) = @_;
  $vq->logwarn("ignoring filter expression of type ", ref($cq), " (", $cq->toString, ")");
  return $vq;
}
BEGIN {
  *DDC::XS::CQFRankSort::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFDateSort::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
  *DDC::XS::CQFSizeSort::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
  *DDC::XS::CQFRandomSort::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFBiblSort::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
  *DDC::XS::CQFContextSort::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasField::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldValue::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldRegex::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldPrefix::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldSuffix::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldInfix::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldSet::__dcvs_eval = \&DDC::XS::CQFilter::__dcvs_ignore;
}

##----------------------------------------------------------------------
## $vq_or_undef = $CQFHasField->__dcvs_init($vq,%opts)
##  + ensures that $CQFHasField->Arg0 is a supported metadata attribute
sub DDC::XS::CQFHasField::__dcvs_init {
  my ($cq,$vq,%opts) = @_;
  my $attr = $cq->getArg0;
  $vq->logconfess("unsupported metadata attribute \`$attr' in ", ref($cq), " expression (", $cq->toString, ")")
    if (!$opts{vsem}->hasMeta($attr));
  #$vq->logconfess("negated filters not yet supported in ", ref($cq), " expression (", $cq->toString, ")")
  #  if ($cq->getNegated);
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldSet->__dcvs_eval_neg($vq,%opts)
##  + honors $cq->getNegated() flag, alters $vq->{ci} if applicable
sub DDC::XS::CQFHasField::__dcvs_eval_neg {
  my ($cq,$vq,%opts) = @_;
  return $vq if (!$cq->getNegated);
  if (!defined($vq->{ci})) {
    ##-- neg(\universe) = \emptyset
    $vq->{ci} = null->long;
  }
  elsif ($vq->{ci}->nelem==0) {
    ##-- neg(\emptyset) = \universe
    delete($vq->{ci});
  }
  else {
    ##-- non-trivial negation
    my $NC = $opts{vsem}{c2date}->nelem;
    ##
    ##-- v_mask: ca. 2.2x faster than v_setdiff
    my $ci_mask = ones(byte,$NC);
    (my $tmp=$ci_mask->index($vq->{ci})) .= 0;
    $vq->{ci} = $ci_mask->which;
    ##
    ##-- v_setdiff: ca. 68% slower than mask
    #my $C  = sequence($vq->{ci}->type, $NC);
    #$vq->{ci} = $C->v_setdiff($vq->{ci});
  }
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldSet->__dcvs_eval_p($vq,%opts)
##  + populates $vq->{ci} from @opts{qw(attrs ais)}
##  + calls $CQFHasFieldValue->__dcvs_eval_neg($vq,%opts)
##  + requires additional %opts:
##    (
##     attr => \%attr,   ##-- attribute data as returned by Vsem::metaAttr()
##     ais  => $ais,     ##-- attribute-value index piddle
##    )
##  + TODO: use a persistent reverse-index here (but first build it in create())
sub DDC::XS::CQFHasField::__dcvs_eval_p {
  my ($cq,$vq,%opts) = @_;
  my ($attr,$ais) = @opts{qw(attr ais)};
  ##
  if ($ais->nelem == 0) {
    ##-- null-value: easy answer
    $vq->{ci} = null->long;
  }
  elsif ($ais->nelem == 1) {
    ##-- single-value: easy answer
    $vq->{ci} = ($attr->{vals}==$ais)->which; ##-- TODO: use reverse-index
  }
  else {
    ##-- multiple target values: harder (TODO: use reverse-index)
    #$vq->{ci} = ($attr->{vals}->slice("*1,")==$ais)->borover->which;
    my $vals     = $attr->{vals};
    my $vals_qsi = $vals->qsorti;
    my $vals_qs  = $vals->index($vals_qsi);
    my $i0       = $ais->vsearch($vals_qs);
    my $i0_mask  = ($vals_qs->index($i0) == $ais);
    $i0          = $i0->where($i0_mask);
    my $ilen     = ($ais->where($i0_mask)+1)->vsearch($vals_qs);
    $ilen       -= $i0;
    $ilen->inplace->lclip(1);
    my $iseq     = $ilen->rldseq($i0);
    $vq->{ci} = $vals_qsi->index($iseq)->qsort;
  }
  return DDC::XS::CQFHasField::__dcvs_eval_neg($cq,$vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldValue->__dcvs_eval($vq,%opts)
##  + populates $vq->{ci}
##  + calls $CQFHasFieldValue->__dcvs_eval_p($vq,%opts,...)
##  + TODO: use a persistent reverse-index here (but first build it in create())
sub DDC::XS::CQFHasFieldValue::__dcvs_eval {
  my ($cq,$vq,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr = $opts{vsem}->metaAttr($cq->getArg0);
  my $ai   = $attr->{enum}->s2i($cq->getArg1);
  return $cq->__dcvs_eval_p($vq,%opts, attr=>$attr, ais=>(defined($ai) ? pdl(long,$ai) : null->long));
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldValue->__dcvs_eval($vq,%opts)
##  + populates $vq->{ci}
sub DDC::XS::CQFHasFieldRegex::__dcvs_eval {
  my ($cq,$vq,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr = $opts{vsem}->metaAttr($cq->getArg0);
  my $ais  = $attr->{enum}->re2i($cq->getArg1);
  return $cq->__dcvs_eval_p($vq,%opts, attr=>$attr, ais=>($ais && @$ais ? pdl(long,$ais) : null->long));
}



##======================================================================
## Wrappers: DDC::XS::CQOptions

##----------------------------------------------------------------------
## $vq = $CQueryOptions->__dcvs_eval($vq,%opts)
##  + $vq is the parent query, which should already have been evaluated
sub DDC::XS::CQueryOptions::__dcvs_eval {
  my ($qo,$vq,%opts) = @_;
  $vq->logwarn("ignoring non-empty #WITHIN clause (".join(',',@{$qo->getWithin}).")") if (@{$qo->getWithin});
  $vq->logwarn("ignoring non-empty subcorpus list (".join(',', @{$qo->getSubcorpora}).")") if (@{$qo->getSubcorpora});
  foreach (@{$qo->getFilters}) {
    $vq->_intersect(ref($vq)->new($_)->evaluate(%opts)) or return undef;
  }
  return $vq;
}



##==============================================================================
## Footer
1;

__END__
