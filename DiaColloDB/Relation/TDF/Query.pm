## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::TDF::Query.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: (term x document) frequency matrix: query hacks
##  + formerly DiaColloDB::Relation::Vsem::Query ("vector-space distributional semantic index")

package DiaColloDB::Relation::TDF::Query;
use DiaColloDB::Utils qw(:pdl);
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
##    ti => $ti_pdl,      ##-- pdl ($NTi) : selected term-indices (undef: all)
##    ci => $ci_pdl,      ##-- pdl ($NCi) : selected cat-indices (undef: all)
##    ##
##    ##-- slice guts
##    #cdate   => $cdate ,   ##-- pdl ($NCi)
##    #cslice  => $cslice ,  ##-- pdl ($NCi)     : [$cii]    => $c_slice_label
##    #slices  => $slices,   ##-- pdl ($NSlices) : [$slicei] => $slice_label    (all slices)
##   )
sub new {
  my $that = shift;
  my $cq   = (@_%2)==1 ? shift : undef;
  return bless({
		cq=>$cq,
		#ti=>undef,
		#ci=>undef,
		@_
	       }, ref($that)||$that);
}

##==============================================================================
## API: compilation

## $vq_or_undef = $vq->compile(%opts)
##  + wraps $vq->compileLocal() #->compileDate() #->compileSlice()
##  + %opts: as for DiaColloDB::profile(), also
##    (
##     coldb => $coldb,   ##-- DiaColloDB context (for enums)
##     tdf   => $tdf,     ##-- DiaColloDB::Relation::TDF context (for meta-enums)
##     #dlo   => $dlo,     ##-- minimum date (undef or '': no minimum)
##     #dhi   => $dhi,     ##-- maximum date (undef or '': no maximum)
##    )
sub compile {
  my $vq = shift;
  return undef if (!defined($vq->compileLocal(@_)));
  return $vq->compileOptions(@_); #->compileDate(@_); #->compileSlice(@_);
}

## $vq_or_undef = $vq->compileLocal(%opts)
##  + calls $vq->{cq}->__dcvs_compile($vq,%opts), and optionally $vq->compileOptions()
##  + %opts: as for DiaColloDB::profile(), also
##    (
##     coldb => $coldb,   ##-- DiaColloDB context (for enums)
##     tdf  => $tdf,    ##-- DiaColloDB::Relation::TDF context (for meta-enums)
##     #dlo   => $dlo,     ##-- minimum date (undef or '': no minimum)
##     #dhi   => $dhi,     ##-- maximum date (undef or '': no maximum)
##    )
sub compileLocal {
  my $vq = shift;
  return undef if (!defined($vq->{cq}));
  return undef if (!defined($vq->{cq}->__dcvs_compile($vq,@_)));
  return $vq->compileOptions(@_);
}

## $vq_or_undef = $vq->compileOptions(%opts)
##  + merges underlying CQueryOptions restrictions into $vq piddles
##  + %opts: as for DiaColloDB::Relation::TDF::Query::compile()
sub compileOptions {
  my $vq = shift;
  return $vq if (!$vq->{cq}->can('getOptions') || !defined(my $qo=$vq->{cq}->getOptions));
  return $qo->__dcvs_compile($vq,@_);
}


##==============================================================================
## Utils: set operations

## $vq = $vq->_intersect($vq2)
##   + destructive intersection
sub _intersect {
  my ($vq,$vq2) = @_;
  $vq->{ti} = DiaColloDB::Utils::_intersect_p($vq->{ti},$vq2->{ti});
  $vq->{ci} = DiaColloDB::Utils::_intersect_p($vq->{ci},$vq2->{ci});
  return $vq;
}

## $vq = $vq->_union($vq2)
##   + destructive union
sub _union {
  my ($vq,$vq2) = @_;
  $vq->{ti} = DiaColloDB::Utils::_union_p($vq->{ti},$vq2->{ti});
  $vq->{ci} = DiaColloDB::Utils::_union_p($vq->{ci},$vq2->{ci});
  return $vq;
}


##==============================================================================
## Wrappers: DDC::XS::Object
##  + each supported DDC::XS::CQuery or DDC::XS::CQFilter subclass gets its
##    API extended by method(s):
##      __dcvs_compile($vq,%opts) : compile query

BEGIN {
  ##-- enable logging for DDC::XS::Object
  push(@DDC::XS::Object::ISA, 'DiaColloDB::Logger') if (!UNIVERSAL::isa('DDC::XS::Object', 'DiaColloDB::Logger'));
}

##----------------------------------------------------------------------
## $vq = $DDC_XS_OBJECT->__dcvs_compile($vq,%opts)
##  + compiles DDC::XS::Object (CQuery or CQFilter) $cquery
##  + returns a new DiaColloDB::Relation::TDF::Query object representing the evalution, or undef on failure
##  + %opts: as for DiaColloDB::Relation::TDF::Query::compile()
sub DDC::XS::Object::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $vq->logconfess("unsupported query expression of type ", ref($cq), " (", $cq->toString, ")");
}

##======================================================================
## Wrappers: DDC::XS::CQuery: CQToken

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

## \%adata = $CQToken->__dcvs_compile_neg($vq,%opts)
##   + negates $vq if applicable ({ti} only)
sub DDC::XS::CQToken::__dcvs_compile_neg {
  my ($cq,$vq,%opts) = @_;
  $vq->{ti} = DiaColloDB::Utils::_negate_p($vq->{ti}, $opts{tdf}->nTerms)
    if ($cq->getNegated xor ($cq->can('getRegexNegated') ? $cq->getRegexNegated : 0));
  return $vq;
}

## $vq = $CQToken->__dcvs_compile_re($vq,$regex,%opts)
sub DDC::XS::CQToken::__dcvs_compile_re {
  my ($cq,$vq,$regex,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->__dcvs_attr($vq,%opts);
  my $ais  = $attr->{enum}->re2i($regex);
  my $ti = $vq->{ti} = $opts{tdf}->termIds($attr->{a}, $ais);
  return $cq->__dcvs_compile_neg($vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokExact->__dcvs_compile($vq,%opts)
##  + cals DDC::XS::CQToken::__dcvs_compile_neg()
##  + basically a no-op
sub DDC::XS::CQTokAny::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->__dcvs_attr($vq,%opts);    ##-- ensure valid attribute (even though we don't really need it)
  return $cq->__dcvs_compile_neg($vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokExact->__dcvs_compile($vq,%opts)
##  + sets $vq->{ti}
##  + cals DDC::XS::CQToken::__dcvs_compile_neg()
sub DDC::XS::CQTokExact::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->__dcvs_attr($vq,%opts);
  my $ai = $attr->{enum}->s2i($cq->getValue);
  my $ti = $vq->{ti} = $opts{tdf}->termIds($attr->{a}, $ai);
  return $cq->__dcvs_compile_neg($vq,%opts);
}


##----------------------------------------------------------------------
## $vq = $CQTokInfl->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokInfl::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile(@_) if (ref($cq) ne 'DDC::XS::CQTokInfl');
  $vq->logwarn("ignoring non-trivial expansion chain in ", ref($cq), " expression (", $cq->toString, ")")
    if (@{$cq->getExpanders//[]});
  return DDC::XS::CQTokExact::__dcvs_compile(@_);
}

##----------------------------------------------------------------------
## $vq = $CQTokSet->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokSet::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile(@_) if (ref($cq) !~ /^DDC::XS::CQTokSet(?:Infl)?$/);
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->__dcvs_attr($vq,%opts);
  my $enum = $attr->{enum};
  my $ais  = [map {$enum->s2i($_)} @{$cq->getValues}];
  my $ti   = $vq->{ti} = $opts{tdf}->termIds($attr->{a}, $ais);
  return $cq->__dcvs_compile_neg($vq,%opts);
}


##----------------------------------------------------------------------
## $vq = $CQTokSetInfl->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokSetInfl::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $vq->logwarn("ignoring non-trivial expansion chain in ", ref($cq), " expression (", $cq->toString, ")")
    if (@{$cq->getExpanders//[]});
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokSetInfl');
  return $cq->DDC::XS::CQTokSet::__dcvs_compile($vq,@_);
}


##----------------------------------------------------------------------
## $vq = $CQTokRegex->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokRegex::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokRegex');
  return $cq->__dcvs_compile_re($vq,$cq->getValue,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokPrefix->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokPrefix::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokPrefix');
  return $cq->__dcvs_compile_re($vq,('^'.quotemeta($cq->getValue)),%opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokSuffix->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokSuffix::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokSuffix');
  return $cq->__dcvs_compile_re($vq,(quotemeta($cq->getValue).'$'),%opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokInfix->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokInfix::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokInfix');
  return $cq->__dcvs_compile_re($vq,quotemeta($cq->getValue),%opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokPrefixSet->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokPrefixSet::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokPrefixSet');
  return $cq->__dcvs_compile_re($vq, ('^(?:'.join('|', map {quotemeta($_)} @{$cq->getValues}).')'), %opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokSuffixSet->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokSuffixSet::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokSuffixSet');
  return $cq->__dcvs_compile_re($vq, ('(?:'.join('|', map {quotemeta($_)} @{$cq->getValues}).')$'), %opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokInfixSet->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokInfixSet::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokInfixSet');
  return $cq->__dcvs_compile_re($vq, ('(?:'.join('|', map {quotemeta($_)} @{$cq->getValues}).')'), %opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokLemma->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokLemma::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQTokLemma');
  return DDC::XS::CQTokExact::__dcvs_compile(@_);
}

##======================================================================
## Wrappers: DDC::XS::CQuery: CQNegatable

##----------------------------------------------------------------------
## \%adata = $CQNegatable->__dcvs_compile_neg($vq,%opts)
##   + negates $vq if applicable (both {ti} and {ci})
sub DDC::XS::CQNegatable::__dcvs_compile_neg {
  my ($cq,$vq,%opts) = @_;
  if ($cq->getNegated) {
    $vq->{ti} = DiaColloDB::Utils::_negate_p($vq->{ti}, $opts{tdf}->nTerms);
    $vq->{ci} = DiaColloDB::Utils::_negate_p($vq->{ci}, $opts{tdf}->nCats);
  }
  return $vq;
}

##======================================================================
## Wrappers: DDC::XS::CQuery: CQBinOp

##----------------------------------------------------------------------
## ($vq1,$vq2) = $CQBinOp->__dcvs_compile_dtrs($vq,%opts)
##  + compiles daughter nodes
sub DDC::XS::CQBinOp::__dcvs_compile_dtrs {
  my ($cq,$vq,%opts) = @_;
  my $dtr1 = $cq->getDtr1;
  my $dtr2 = $cq->getDtr2;
  my $vq1 = ($dtr1 ? $dtr1->__dcvs_compile(ref($vq)->new(), %opts) : ref($vq)->new)
    or $vq->logconfess("failed to compile ", ref($cq), " sub-query expression (", $dtr1->toString, ")");
  my $vq2 = ($dtr2 ? $dtr2->__dcvs_compile(ref($vq)->new(), %opts) : ref($vq)->new)
      or $vq->logconfess("failed to compile ", ref($cq), " sub-query expression (", $dtr2->toString, ")");
  return ($vq1,$vq2);
}

##----------------------------------------------------------------------
## $vq = $CQWith->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti} (term-intersection only)
sub DDC::XS::CQWith::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQWith');
  my ($vq1,$vq2) = $cq->__dcvs_compile_dtrs($vq,%opts);
  $vq->{ti} = DiaColloDB::Utils::_intersect_p($vq1->{ti},$vq2->{ti});
  return DDC::XS::CQToken::__dcvs_compile_neg($cq,$vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQWith->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti} (term-difference only)
sub DDC::XS::CQWithout::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQWithout');
  my ($vq1,$vq2) = $cq->__dcvs_compile_dtrs($vq,%opts);
  $vq->{ti} = DiaColloDB::Utils::_setdiff_p($vq1->{ti},$vq2->{ti},$opts{tdf}->nTerms);
  return DDC::XS::CQToken::__dcvs_compile_neg($cq,$vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQWith->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}, $vq->{ci} (cat-intersection, term-union)
##  + TODO: fix this to use minimum tdm0 frequency of compiled daughters
##    - requires generalizing Query.pm to allow explicit frequencies (full tdm?) -- LATER!
sub DDC::XS::CQAnd::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQAnd');
  my ($vq1,$vq2) = $cq->__dcvs_compile_dtrs($vq,%opts);
  $vq->{ti} = DiaColloDB::Utils::_union_p($vq1->{ti}, $vq2->{ti});
  $vq->{ci} = DiaColloDB::Utils::_intersect_p($opts{tdf}->catSubset($vq1->{ti}, $vq1->{ci}),
					      $opts{tdf}->catSubset($vq2->{ti}, $vq2->{ci}));
  return $cq->__dcvs_compile_neg($vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQWith->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}, $vq->{ci} (cat-union, term-union)
sub DDC::XS::CQOr::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile($vq,@_) if (ref($cq) ne 'DDC::XS::CQOr');
  my ($vq1,$vq2) = $cq->__dcvs_compile_dtrs($vq,%opts);
  $vq->{ti} = DiaColloDB::Utils::_union_p($vq1->{ti}, $vq2->{ti});
  $vq->{ci} = DiaColloDB::Utils::_union_p($vq1->{ci}, $vq2->{ci});
  return $cq->__dcvs_compile_neg($vq,%opts);
}



##======================================================================
## Wrappers: DDC::XS::CQuery: TODO

## DDC::XS::CQuery : @ISA=qw(DDC::XS::Object)
# DDC::XS::CQNegatable : @ISA=qw(DDC::XS::CQuery)
# DDC::XS::CQAtomic : @ISA=qw(DDC::XS::CQNegatable)
# DDC::XS::CQBinOp : @ISA=qw(DDC::XS::CQNegatable)
## DDC::XS::CQAnd : @ISA=qw(DDC::XS::CQBinOp)
## DDC::XS::CQOr : @ISA=qw(DDC::XS::CQBinOp)
## DDC::XS::CQWith : @ISA=qw(DDC::XS::CQBinOp)
## DDC::XS::CQWithout : @ISA=qw(DDC::XS::CQWith)
## DDC::XS::CQToken : @ISA=qw(DDC::XS::CQAtomic)
## DDC::XS::CQTokExact : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokAny : @ISA=qw(DDC::XS::CQToken)
# DDC::XS::CQTokAnchor : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokRegex : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokSet : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokInfl : @ISA=qw(DDC::XS::CQTokSet)
## DDC::XS::CQTokSetInfl : @ISA=qw(DDC::XS::CQTokInfl)
## DDC::XS::CQTokPrefix : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokSuffix : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokInfix : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokPrefixSet : @ISA=qw(DDC::XS::CQTokSet)
## DDC::XS::CQTokSuffixSet : @ISA=qw(DDC::XS::CQTokSet)
## DDC::XS::CQTokInfixSet : @ISA=qw(DDC::XS::CQTokSet)
# DDC::XS::CQTokMorph : @ISA=qw(DDC::XS::CQToken)
## DDC::XS::CQTokLemma : @ISA=qw(DDC::XS::CQTokMorph)
# DDC::XS::CQTokThes : @ISA=qw(DDC::XS::CQToken)
# DDC::XS::CQTokChunk : @ISA=qw(DDC::XS::CQToken)
# DDC::XS::CQTokFile : @ISA=qw(DDC::XS::CQToken)
# DDC::XS::CQNear : @ISA=qw(DDC::XS::CQNegatable)
# DDC::XS::CQSeq : @ISA=qw(DDC::XS::CQAtomic)

##----------------------------------------------------------------------
## $vq = $OBJECT->__dcvs_compile($vq,%opts)
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
  *DDC::XS::CQFRankSort::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFDateSort::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
  *DDC::XS::CQFSizeSort::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
  *DDC::XS::CQFRandomSort::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFBiblSort::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
  *DDC::XS::CQFContextSort::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasField::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldValue::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldRegex::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldPrefix::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldSuffix::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldInfix::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
#  *DDC::XS::CQFHasFieldSet::__dcvs_compile = \&DDC::XS::CQFilter::__dcvs_ignore;
}

##----------------------------------------------------------------------
## $vq_or_undef = $CQFHasField->__dcvs_init($vq,%opts)
##  + ensures that $CQFHasField->Arg0 is a supported metadata attribute
sub DDC::XS::CQFHasField::__dcvs_init {
  my ($cq,$vq,%opts) = @_;
  my $attr = $cq->getArg0;
  $vq->logconfess("unsupported metadata attribute \`$attr' in ", ref($cq), " expression (", $cq->toString, ")")
    if (!$opts{tdf}->hasMeta($attr));
  #$vq->logconfess("negated filters not yet supported in ", ref($cq), " expression (", $cq->toString, ")")
  #  if ($cq->getNegated);
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldSet->__dcvs_compile_neg($vq,%opts)
##  + honors $cq->getNegated() flag, alters $vq->{ci} if applicable
sub DDC::XS::CQFHasField::__dcvs_compile_neg {
  my ($cq,$vq,%opts) = @_;
  $vq->{ci} = DiaColloDB::Utils::_negate_p($vq->{ci}, $opts{tdf}->nCats) if ($cq->getNegated);
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQFHasField->__dcvs_compile_p($vq,%opts)
##  + populates $vq->{ci} from @opts{qw(attrs ais)}
##  + calls $CQFHasField->__dcvs_compile_neg($vq,%opts)
##  + requires additional %opts:
##    (
##     attr   => \%attr,   ##-- attribute data as returned by TDF::metaAttr()
##     valids => $valids,  ##-- attribute-value ids
##    )
##  + TODO: use a persistent reverse-index here (but first build it in TDF::create())
sub DDC::XS::CQFHasField::__dcvs_compile_p {
  my ($cq,$vq,%opts) = @_;
  my ($attr,$valids) = @opts{qw(attr valids)};
  $vq->{ci} = $opts{tdf}->catIds(@opts{qw(attr valids)});
  return DDC::XS::CQFHasField::__dcvs_compile_neg($cq,$vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldValue->__dcvs_compile($vq,%opts)
##  + populates $vq->{ci}
##  + calls $CQFHasFieldValue->__dcvs_compile_p($vq,%opts,...)
##  + TODO: use a persistent reverse-index here (but first build it in create())
sub DDC::XS::CQFHasFieldValue::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr  = $cq->getArg0;
  my $enum  = $opts{tdf}->metaEnum($attr);
  my $vals  = $enum->s2i($cq->getArg1);
  return $cq->__dcvs_compile_p($vq, %opts, attr=>$attr, valids=>$vals);
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldValue->__dcvs_compile($vq,%opts)
##  + populates $vq->{ci}
sub DDC::XS::CQFHasFieldRegex::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->getArg0;
  my $enum = $opts{tdf}->metaEnum($attr);
  my $vals = $enum->re2i($cq->getArg1);
  return $cq->__dcvs_compile_p($vq,%opts, attr=>$attr, valids=>$vals);
}



##======================================================================
## Wrappers: DDC::XS::CQOptions

##----------------------------------------------------------------------
## $vq = $CQueryOptions->__dcvs_compile($vq,%opts)
##  + $vq is the parent query, which should already have been compiled
sub DDC::XS::CQueryOptions::__dcvs_compile {
  my ($qo,$vq,%opts) = @_;
  $vq->logwarn("ignoring non-empty #WITHIN clause (".join(',',@{$qo->getWithin}).")") if (@{$qo->getWithin});
  $vq->logwarn("ignoring non-empty subcorpus list (".join(',', @{$qo->getSubcorpora}).")") if (@{$qo->getSubcorpora});
  foreach (@{$qo->getFilters}) {
    $vq->_intersect(ref($vq)->new($_)->compileLocal(%opts)) or return undef;
  }
  return $vq;
}



##==============================================================================
## Footer
1;

__END__
