## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem::Query.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model: query hacks

package DiaColloDB::Relation::Vsem::Query;
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
##    cdate   => $cdate ,   ##-- pdl ($NCi)
##    cslice  => $cslice ,  ##-- pdl ($NCi)     : [$cii]    => $c_slice_label
##    slices  => $slices,   ##-- pdl ($NSlices) : [$slicei] => $slice_label    (all slices)
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
## API: compilation

## $vq_or_undef = $vq->compile(%opts)
##  + wraps $vq->compileLocal()->compileDate()->compileSlice()
##  + %opts: as for DiaColloDB::profile(), also
##    (
##     coldb => $coldb,   ##-- DiaColloDB context (for enums)
##     vsem  => $vsem,    ##-- DiaColloDB::Relation::Vsem context (for meta-enums)
##     dlo   => $dlo,     ##-- minimum date (undef or '': no minimum)
##     dhi   => $dhi,     ##-- maximum date (undef or '': no maximum)
##    )
sub compile {
  my $vq = shift;
  return undef if (!defined($vq->compileLocal(@_)));
  return $vq->compileOptions(@_)->compileDate(@_)->compileSlice(@_);
}

## $vq_or_undef = $vq->compileLocal(%opts)
##  + calls $vq->{cq}->__dcvs_compile($vq,%opts), and optionally $vq->compileOptions()
##  + %opts: as for DiaColloDB::profile(), also
##    (
##     coldb => $coldb,   ##-- DiaColloDB context (for enums)
##     vsem  => $vsem,    ##-- DiaColloDB::Relation::Vsem context (for meta-enums)
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
##  + %opts: as for DiaColloDB::Relation::Vsem::Query::compile()
sub compileOptions {
  my $vq = shift;
  return $vq if (!$vq->{cq}->can('getOptions') || !defined(my $qo=$vq->{cq}->getOptions));
  return $qo->__dcvs_compile($vq,@_);
}

## $vq = $vq->compileDate(%opts)
##  + %opts: as for DiaColloDB::Relation::Vsem::Query::compile()
##  + populates $vq->{cdate}, restricts $vq->{ci} if appropriate
sub compileDate {
  my ($vq,%opts) = @_;
  my ($dlo,$dhi) = map {($_//'')} @opts{qw(dlo dhi)};
  my $c_date = $vq->{cdate} //= defined($vq->{ci}) ? $opts{vsem}{c2date}->index($vq->{ci}) : $opts{vsem}{c2date};
  if (($dlo ne '') || ($dhi ne '')) {
    my ($c_mask);
    if ($dlo ne '') {
      $c_mask  = ($c_date>=$dlo);
      $c_mask &= ($c_date<=$dhi) if ($dhi ne '');
    } else {
      $c_mask  = ($c_date<=$dhi);
    }
    $vq->{ci} = defined($vq->{ci}) ? $vq->{ci}->where($c_mask) : $c_mask->which;
  }
  return $vq;
}

## $vq = $vq->compileSlice(%opts)
##  + creates slice-indices over $vq->{ci} according to @opts{qw(vsem slice)}
##  + populates @$vq{qw(cslice slices)}
##  + requires $vq->{cdate}
sub compileSlice {
  my ($vq,%opts) = @_;
  $vq->compileDate(%opts) if (!defined($vq->{cdate}));
  return $vq if (!$opts{slice});

  my $slice = $opts{slice};
  my $cdate = $vq->{cdate};
  my ($cslice);
  if ($opts{slice}==1) {
    $cslice = $cdate;
  } else {
    $cslice  = ($cdate/$slice);
    $cslice *= $slice;
  }
  $vq->{cslice} = $cslice;

  if ($opts{fill}) {
    ##-- fill mode: get all possible slices
    my $dslo = $opts{dslo} // ($slice*int($opts{coldb}{xdmin}/$slice));
    my $dshi = $opts{dshi} // ($slice*int($opts{coldb}{xdmax}/$slice));
    $vq->{slices} = $dslo + sequence(ushort, ($dshi-$dslo)/$slice+1)*$slice;
  } else {
    ##-- no-fill mode: find non-empty slices
    my ($cslicen,$cslicev) = $cslice->qsort->rle;
    $vq->{slices} = $cslicev->where($cslicen>0);
  }

  return $vq;
}

## $ci_or_undef = $vq->sliceCats($sliceVal,%opts)
##  + returns selected category-indices for slice $slicei
##  + requires $vq->compileSlice()
sub sliceCats {
  my ($vq,$sliceVal,%opts) = @_;
  return $vq->{ci} if (!$opts{slice}); ##-- may be undef

  ##-- get slice mask
  my $mask = zeroes(byte,$vq->{cslice}->nelem);
  $vq->{cslice}->eq($sliceVal, $mask, 0);

  ##-- apply slice mask
  return $mask->which if (!defined($vq->{ci}));
  return $vq->{ci}->index($mask->which);
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
##  + returns a new DiaColloDB::Relation::Vsem::Query object representing the evalution, or undef on failure
##  + %opts: as for DiaColloDB::Relation::Vsem::Query::compile()
sub DDC::XS::Object::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $vq->logconfess("unsupported query expression of type ", ref($cq), " (", $cq->toString, ")");
}

##======================================================================
## Wrappers: DDC::XS::CQuery

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
##   + negates $vq if applicable
sub DDC::XS::CQToken::__dcvs_compile_neg {
  my ($cq,$vq,%opts) = @_;
  $vq->{ti} = DiaColloDB::Utils::_negate_p($vq->{ti}, $opts{vsem}->nTerms)
    if ($cq->getNegated xor ($cq->can('getRegexNegated') ? $cq->getRegexNegated : 0));
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQTokExact->__dcvs_compile($vq,%opts)
##  + un-sets $vq->{ci}
##  + cals DDC::XS::CQToken::__dcvs_compile_neg()
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
  my $ti = $vq->{ti} = $opts{vsem}->termIds($attr->{a}, $ai);
  return $cq->__dcvs_compile_neg($vq,%opts);
}

##----------------------------------------------------------------------
## $vq = $CQTokInfl->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokInfl::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  $vq->logwarn("ignoring non-trivial expansion chain in ", ref($cq), " expression (", $cq->toString, ")")
    if (@{$cq->getExpanders//[]});
  return DDC::XS::CQTokExact::__dcvs_compile(@_);
}

##----------------------------------------------------------------------
## $vq = $CQTokRegex->__dcvs_compile($vq,%opts)
##  + should set $vq->{ti}
sub DDC::XS::CQTokRegex::__dcvs_compile {
  my ($cq,$vq,%opts) = @_;
  return DDC::XS::Object::__dcvs_compile(@_) if (ref($cq) ne 'DDC::XS::CQTokRegex');
  $cq->__dcvs_init($vq,%opts);
  my $attr = $cq->__dcvs_attr($vq,%opts);
  my $ais  = $attr->{enum}->re2i($cq->getValue);
  my $ti = $vq->{ti} = $opts{vsem}->termIds($attr->{a}, $ais);
  return $cq->__dcvs_compile_neg($vq,%opts);
}


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
    if (!$opts{vsem}->hasMeta($attr));
  #$vq->logconfess("negated filters not yet supported in ", ref($cq), " expression (", $cq->toString, ")")
  #  if ($cq->getNegated);
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQFHasFieldSet->__dcvs_compile_neg($vq,%opts)
##  + honors $cq->getNegated() flag, alters $vq->{ci} if applicable
sub DDC::XS::CQFHasField::__dcvs_compile_neg {
  my ($cq,$vq,%opts) = @_;
  $vq->{ci} = DiaColloDB::Utils::_negate_p($vq->{ci}, $opts{vsem}->nCats) if ($cq->getNegated);
  return $vq;
}

##----------------------------------------------------------------------
## $vq = $CQFHasField->__dcvs_compile_p($vq,%opts)
##  + populates $vq->{ci} from @opts{qw(attrs ais)}
##  + calls $CQFHasField->__dcvs_compile_neg($vq,%opts)
##  + requires additional %opts:
##    (
##     attr   => \%attr,   ##-- attribute data as returned by Vsem::metaAttr()
##     valids => $valids,  ##-- attribute-value ids
##    )
##  + TODO: use a persistent reverse-index here (but first build it in create())
sub DDC::XS::CQFHasField::__dcvs_compile_p {
  my ($cq,$vq,%opts) = @_;
  my ($attr,$valids) = @opts{qw(attr valids)};
  $vq->{ci} = $opts{vsem}->catIds(@opts{qw(attr valids)});
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
  my $enum  = $opts{vsem}->metaEnum($attr);
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
  my $enum = $opts{vsem}->metaEnum($attr);
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
