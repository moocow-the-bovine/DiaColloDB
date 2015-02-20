## -*- Mode: CPerl -*-
## File: DiaColloDB::Profile::Diff.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profile diffs


package DiaColloDB::Profile::Diff;
use DiaColloDB::Utils qw(:math :html);
use DiaColloDB::Profile;
use IO::File;
use strict;


##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Profile);

##==============================================================================
## Constructors etc.

## $prf = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Profile::Diff
##    prf1 => $prf1,     ##-- 1st operand
##    prf2 => $prf2,     ##-- 2nd operand
##    ##-- DiaColloDB::Profile keys
##    label => $label,   ##-- string label (used by Multi; undef for none(default))
##    N   => $N,         ##-- DIFFERENCE of total marginal relation frequency
##    f1  => $f1,        ##-- DIFFERENCE of total marginal frequency of target word(s)
##    f2  => \%f2,       ##-- DIFFERENCE total marginal frequency of collocates: ($i2=>$f2, ...)
##    f12 => \%f12,      ##-- DIFFERENCE collocation frequencies, %f12 = ($i2=>$f12, ...)
##    #
##    eps => $eps,       ##-- smoothing constant (default=undef: no smoothing)
##    score => $func,    ##-- selected scoring function ('f12', 'mi', or 'ld')
##    mi => \%mi12,      ##-- DIFFERENCE: score: mutual information * logFreq a la Wortprofil; requires compile_mi()
##    ld => \%ld12,      ##-- DIFFERENCE: score: log-dice a la Wortprofil; requires compile_ld()
##    fm => \%fm12,      ##-- DIFFERENCE: score: frequency per million; requires compile_fm()
##   )
sub new {
  my $that = shift;
  my $dprf = $that->SUPER::new(
			       prf1=>undef,
			       prf2=>undef,
			       @_,
			      );
  $dprf->populate() if ($dprf->{prf1} && $dprf->{prf2});
  return $dprf;
}

## $prf2 = $dprf->clone()
## $prf2 = $dprf->clone($keep_compiled)
##  + clones %$mp
##  + if $keep_score is true, compiled data is cloned too
sub clone {
  my ($dprf,$force) = @_;
  $dprf->logconfess("clone(): not implemented");
}

## ($prf1,$prf2) = $dprf->operands();
sub operands {
  return @{$_[0]}{qw(prf1 prf2)};
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: JSON
##  + INHERITED from DiaCollocDB::Persistent

##--------------------------------------------------------------
## I/O: Text

## $bool = $prf->saveTextFh($fh, %opts)
##  + %opts:
##    (
##     label => $label,   ##-- override $prf->{label} (used by Profile::Multi), no tab-separators required
##    )
##  + format (flat, TAB-separated): N F1 F2 F12 SCORE LABEL ITEM2
##  + TODO
sub saveTextFh {
  my ($dprf,$fh,%opts) = @_;
  $dprf->logconfess("saveTextFh: not yet implemented");
}

##--------------------------------------------------------------
## I/O: HTML

## $bool = $prf->saveHtmlFile($filename_or_handle, %opts)
##  + %opts:
##    (
##     table  => $bool,     ##-- include <table>..</table> ? (default=1)
##     body   => $bool,     ##-- include <html><body>..</html></body> ? (default=1)
##     header => $bool,     ##-- include header-row? (default=1)
##     hlabel => $hlabel,   ##-- prefix header item-cells with $hlabel (used by Profile::Multi), no '<th>..</th>' required
##     label => $label,     ##-- prefix item-cells with $label (used by Profile::Multi), no '<td>..</td>' required
##    )
##  + saves rows of the format "N F1 F2 F12 SCORE PREFIX? ITEM2"
sub saveHtmlFile {
  my ($dprf,$file,%opts) = @_;
  $dprf->logconfess("saveHtmlFile: not yet implemented");
}


##==============================================================================
## Compilation

## $dprf = $dprf->populate()
## $dprf = $dprf->populate($prf1,$prf2)
sub populate {
  my ($dprf,$pa,$pb) = @_;
  $pa = $dprf->{prf1} = ($pa // $dprf->{prf1});
  $pb = $dprf->{prf2} = ($pb // $dprf->{prf2});
  $dprf->{label} = "$pa->{label}-$pb->{label}";

  $dprf->{N} = $pa->{N}-$pb->{N};
  $dprf->{f1} = $pa->{f1}-$pb->{f1};
  my $scoref = $pa->{score} // $pb->{score} // 'f12';
  my ($af2,$af12,$ascore) = @$pa{qw(f2 f12),$scoref};
  my ($bf2,$bf12,$bscore) = @$pb{qw(f2 f12),$scoref};
  my ($df2,$df12,$dscore) = @$dprf{qw(f2 f12),$scoref};
  $dprf->logconfess("populate(): no {$scoref} key for \$pa") if (!$ascore);
  $dprf->logconfess("populate(): no {$scoref} key for \$pb") if (!$bscore);
  $dscore = $dprf->{$scoref} = ($dscore // {});
  foreach (keys %$bscore) {
    $df2->{$_}    = $af2->{$_} - $bf2->{$_};
    $df12->{$_}   = $af12->{$_} - $bf12->{$_};
    $dscore->{$_} = $ascore->{$_} - $bscore->{$_};
  }
  return $dprf;
}


## $dprf = $dprf->compile($func,%opts)
##  + compile for score-function $func, one of qw(f fm mi ld); default='f'
sub compile {
  my ($dprf,$func) = (shift,shift);
  $dprf->logconfess("compile(): cannot compile without operand profiles")
    if (!$dprf->{prf1} || !$dprf->{prf2});
  $_->compile() or return undef foreach (@$dprf{qw(prf1 prf2)});
  $dprf->{prf1}->compile($func,@_);
  $dprf->{prf2}->compile($func,@_);
  $dprf->{score} = $dprf->{prf1}{score};
  return $dprf->populate();
}

## $dprf = $dprf->uncompile()
##  + un-compiles all scores for $dprd
sub uncompile {
  my $dprf = shift;
  $dprf->{prf1}->uncompile() if ($dprf->{prf1});
  $dprf->{prf2}->uncompile() if ($dprf->{prf2});
  return $dprf->SUPER::uncompile();
}

##==============================================================================
## Stringification

## $dprf = $dprf->stringify( $obj)
## $dprf = $dprf->stringify(\@key2str)
## $dprf = $dprf->stringify(\&key2str)
## $dprf = $dprf->stringify(\%key2str)
##  + stringifies profile (destructive) via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
sub stringify {
  my $dprf = shift;
  $_->stringify(@_) or return undef foreach (grep {defined($_)} @{$dprf->operands});
  return $dprf->SUPER::stringify(@_);
}

##==============================================================================
## Algebraic operations

## $prf = $prf->_add($prf2,%opts)
##  + adds $prf2 frequency data to $prf (destructive)
##  + implicitly un-compiles $prf
##  + %opts:
##     N  => $bool, ##-- whether to add N values (default:true)
##     f1 => $bool, ##-- whether to add f1 values (default:true)
##  + INHERITED but probably useless

## $prf3 = $prf1->add($prf2,%opts)
##  + returns sum of $prf1 and $prf2 frequency data (destructive)
##  + see _add() method for %opts
##  + INHERITED but probably useless

## $prf = $prf->_diff($prf2,%opts)
##  + subtracts $prf2 scores from $prf (destructive)
##  + $prf and $prf2 must be compatibly compiled
##  + %opts:
##     N  => $bool, ##-- whether to subtract N values (default:true)
##     f1 => $bool, ##-- whether to subtract f1 values (default:true)
##     f2 => $bool, ##-- whether to subtract f2 values (default:true)
##     f12 => $bool, ##-- whether to subtract f12 values (default:true)
##     score => $bool, ##-- whether to subtract score values (default:true)
##  + INHERITED but probably useless

## $prf3 = $prf1->diff($prf2,%opts)
##  + returns score-diff of $prf1 and $prf2 frequency data (destructive)
##  + %opts: see _diff() method
##  + INHERITED but probably useless


##==============================================================================
## Footer
1;

__END__
