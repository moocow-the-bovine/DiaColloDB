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
## $prf = CLASS_OR_OBJECT->new($prf1,$prf2,%args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Profile::Diff
##    prf1 => $prf1,     ##-- 1st operand
##    prf2 => $prf2,     ##-- 2nd operand
##    ##-- DiaColloDB::Profile keys
##    label => $label,   ##-- string label (used by Multi; undef for none(default))
##    #N   => $N,         ##-- OVERRIDE:unused: total marginal relation frequency
##    #f1  => $f1,        ##-- OVERRIDE:unused: total marginal frequency of target word(s)
##    #f2  => \%f2,       ##-- OVERRIDE:unused: total marginal frequency of collocates: ($i2=>$f2, ...)
##    #f12 => \%f12,      ##-- OVERRIDE:unused: collocation frequencies, %f12 = ($i2=>$f12, ...)
##    #
##    eps => $eps,       ##-- smoothing constant (default=undef: no smoothing)
##    score => $func,    ##-- selected scoring function ('f12', 'mi', or 'ld')
##    mi => \%mi12,      ##-- DIFFERENCE: score: mutual information * logFreq a la Wortprofil; requires compile_mi()
##    ld => \%ld12,      ##-- DIFFERENCE: score: log-dice a la Wortprofil; requires compile_ld()
##    fm => \%fm12,      ##-- DIFFERENCE: score: frequency per million; requires compile_fm()
##   )
sub new {
  my $that = shift;
  my $prf1 = UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile') ? shift : undef;
  my $prf2 = UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile') ? shift : undef;
  my %opts = @_;
  my $dprf = $that->SUPER::new(
			       prf1=>$prf1,
			       prf2=>$prf2,
			       %opts,
			      );
  delete @$dprf{grep {!defined($opts{$_})} qw(N f1 f2 f12)};
  return $dprf->populate() if ($dprf->{prf1} && $dprf->{prf2});
  return $dprf;
}

## $dprf2 = $dprf->clone()
## $dprf2 = $dprf->clone($keep_compiled)
##  + clones %$dprf
##  + if $keep_score is true, compiled data is cloned too
sub clone {
  my ($dprf,$force) = @_;
  return bless({
		label=>$dprf->{label},
		(defined($dprf->{prf1}) ? $dprf->{prf1}->clone($force) : qw()),
		(defined($dprf->{prf2}) ? $dprf->{prf2}->clone($force) : qw()),
	       }, ref($dprf));
}

## ($prf1,$prf2) = $dprf->operands();
sub operands {
  return @{$_[0]}{qw(prf1 prf2)};
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: JSON
##  + mostly INHERITED from DiaCollocDB::Persistent

## $obj = $CLASS_OR_OBJECT->loadJsonData( $data,%opts)
##  + guts for loadJsonString(), loadJsonFile()
sub loadJsonData {
  my $that = shift;
  my $dprf = $that->DiaColloDB::Persistent::loadJsonData(@_);
  bless($_,'DiaColloDB::Profile') foreach (grep {defined($_)} @$dprf{qw(prf1 prf2)});
  return $dprf;
}

##--------------------------------------------------------------
## I/O: Text

## $bool = $prf->saveTextFh($fh, %opts)
##  + %opts:
##    (
##     label => $label,   ##-- override $prf->{label} (used by Profile::Multi), no tab-separators required
##     format => $fmt,      ##-- printf score formatting (default="%.4f")
##    )
##  + format (flat, TAB-separated): Na Nb F1a F1b F2a F2b F12a F12b SCOREa SCOREb SCOREdiff LABEL ITEM2
sub saveTextFh {
  my ($dprf,$fh,%opts) = @_;
  binmode($fh,':utf8');

  my ($pa,$pb,$fscore) = @$dprf{qw(prf1 prf2 score)};
  $fscore //= 'f12';
  my ($Na,$f1a,$f2a,$f12a,$scorea) = @$pa{qw(N f1 f2 f12),$fscore};
  my ($Nb,$f1b,$f2b,$f12b,$scoreb) = @$pb{qw(N f1 f2 f12),$fscore};
  my $scored = $dprf->{$fscore};
  my $label = exists($opts{label}) ? $opts{label} : $dprf->{label};
  my $fmt   = $opts{fmt} || '%f';
  foreach (sort {$scored->{$b} <=> $scored->{$a}} keys %$scored) {
    $fh->print(join("\t",
		    $Na, $Nb,
		    $f1a,$f1b,
		    $f2a->{$_}, $f2b->{$_},
		    $f12a->{$_}, $f2b->{$_},
		    sprintf($fmt,$scorea->{$_}//'nan'),
		    sprintf($fmt,$scoreb->{$_}//'nan'),
		    sprintf($fmt,$scored->{$_}//'nan'),
		    (defined($label) ? $label : qw()),
		    $_),
	       "\n");
  }
  return $dprf;
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
##     format => $fmt,      ##-- printf score formatting (default="%.4f")
##    )
##  + saves rows of the format "SCOREa SCOREb DIFF PREFIX? ITEM2"
sub saveHtmlFile {
  my ($dprf,$file,%opts) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $dprf->logconfess("saveHtmlFile(): failed to open '$file': $!") if (!ref($fh));
  binmode($fh,':utf8');

  $fh->print("<html><body>\n") if ($opts{body}//1);
  $fh->print("<table><tbody>\n") if ($opts{table}//1);
  $fh->print("<tr>",(
		     map {"<th>".htmlesc($_)."</th>"}
		     qw(ascore bscore diff),
		     (defined($opts{hlabel}) ? $opts{hlabel} : qw()),
		     qw(item2)
		    ),
	     "</tr>\n"
	    ) if ($opts{header}//1);

  my ($pa,$pb,$fscore) = @$dprf{qw(prf1 prf2 score)};
  $fscore //= 'f12';
  my $scorea = $pa->{$fscore};
  my $scoreb = $pb->{$fscore};
  my $scored = $dprf->{$fscore};
  my $fmt    = $opts{format} || "%.4f";
  my $label  = exists($opts{label}) ? $opts{label} : $dprf->{label};
  foreach (sort {$scored->{$b} <=> $scored->{$a}} keys %$scored) {
    $fh->print("<tr>", (map {"<td>".htmlesc($_)."</td>"}
			sprintf($fmt,$scorea->{$_}//'nan'),
			sprintf($fmt,$scoreb->{$_}//'nan'),
			sprintf($fmt,$scored->{$_}//'nan'),
			(defined($label) ? $label : qw()),
			$_),
	       "</tr>\n");
  }
  $fh->print("</tbody><table>\n") if ($opts{table}//1);
  $fh->print("</body></html>\n") if ($opts{body}//1);
  $fh->close() if (!ref($file));
  return $dprf;
}


##==============================================================================
## Compilation

## $dprf = $dprf->populate()
## $dprf = $dprf->populate($prf1,$prf2)
##  + populates diff-profile by subtracting $prf2 scores from $prf1
sub populate {
  my ($dprf,$pa,$pb) = @_;
  $pa = $dprf->{prf1} = ($pa // $dprf->{prf1});
  $pb = $dprf->{prf2} = ($pb // $dprf->{prf2});
  $dprf->{label} //= $pa->label() ."-" . $pb->label();

  my $scoref = $dprf->{score} = $dprf->{score} // $pa->{score} // $pb->{score} // 'f12';
  my ($af2,$af12,$ascore) = @$pa{qw(f2 f12),$scoref};
  my ($bf2,$bf12,$bscore) = @$pb{qw(f2 f12),$scoref};
  my $dscore              = $dprf->{$scoref} = ($dprf->{$scoref} // {});
  $dprf->logconfess("populate(): no {$scoref} key for \$pa") if (!$ascore);
  $dprf->logconfess("populate(): no {$scoref} key for \$pb") if (!$bscore);
  foreach (keys %$bscore) {
    $af2->{$_}    //= 0;
    $af12->{$_}   //= 0;
    $ascore->{$_} //= 0;
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
  $dprf->{prf1}->compile($func,@_) or return undef;
  $dprf->{prf2}->compile($func,@_) or return undef;
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
## Trimming

## \@keys = $prf->which(%opts)
##  + returns 'good' keys for trimming options %opts:
##    (
##     cutoff => $cutoff,  ##-- retain only items with $prf->{$prf->{score}}{$item} >= $cutoff
##     kbest  => $kbest,   ##-- retain only $kbest items
##     kbesta => $kbesta,  ##-- retain only $kbest items (absolute value)
##     return => $which,   ##-- either 'good' (default) or 'bad'
##     as     => $as,      ##-- 'hash' or 'array'; default='array'
##    )
##  + INHERITED from DiaColloDB::Profile

## $dprf = $dprf->trim(%opts)
##  + %opts:
##    (
##     kbest => $kbest,    ##-- retain only $kbest items (by score value)
##     kbesta => $kbesta,  ##-- retain only $kbest items (by score absolute value)
##     cutoff => $cutoff,  ##-- retain only items with $prf->{$prf->{score}}{$item} >= $cutoff
##     keep => $keep,      ##-- retain keys @$keep (ARRAY) or keys(%$keep) (HASH)
##     drop => $drop,      ##-- drop keys @$drop (ARRAY) or keys(%$drop) (HASH)
##    )
sub trim {
  my $dprf = shift;
  $dprf->SUPER::trim(@_) or return undef;
  my $dscore = $dprf->{$dprf->{score}//'f12'};
  $dprf->{prf1}->trim(keep=>$dscore) or return undef if ($dprf->{prf1});
  $dprf->{prf2}->trim(keep=>$dscore) or return undef if ($dprf->{prf2});
  return $dprf;
}


##==============================================================================
## Stringification

## $dprf = $dprf->stringify( $obj)
## $dprf = $dprf->stringify(\@key2str)
## $dprf = $dprf->stringify(\&key2str)
## $dprf = $dprf->stringify(\%key2str)
##  + stringifies profile (destructive) via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
sub stringify {
  my ($dprf,$i2s) = @_;
  $i2s = $dprf->stringify_map($i2s);
  $_->stringify($i2s) or return undef foreach (grep {defined($_)} $dprf->operands);
  return $dprf->SUPER::stringify($i2s);
}

##==============================================================================
## Binary operations

## $dprf = $dprf->_add($dprf2,%opts)
##  + adds $prf2 operatnd frequency data to $prf operands (destructive)
##  + implicitly un-compiles $dprf
##  + %opts:
##     N  => $bool, ##-- whether to add N values (default:true)
##     f1 => $bool, ##-- whether to add f1 values (default:true)
sub _add {
  my ($dpa,$dpb,%opts) = @_;
  $dpa->{prf1}->_add($dpb->{prf1}) if ($dpa->{prf1} && $dpb->{prf1});
  $dpa->{prf2}->_add($dpb->{prf2}) if ($dpa->{prf2} && $dpb->{prf2});
  return $dpa->uncompile();
}

## $dprf3 = $dprf1->add($dprf2,%opts)
##  + returns sum of $dprf1 and $dprf2 operatnd frequency data (destructive)
##  + see _add() method for %opts
##  + INHERITED from DiaColloDB::Profile

## $diff = $prf1->diff($prf2,%opts)
##  + returns score-diff of $prf1 and $prf2 frequency data (destructive)
##  + %opts: see _diff() method
##  + INHERITED but probably useless


##==============================================================================
## Footer
1;

__END__