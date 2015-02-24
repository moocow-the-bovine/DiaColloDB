## -*- Mode: CPerl -*-
## File: DiaColloDB::Profile.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, (co-)frequency profile
##  + for scoring heuristics, see:
##
##    - Jörg Didakowski; Alexander Geyken, 2013. From DWDS corpora to a German Word Profile – methodological problems and solutions.
##      In: Network Strategies, Access Structures and Automatic Extraction of Lexicographical Information. 2nd Work Report of the
##      Academic Network "Internet Lexicography". Mannheim: Institut für Deutsche Sprache. (OPAL - Online publizierte Arbeiten zur
##      Linguistik X/2012), S. 43-52.
##      URL http://www.dwds.de/static/website/publications/pdf/didakowski_geyken_internetlexikografie_2012_final.pdf
##
##    - Rychlý, P. 2008. `A lexicographer-friendly association score'. In P. Sojka and A. Horák (eds.) Proceedings of Recent
##      Advances in Slavonic Natural Language Processing. RASLAN 2008, 6­9.
##      URL http://www.muni.cz/research/publications/937193 , http://www.fi.muni.cz/usr/sojka/download/raslan2008/13.pdf
##
##    - Kilgarriff, A. and Tugwell, D. 2002. `Sketching words'. In M.-H. Corréard (ed.) Lexicography and Natural
##      Language Processing: A Festschrift in Honour of B. T. S. Atkins. EURALEX, 125-137.
##      URL http://www.kilgarriff.co.uk/Publications/2002-KilgTugwell-AtkinsFest.pdf


package DiaColloDB::Profile;
use DiaColloDB::Utils qw(:math :html);
use DiaColloDB::Persistent;
use DiaColloDB::Profile::Diff;
use IO::File;
use strict;

#use overload
#  #fallback => 0,
#  bool => sub {defined($_[0])},
#  int => sub {$_[0]{N}},
#  '+' => \&add,
#  '+=' => \&_add,
#  '-' => \&diff,
#  '-=' => \&_diff;


##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Persistent);

##==============================================================================
## Constructors etc.

## $prf = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    label => $label,   ##-- string label (used by Multi; undef for none(default))
##    N   => $N,         ##-- total marginal relation frequency
##    f1  => $f1,        ##-- total marginal frequency of target word(s)
##    f2  => \%f2,       ##-- total marginal frequency of collocates: ($i2=>$f2, ...)
##    f12 => \%f12,      ##-- collocation frequencies, %f12 = ($i2=>$f12, ...)
##    #
##    eps => $eps,       ##-- smoothing constant (default=undef: no smoothing)
##    score => $func,    ##-- selected scoring function ('f12', 'mi', or 'ld')
##    mi => \%mi12,      ##-- score: mutual information * logFreq a la Wortprofil; requires compile_mi()
##    ld => \%ld12,      ##-- score: log-dice a la Wortprofil; requires compile_ld()
##    fm => \%fm12,      ##-- frequency per million score; requires compile_fm()
##   )
sub new {
  my $that = shift;
  my $prf  = bless({
		    #label=>undef,
		    N=>1,
		    f1=>0,
		    f2=>{},
		    f12=>{},
		    #eps=>0,
		    #mi=>{},
		    #ld=>{},
		    #fm=>{},
		    @_
		   }, (ref($that)||$that));
  return $prf;
}

## $label = $prf->label()
##  + get label
sub label {
  return $_[0]{label} // '';
}

## @keys = $prf->scoreKeys()
##  + returns known score function keys
sub scoreKeys {
  return qw(mi ld fm);
}

## $prf2 = $prf->clone()
## $prf2 = $prf->clone($keep_compiled)
##  + clones %$mp
##  + if $keep_score is true, compiled data is cloned too
sub clone {
  my ($prf,$force) = @_;
  return bless({
		label=>$prf->{label},
		N=>$prf->{N},
		f1=>$prf->{f1},
		f2=>{ %{$prf->{f2}} },
		f12=>{ %{$prf->{f12}} },
		(exists($prf->{eps}) ? (eps=>$prf->{eps}) : qw()),
		($force
		 ? (
		    ($prf->{score} ? (score=>$prf->{score}) : qw()),
		    (map {$prf->{$_} ? ($_=>{%{$prf->{$_}}}) : qw()} $prf->scoreKeys),
		   )
		 : qw()),
	       }, ref($prf));
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
##     format => $fmt,    ##-- printf format for scores (default="%f")
##    )
##  + format (flat, TAB-separated): N F1 F2 F12 SCORE LABEL ITEM2
sub saveTextFh {
  my ($prf,$fh,%opts) = @_;
  my $label = (exists($opts{label}) ? $opts{label} : $prf->{label});
  my ($N,$f1,$f2,$f12) = @$prf{qw(N f1 f2 f12)};
  my $fscore = $prf->{$prf->{score}//'f12'};
  my $fmt    = $opts{format} || '%f';
  binmode($fh,':utf8');
  foreach (sort {$fscore->{$b} <=> $fscore->{$a}} keys %$fscore) {
    $fh->print(join("\t",
		    $N,
		    $f1,
		    $f2->{$_},
		    $f12->{$_},
		    sprintf($fmt,$fscore->{$_}//'nan'),
		    (defined($label) ? $label : qw()),
		    $_),
	       "\n");
  }
  return $prf;
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
##  + saves rows of the format "N F1 F2 F12 SCORE PREFIX? ITEM2"
sub saveHtmlFile {
  my ($prf,$file,%opts) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $prf->logconfess("saveHtmlFile(): failed to open '$file': $!") if (!ref($fh));

  $fh->print("<html><body>\n") if ($opts{body}//1);
  $fh->print("<table><tbody>\n") if ($opts{table}//1);
  $fh->print("<tr>",(
		     map {"<th>".htmlesc($_)."</th>"}
		     qw(N f1 f2 f12 score),
		     (defined($opts{hlabel}) ? $opts{hlabel} : qw()),
		     qw(item2)
		    ),
	     "</tr>\n"
	    ) if ($opts{header}//1);

  my ($N,$f1,$f2,$f12) = @$prf{qw(N f1 f2 f12)};
  my $label = (exists($opts{label}) ? $opts{label} : $prf->{label});
  my $fscore = $prf->{$prf->{score}//'f12'};
  my $fmt   = $opts{format} || "%.4f";
  foreach (sort {$fscore->{$b} <=> $fscore->{$a}} keys %$fscore) {
    $fh->print("<tr>", (map {"<td>".htmlesc($_)."</td>"}
			$N,
			$f1,
			$f2->{$_},
			$f12->{$_},
			sprintf($fmt, $fscore->{$_}//'nan'),
			(defined($label) ? $label : qw()),
			$_
		       ),
	       "</tr>\n");
  }
  $fh->print("</tbody><table>\n") if ($opts{table}//1);
  $fh->print("</body></html>\n") if ($opts{body}//1);
  $fh->close() if (!ref($file));
  return $prf;
}


##==============================================================================
## Compilation

## $prf = $prf->compile($func,%opts)
##  + compile for score-function $func, one of qw(f fm mi ld); default='f'
sub compile {
  my $prf = shift;
  my $func = shift;
  return $prf->compile_f(@_)  if (!$func || $func =~ m{^(?:f(?:req(?:uency)?)?(?:12)?)$}i);
  return $prf->compile_fm(@_) if ($func =~ m{^(?:f(?:req(?:uency)?)?(?:-?p(?:er)?)?(?:-?m(?:(?:ill)?ion)?)(?:12)?)$}i);
  return $prf->compile_ld(@_) if ($func =~ m{^(?:ld|log-?dice)}i);
  return $prf->compile_mi(@_) if ($func =~ m{^(?:l?f?mi|mutual-?information)$}i);
  $prf->logwarn("compile(): unknown score function '$func'");
  return $prf->compile_f(@_);
}

## $prf = $prf->uncompile()
##  + un-compiles all scores for $prf
sub uncompile {
  delete @{$_[0]}{$_[0]->scoreKeys,'score'};
  return $_[0];
}

## $prf = $prf->compile_f()
##  + just sets $prf->{score} = 'f12'
sub compile_f {
  $_[0]{score} = 'f12';
  return $_[0];
}

## $prf = $prf->compile_fm()
##  + computes frequency-per-million in $prf->{fm}
##  + sets $prf->{score}='fm'
sub compile_fm {
  my $prf = shift;
  my ($N,$pf12) = @$prf{qw(N f12)};
  my $fm = $prf->{fm} = {};
  my ($i2,$f12);
  while (($i2,$f12)=each(%$pf12)) {
    $fm->{$i2} = (1000000 * $f12) / $N;
  }
  $prf->{score} = 'fm';
  return $prf;
}

## $prf = $prf->compile_mi(%opts)
##  + computes MI*logF-profile in $prf->{mi} a la Rychly (2008)
##  + sets $prf->{score}='mi'
##  + %opts:
##     eps => $eps  #-- clobber $prf->{eps}
sub compile_mi {
  my ($prf,%opts) = @_;
  my ($N,$f1,$pf2,$pf12) = @$prf{qw(N f1 f2 f12)};
  my $mi = $prf->{mi} = {};
  my $eps = $opts{eps} // $prf->{eps} // 0; #0.5;
  my ($i2,$f2,$f12);
  while (($i2,$f2)=each(%$pf2)) {
    $f12 = $pf12->{$i2} // 0;
    $mi->{$i2} = (
		  log2($f12+$eps)
		  *
		  log2(
		       (($f12+$eps)*($N+$eps))
		       /
		       (($f1+$eps)*($f2+$eps))
		      )
		 );

  }
  $prf->{score} = 'mi';
  return $prf;
}

## $prf = $prf->compile_ld(%opts)
##  + computes log-dice profile in $prf->{ld} a la Rychly (2008)
##  + sets $pf->{score}='ld'
##  + %opts:
##     eps => $eps  #-- clobber $prf->{eps}
sub compile_ld {
  my ($prf,%opts) = @_;
  my ($N,$f1,$pf2,$pf12) = @$prf{qw(N f1 f2 f12)};
  my $ld = $prf->{ld} = {};
  my $eps = $opts{eps} // $prf->{eps} // 0; #0.5;
  my ($i2,$f2,$f12);
  while (($i2,$f2)=each(%$pf2)) {
    $f12 = $pf12->{$i2} // 0;
    $ld->{$i2} = 14 + log2(
			   (2 * ($f12+$eps))
			   /
			   (($f1+$eps) + ($f2+$eps))
			  );
  }
  $prf->{score} = 'ld';
  return $prf;
}

##==============================================================================
## Trimming

## \@keys = $prf->which(%opts)
##  + returns 'good' keys for trimming options %opts:
##    (
##     cutoff => $cutoff,  ##-- retain only items with $prf->{$prf->{score}}{$item} >= $cutoff
##     kbest  => $kbest,   ##-- retain only $kbest items
##     return => $which,   ##-- either 'good' (default) or 'bad'
##     as     => $as,      ##-- 'hash' or 'array'; default='array'
##    )
sub which {
  my ($prf,%opts) = @_;

  ##-- trim: scoring function
  my $score = $prf->{$prf->{score}//'f12'}
    or $prf->logconfess("trim(): no profile scores for '$prf->{score}'");
  my $bad = {};

  ##-- which: by user-specified cutoff
  if ((my $cutoff=$opts{cutoff}//'') ne '') {
    my ($key,$val);
    while (($key,$val) = each %$score) {
      $bad->{$key} = undef if ($val < $cutoff);
    }
  }

  ##-- which: k-best
  my $kbest;
  if (defined($kbest = $opts{kbest}) && $kbest > 0) {
    my @keys = sort {$score->{$b} <=> $score->{$a}} grep {!exists($bad->{$_})} keys %$score;
    if (@keys > $kbest) {
      splice(@keys, 0, $kbest);
      $bad->{$_} = 1 foreach (@keys);
    }
  }

  ##-- which: return
  if (($opts{return}//'') eq 'bad') {
    return lc($opts{as}//'array') eq 'hash' ?  $bad : [keys %$bad];
  }
  return lc($opts{as}//'array') eq 'hash' ? {map {$bad->{$_} ? qw() : ($_=>undef)} keys %$score } : [grep {!$bad->{$_}} keys %$score];
}


## $prf = $prf->trim(%opts)
##  + %opts:
##    (
##     kbest => $kbest,    ##-- retain only $kbest items
##     cutoff => $cutoff,  ##-- retain only items with $prf->{$prf->{score}}{$item} >= $cutoff
##     keep => $keep,      ##-- retain keys @$keep (ARRAY) or keys(%$keep) (HASH)
##     drop => $drop,      ##-- drop keys @$drop (ARRAY) or keys(%$drop) (HASH)
##    )
##  + this COULD be factored out into s.t. like $prf->trim($prf->which(%opts)), but it's about 15% faster inline
sub trim {
  my ($prf,%opts) = @_;

  ##-- trim: scoring function
  my $score = $prf->{$prf->{score}//'f12'}
    or $prf->logconfess("trim(): no profile scores for '$prf->{score}'");

  ##-- trim: by user request: keep
  if (defined($opts{keep})) {
    my $keep = (UNIVERSAL::isa($opts{keep},'ARRAY') ? {map {($_=>undef)} @{$opts{keep}}} : $opts{keep});
    my @trim = grep {!exists($keep->{$_})} keys %$score;
    foreach (grep {defined($prf->{$_})} qw(f2 f12),$prf->scoreKeys) {
      delete @{$prf->{$_}}{@trim};
      $_ //= 0 foreach (@{$prf->{$_}}{keys %$keep});
    }
  }

  ##-- trim: by user request: drop
  if (defined($opts{drop})) {
    my $drop = (UNIVERSAL::isa($opts{drop},'ARRAY') ? $opts{drop} : [keys %{$opts{drop}}]);
    delete @{$prf->{$_}}{@$drop} foreach (grep {defined($prf->{$_})} qw(f2 f12),$prf->scoreKeys);
  }

  ##-- trim: by user-specified cutoff
  if ((my $cutoff=$opts{cutoff}//'') ne '') {
    my @trim = qw();
    my ($key,$val);
    while (($key,$val) = each %$score) {
      push(@trim,$key) if ($val < $cutoff);
    }
    delete @{$prf->{$_}}{@trim} foreach (grep {defined($prf->{$_})} qw(f2 f12),$prf->scoreKeys);
  }

  ##-- trim: k-best
  my $kbest;
  if (defined($kbest = $opts{kbest}) && $kbest > 0) {
    my @trim = sort {$score->{$b} <=> $score->{$a}} keys %$score;
    if (@trim > $kbest) {
      splice(@trim, 0, $kbest);
      delete @{$prf->{$_}}{@trim} foreach (grep {defined($prf->{$_})} qw(f2 f12),$prf->scoreKeys);
    }
  }

  return $prf;
}

##==============================================================================
## Stringification

## $prf = $prf->stringify( $obj)
## $prf = $prf->stringify(\@key2str)
## $prf = $prf->stringify(\&key2str)
## $prf = $prf->stringify(\%key2str)
##  + stringifies profile (destructive) via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
sub stringify {
  my ($prf,$i2s) = @_;
  if (UNIVERSAL::can($i2s,'i2s')) {
    $i2s = { map {($_=>$i2s->i2s($_))} sort {$a<=>$b} keys %{$prf->{f2}} };
  }
  elsif (UNIVERSAL::isa($i2s,'CODE')) {
    $i2s = { map {($_=>$i2s->($_))} sort {$a<=>$b} keys %{$prf->{f2}} };
  }
  ##-- guts
  if (UNIVERSAL::isa($i2s,'HASH')) {
    foreach (grep {defined $prf->{$_}} qw(f2 f12),$prf->scoreKeys) {
      my $sh = {};
      @$sh{@$i2s{keys %{$prf->{$_}}}} = values %{$prf->{$_}};
      $prf->{$_} = $sh;
    }
    return $prf;
  }
  elsif (UNIVERSAL::isa($i2s,'ARRAY')) {
    foreach (grep {defined $prf->{$_}} qw(f2 f12),$prf->scoreKeys) {
      my $sh = {};
      @$sh{@$i2s[keys %{$prf->{$_}}]} = values %{$prf->{$_}};
      $prf->{$_} = $sh;
    }
    return $prf;
  }

  $prf->logconfess("stringify(): don't know how to stringify via object '$i2s'");
}

##==============================================================================
## Algebraic operations

## $prf = $prf->_add($prf2,%opts)
##  + adds $prf2 frequency data to $prf (destructive)
##  + implicitly un-compiles $prf
##  + %opts:
##     N  => $bool, ##-- whether to add N values (default:true)
##     f1 => $bool, ##-- whether to add f1 values (default:true)
sub _add {
  my ($pa,$pb,%opts) = @_;
  $pa->{N}  += $pb->{N}  if (!exists($opts{N}) || $opts{N});
  $pa->{f1} += $pb->{f1} if (!exists($opts{f1}) || $opts{f1});
  my ($af2,$af12) = @$pa{qw(f2 f12)};
  my ($bf2,$bf12) = @$pb{qw(f2 f12)};
  foreach (keys %$bf12) {
    $af2->{$_}  += $bf2->{$_};
    $af12->{$_} += $bf12->{$_};
  }
  return $pa->uncompile();
}

## $prf3 = $prf1->add($prf2,%opts)
##  + returns sum of $prf1 and $prf2 frequency data (destructive)
##  + see _add() method for %opts
sub add {
  return $_[0]->clone->_add(@_[1..$#_]);
}

## $diff = $prf1->diff($prf2,%opts)
##  + wraps DiaColloDB::Profile::Diff->new($prf1,$prf2,%opts)
##  + %opts:
##     N  => $bool, ##-- whether to subtract N values (default:true)
##     f1 => $bool, ##-- whether to subtract f1 values (default:true)
##     f2 => $bool, ##-- whether to subtract f2 values (default:true)
##     f12 => $bool, ##-- whether to subtract f12 values (default:true)
##     score => $bool, ##-- whether to subtract score values (default:true)
sub diff {
  return DiaColloDB::Profile::Diff->new(@_);
}


##==============================================================================
## Footer
1;

__END__
