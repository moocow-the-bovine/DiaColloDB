## -*- Mode: CPerl -*-
## File: DiaColloDB::Profile.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, co-frequency profile
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
use IO::File;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## Constructors etc.

## $prf = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    N   => $N,         ##-- total marginal relation frequency
##    f1  => $f1,        ##-- total marginal frequency of target word(s)
##    f2  => \%f2,       ##-- total marginal frequency of collocates: ($i2=>$f2, ...)
##    f12 => \%f12,      ##-- collocation frequencies, %f12 = ($i2=>$f12, ...)
##    #
##    score => $func,    ##-- selected scoring function ('f12', 'mi', or 'ld')
##    mi => \%mi12,      ##-- mutual information * logFreq scores a la Wortprofil; requires compile_mi()
##    ld => \%ld12,      ##-- log-dice scores a la Wortprofil; requires compile_ld()
##   )
sub new {
  my $that = shift;
  my $prf  = bless({
		    N=>1,
		    f1=>0,
		    f2=>{},
		    f12=>{},
		    #mi=>{},
		    #ld=>{},
		    @_
		   }, (ref($that)||$that));
  return $prf;
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: Text

## $bool = $prf->saveTextFile($filename_or_handle, %opts)
##  + %opts:
##    (
##     prefix => $prefix,   ##-- prefix each item-string with $sprefix (used by Profile::Multi), no tab-separators required
##    )
sub saveTextFile {
  my ($prf,$file,%opts) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $prf->logconfess("saveTextFile(): failed to open '$file': $!") if (!ref($fh));

  my ($f1,$f2,$f12) = @$prf{qw(f1 f2 f12)};
  my $prefix = $opts{prefix} // '';
  my $fscore = $prf->{$prf->{score}//'f12'};
  foreach (sort {$fscore->{$b} <=> $fscore->{$a}} keys %$fscore) {
    $fh->print(join("\t",
		    #$N,
		    $f1,
		    $f2->{$_},
		    $f12->{$_},
		    ($fscore ? $fscore->{$_} : 'NA'),
		    #($mi ? $mi->{$_} : 'NA'),
		    #($ld ? $ld->{$_} : 'NA'),
		    (defined($prefix) ? $prefix : qw()),
		    $_),
	       "\n");
  }
  $fh->close() if (!ref($file));
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
##     hprefix => $hprefix, ##-- prefix header item-cells with $hprefix (used by Profile::Multi), no '<th>..</th>' required
##     prefix => $prefix,   ##-- prefix item-cells with $prefix (used by Profile::Multi), no '<td>..</td>' required
##    )
sub saveHtmlFile {
  my ($prf,$file,%opts) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $prf->logconfess("saveTextFile(): failed to open '$file': $!") if (!ref($fh));

  $fh->print("<html><body>\n") if ($opts{body}//1);
  $fh->print("<table><tbody>\n") if ($opts{table}//1);
  $fh->print("<tr>",(
		     map {"<th>".htmlesc($_)."</th>"}
		     #'N'
		     qw(f1 f2 f12 score),
		     (defined($opts{hprefix}) ? $opts{hprefix} : qw()),
		     qw(item2)
		    ),
	     "</tr>\n"
	    ) if ($opts{header}//1);

  my ($f1,$f2,$f12) = @$prf{qw(f1 f2 f12)};
  my $prefix = $opts{prefix} // '';
  my $fscore = $prf->{$prf->{score}//'f12'};
  foreach (sort {$fscore->{$b} <=> $fscore->{$a}} keys %$fscore) {
    $fh->print("<tr>", (map {"<td>".htmlesc($_)."</td>"}
			#$N,
			$f1,
			$f2->{$_},
			$f12->{$_},
			($fscore ? $fscore->{$_} : 'NA'),
			(defined($prefix) ? $prefix : qw()),
			$_
		       ),
	       "</tr>\n");
  }
  $fh->print("</tbody><table>\n") if ($opts{table}//1);
  $fh->print("</body></html>\n") if ($opts{body}//1);
  $fh->close() if (!ref($file));
  return $prf;
}

##--------------------------------------------------------------
## I/O: JSON

## $thingy = $prf->TO_JSON()
##   + JSON module wrapper
sub TO_JSON {
  return { %{$_[0]} };
}

##==============================================================================
## Compilation

## $prf = $prf->compile($func)
##  + compile for score-function $func, one of qw(f mi ld); default='f'
sub compile {
  my ($prf,$func) = @_;
  return $prf->compile_f if (!$func || $func =~ m{^(?:f(?:req(?:uency)?)?(?:12)?)$}i);
  return $prf->compile_ld if ($func =~ m{^(?:ld|log-?dice)}i);
  return $prf->compile_mi if ($func =~ m{^(?:l?f?mi|mutual-?information)$}i);
  $prf->logwarn("compile(): unknown score function '$func'");
  return $prf->compile_f;
}

## $prf = $prf->compile_f()
##  + just sets $prf->{score} = 'f12'
sub compile_f {
  $_[0]{score} = 'f12';
  return $_[0];
}

## $prf = $prf->compile_mi()
##  + computes MI*logF-profile in $prf->{mi} a la Rychly (2008)
##  + sets $prf->{score}=$prf->{mi}
sub compile_mi {
  my $prf = shift;
  my ($N,$f1,$pf2,$pf12) = @$prf{qw(N f1 f2 f12)};
  my $mi = $prf->{mi} = {};
  my $eps = 0; #0.5;
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

## $prf = $prf->compile_ld()
##  + computes log-dice profile in $prf->{ld} a la Rychly (2008)
sub compile_ld {
  my $prf = shift;
  my ($N,$f1,$pf2,$pf12) = @$prf{qw(N f1 f2 f12)};
  my $ld = $prf->{ld} = {};
  my $eps = 0; #0.5;
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

## $prf = $prf->trim(%opts)
##  + %opts:
##    (
##     kbest => $kbest,    ##-- retain only $kbest items
##     cutoff => $cutoff,  ##-- retain only items with $prf->{$prf->{score}}{$item} >= $cutoff
##    )
sub trim {
  my ($prf,%opts) = @_;

  ##-- trim: scoring function
  my $score = $prf->{$prf->{score}//'f12'}
    or $prf->logconfess("trim(): no profile scores for '$prf->{score}'");

  ##-- trim: by user-specified cutoff
  if (defined(my $cutoff = $opts{cutoff})) {
    my @trim = qw();
    my ($key,$val);
    while (($key,$val) = each %$score) {
      push(@trim,$key) if ($val < $cutoff);
    }
    delete @{$prf->{$_}}{@trim} foreach (grep {defined($prf->{$_})} qw(f2 f12 mi ld));
  }

  ##-- trim: k-best
  my $kbest;
  if (defined($kbest = $opts{kbest}) && $kbest > 0) {
    my @trim = sort {$score->{$b} <=> $score->{$a}} keys %$score;
    if (@trim > $kbest) {
      splice(@trim, 0, $kbest);
      delete @{$prf->{$_}}{@trim} foreach (grep {defined($prf->{$_})} qw(f2 f12 mi ld));
    }
  }

  return $prf;
}

##==============================================================================
## Stringification

## $sprf = $prf->stringify( $obj)
## $sprf = $prf->stringify(\@key2str)
## $sprf = $prf->stringify(\&key2str)
## $sprf = $prf->stringify(\%key2str)
##  + returns stringified profile via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
sub stringify {
  my ($prf,$key2str) = @_;
  if (UNIVERSAL::can($key2str,'i2s')) {
    my $i2s = { map {($_=>$key2str->i2s($_))} sort {$a<=>$b} keys %{$prf->{f2}} };
    return $prf->stringify($i2s);
  }
  elsif (UNIVERSAL::isa($key2str,'CODE')) {
    my $i2s = { map {($_=>$key2str->($_))} sort {$a<=>$b} keys %{$prf->{f2}} };
    return $prf->stringify($i2s);
  }
  elsif (UNIVERSAL::isa($key2str,'HASH')) {
    my $sprf = $prf->new(N=>$prf->{N}, f1=>$prf->{f1}, score=>$prf->{score});
    foreach (grep {defined $prf->{$_}} qw(f2 f12 mi ld)) {
      @{$sprf->{$_}}{@$key2str{keys %{$prf->{$_}}}} = values %{$prf->{$_}};
    }
    return $sprf;
  }
  elsif (UNIVERSAL::isa($key2str,'ARRAY')) {
    my $sprf = $prf->new(N=>$prf->{N}, f1=>$prf->{f1}, score=>$prf->{score});
    foreach (grep {defined $prf->{$_}} qw(f2 f12 mi ld)) {
      @{$sprf->{$_}}{@$key2str[keys %{$prf->{$_}}]} = values %{$prf->{$_}};
    }
    return $sprf;
  }

  $prf->logconfess("stringify(): don't know how to stringify via object '$key2str'");
}


##==============================================================================
## Footer
1;

__END__
