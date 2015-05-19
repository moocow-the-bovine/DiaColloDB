## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Profile::MultiDiff.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, co-frequency profile diffs, by date


package DiaColloDB::Profile::MultiDiff;
use DiaColloDB::Profile::Multi;
use DiaColloDB::Profile::Diff;
use DiaColloDB::Utils qw(:html);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Profile::Multi);

##==============================================================================
## Constructors etc.

## $mpd = CLASS_OR_OBJECT->new(%args)
## $mpd = CLASS_OR_OBJECT->new($mp1,$mp2,%args)
## + %args, object structure:
##   (
##    data => \%key2prf,   ##-- ($date => $profile, ...) : profiles by date
##   )
sub new {
  my $that = shift;
  my $mp1  = UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile::Multi') ? shift : undef;
  my $mp2  = UNIVERSAL::isa(ref($_[0]),'DiaColloDB::Profile::Multi') ? shift : undef;
  my $mpd  = $that->SUPER::new(@_);
  return $mpd->populate($mp1,$mp2) if ($mp1 && $mp2);
  return $mpd;
}


## $mp2 = $mp->clone()
## $mp2 = $mp->clone($keep_compiled)
##  + clones %$mp
##  + if $keep_score is true, compiled data is cloned too
##  + INHERITED from DiaColloDB::Profile::Multi

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: JSON
##  + mostly INHERITED from DiaCollocDB::Persistent

## $obj = $CLASS_OR_OBJECT->loadJsonData( $data,%opts)
##  + guts for loadJsonString(), loadJsonFile()
sub loadJsonData {
  my $that = shift;
  my $mp   = $that->DiaColloDB::Persistent::loadJsonData(@_);
  foreach (@{$mp->{profiles}//[]}) {
    bless($_,'DiaColloDB::Profile::Diff');
    bless($_->{prf1}, 'DiaColloDB::Profile') if ($_->{prf1});
    bless($_->{prf2}, 'DiaColloDB::Profile') if ($_->{prf2});
  }
  return $mp;
}

##--------------------------------------------------------------
## I/O: Text

## undef = $CLASS_OR_OBJECT->saveTextHeader($fh, hlabel=>$hlabel, titles=>\@titles)
sub saveTextHeader {
  my ($that,$fh,%opts) = @_;
  DiaColloDB::Profile::Diff::saveTextHeader($that,$fh,hlabel=>'label',@_);
}

## $bool = $obj->saveTextFile($filename_or_handle, %opts)
##  + wraps saveTextFh(); INHERITED from DiaCollocDB::Persistent

## $bool = $mp->saveTextFh($fh,%opts)
##  + save text representation to a filehandle (guts)
##  + INHERITED from DiaCollocDB::Profile::Multi

##--------------------------------------------------------------
## I/O: HTML

## $bool = $mp->saveHtmlFile($filename_or_handle, %opts)
##  + %opts:
##    (
##     table  => $bool,     ##-- include <table>..</table> ? (default=1)
##     body   => $bool,     ##-- include <html><body>..</html></body> ? (default=1)
##     header => $bool,     ##-- include header-row? (default=1)
##     format => $fmt,      ##-- printf score formatting (default="%.2f")
##    )
sub saveHtmlFile {
  my ($mp,$file,%opts) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $mp->logconfess("saveHtmlFile(): failed to open '$file': $!") if (!ref($fh));
  $fh->print("<html><body>\n") if ($opts{body}//1);
  $fh->print("<table><tbody>\n") if ($opts{table}//1);
  $fh->print("<tr>",(
		     map {"<th>".htmlesc($_)."</th>"}
		     qw(ascore bscore diff label),
		     @{$mp->{titles}//[qw(item2)]},
		    ),
	     "</tr>\n"
	    ) if ($opts{header}//1);
  my $ps = $mp->{profiles};
  foreach (@$ps) {
    $_->saveHtmlFile($file, %opts,table=>0,body=>0,header=>0)
      or $mp->logconfess("saveHtmlFile() saved for sub-profile with label '", $_->label, "': $!");
  }
  $fh->print("</tbody><table>\n") if ($opts{table}//1);
  $fh->print("</body></html>\n") if ($opts{body}//1);
  $fh->close() if (!ref($file));
  return $mp;
}

##==============================================================================
## Compilation

##  @ppairs = $CLASS_OR_OBJECT->align($mp1,$mp2)
## \@ppairs = $CLASS_OR_OBJECT->align($mp1,$mp2)
##  + aligns subprofile-pairs from $mp1 and $mp2
##  + subprofiles are aligned in stored order
##  + arguments must be EITHER singletons (1 subprofile) OR of same size
##    - this lets you compare e.g. a global profile with a sliced one by
##      something like PDL's "implicit threading"
sub align {
  my ($that,$mpa,$mpb) = @_;
  my $psa = $mpa->{profiles};
  my $psb = $mpb->{profiles};
  if (@$psa==1 || @$psb==1 || @$psa==@$psb) {
    ##-- align cyclically (allow slices)
    my @pairs = map {
      [
       (@$psa==1 && $_ != 0 ? $psa->[0]->clone(1) : $psa->[$_]),
       (@$psb==1 && $_ != 0 ? $psb->[0]->clone(1) : $psb->[$_])
      ]
    } (0..($#$psa > $#$psb ? $#$psa : $#$psb));
    return wantarray ? @pairs : \@pairs;
  }
  $that->logconfess("align(): cannot align non-trivial multi-profiles of unequal size (".scalar(@$psa)." != ".scalar(@$psb).")");
}

## $mpd = $mpd->populate($mp1,$mp2)
##  + populates multi-diff by subtracting $mp1 sub-profile scores from $mp1
##  + uses $mpd->align() to align sub-profiles
sub populate {
  my ($mpd,$mpa,$mpb) = @_;
  @{$mpd->{profiles}} = map {
    DiaColloDB::Profile::Diff->new($_->[0],$_->[1])
  } @{$mpd->align($mpa,$mpb)};
  return $mpd;
}

## $mp_or_undef = $mp->compile($func)
##  + compile all sub-profiles for score-function $func, one of qw(f mi ld); default='f'
##  + INHERITED from DiaColloDB::Profile::Multi

## $mp = $mp->uncompile()
##  + un-compiles all scores for $mp
##  + INHERITED from DiaColloDB::Profile::Multi

## $mp_or_undef = $mp->trim(%opts)
##  + calls $prf->trim(%opts) for each sub-profile $prf
##  + INHERITED from DiaColloDB::Profile::Multi

## $mp = $mp->stringify( $obj)
## $mp = $mp->stringify(\@key2str)
## $mp = $mp->stringify(\&key2str)
## $mp = $mp->stringify(\%key2str)
##  + stringifies multi-profile (destructive) via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
##  + INHERITED from DiaColloDB::Profile::Multi

##==============================================================================
## Binary operations

## $mp = $mp->_add($mp2,%opts)
##  + adds $mp2 frequency data to $mp (destructive)
##  + implicitly un-compiles sub-profiles
##  + %opts: passed to Profile::_add()
##  + INHERITED but probably useless

## $mp3 = $mp1->add($mp2,%opts)
##  + returns sum of $mp1 and $mp2 frequency data (destructive)
##  + %opts: passed to Profile::_add()
##  + INHERITED but probably useless

## $diff = $mp1->diff($mp2)
##  + returns score-diff of $mp1 and $mp2 frequency data (destructive)
##  + INHERITED but probably useless

##==============================================================================
## Package DiaColloDB::Profile::Multi::Diff : alias
package DiaColloDB::Profile::Multi::Diff;
our @ISA = qw(DiaColloDB::Profile::MultiDiff);


##==============================================================================
## Footer
1;

__END__
