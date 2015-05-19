## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Profile::Multi.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, co-frequency profiles, by date


package DiaColloDB::Profile::Multi;
use DiaColloDB::Profile;
use DiaColloDB::Persistent;
use DiaColloDB::Utils qw(:html);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Persistent);

##==============================================================================
## Constructors etc.

## $mp = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    profiles => \@profiles,   ##-- ($profile, ...) : sub-profiles, with {label} key
##    titles   => \@titles,     ##-- item group titles (default:undef: unknown)
##   )
sub new {
  my $that = shift;
  my $mp   = bless({
		    profiles=>[],
		    #titles=>undef,
		    @_
		   }, (ref($that)||$that));
  return $mp;
}

## $mp2 = $mp->clone()
## $mp2 = $mp->clone($keep_compiled)
##  + clones %$mp
##  + if $keep_score is true, compiled data is cloned too
sub clone {
  my $mp = shift;
  my $profiles = $mp->{profiles};
  return bless({
		profiles=>[map {$_->clone(@_)} @$profiles],
		($mp->{titles} ? (titles=>[@{$mp->{titles}}]) : qw()),
	       }, ref($mp)
	      );
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
  my $mp   = $that->DiaColloDB::Persistent::loadJsonData(@_);
  bless($_,'DiaColloDB::Profile') foreach (@{$mp->{profiles}//[]});
  return $mp;
}

##--------------------------------------------------------------
## I/O: Text

## undef = $CLASS_OR_OBJECT->saveTextHeader($fh, hlabel=>$hlabel, titles=>\@titles)
sub saveTextHeader {
  my ($that,$fh,%opts) = @_;
  DiaColloDB::Profile::saveTextHeader($that,$fh,hlabel=>'label',@_);
}

## $bool = $obj->saveTextFile($filename_or_handle, %opts)
##  + wraps saveTextFh(); INHERITED from DiaCollocDB::Persistent

## $bool = $mp->saveTextFh($fh,%opts)
##  + %opts:
##     header => $bool,     ##-- include header-row? (default=1)
##     ...                  ##-- other options passed to DiaColloDB::Profile::saveTextFh()
##  + save text representation to a filehandle (guts)
sub saveTextFh {
  my ($mp,$fh,%opts) = @_;
  my $ps = $mp->{profiles};
  $mp->saveTextHeader($fh,%opts) if ($opts{header}//1);
  foreach (@$ps) {
    $_->saveTextFh($fh,%opts,header=>0)
      or $mp->logconfess("saveTextFile() saved for sub-profile with label '", $_->label, "': $!");
  }
  return $mp;
}

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
		     qw(N f1 f2 f12 score),
		     qw(label),
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
## Compilation and Trimming

## $mp_or_undef = $mp->compile($func)
##  + compile all sub-profiles for score-function $func, one of qw(f mi ld); default='f'
sub compile {
  my ($mp,$func) = @_;
  $_->compile($func) or return undef foreach (@{$mp->{profiles}});
  return $mp;
}

## $mp = $mp->uncompile()
##  + un-compiles all scores for $mp
sub uncompile {
  $_->uncompile() foreach (@{$_[0]{profiles}});
  return $_[0];
}

## $mp_or_undef = $mp->trim(%opts)
##  + new %opts:
##    empty => $bool,   ##-- remove empty profiles (default=true)
##  + calls $prf->trim(%opts) for each sub-profile $prf
sub trim {
  my ($mp,%opts) = @_;
  @{$mp->{profiles}} = grep {!$_->empty} @{$mp->{profiles}} if (!exists($opts{empty}) || $opts{empty});
  $_->trim(%opts) or return undef foreach (@{$mp->{profiles}});
  return $mp;
}

## $mp = $mp->stringify( $obj)
## $mp = $mp->stringify(\@key2str)
## $mp = $mp->stringify(\&key2str)
## $mp = $mp->stringify(\%key2str)
##  + stringifies multi-profile (destructive) via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
sub stringify {
  my $mp = shift;
  $_->stringify(@_) or return undef foreach (@{$mp->{profiles}});
  return $mp;
}

##==============================================================================
## Binary operations

## $mp = $mp->_add($mp2,%opts)
##  + adds $mp2 frequency data to $mp (destructive)
##  + implicitly un-compiles sub-profiles
##  + %opts: passed to Profile::_add()
sub _add {
  my ($amp,$bmp) = (shift,shift);
  my %a2data = map {($_->label=>$_)} @{$amp->{profiles}};
  my ($bkey,$bprf,$aprf);
  foreach $bprf (@{$bmp->{profiles}}) {
    $bkey = $bprf->label;
    if (defined($aprf=$a2data{$bkey})) {
      $aprf->_add($bprf,@_);
    } else {
      $a2data{$bkey} = $bprf->clone();
    }
  }
  @{$amp->{profiles}} = sort {$a->label cmp $b->label} values %a2data; ##-- re-sort
  return $amp->uncompile();
}

## $mp3 = $mp1->add($mp2,%opts)
##  + returns sum of $mp1 and $mp2 frequency data (destructive)
##  + %opts: passed to Profile::_add()
sub add {
  return $_[0]->clone->_add(@_[1..$#_]);
}

## $diff = $mp1->diff($mp2)
##  + returns score-diff of multi-profiles $mp1 and $mp2; wraps DiaColloDB::Profile::MultiDiff->new($mp1,$mp2)
sub diff {
  return DiaColloDB::Profile::MultiDiff->new(@_);
}


##==============================================================================
## Footer
1;

__END__
