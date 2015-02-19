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
##    data => \%key2prf,   ##-- ($date => $profile, ...) : profiles by date
##   )
sub new {
  my $that = shift;
  my $mp   = bless({
		    data=>{},
		    @_
		   }, (ref($that)||$that));
  return $mp;
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: JSON
##  + INHERITED from DiaCollocDB::Persistent

##--------------------------------------------------------------
## I/O: Text

## $bool = $obj->saveTextFile($filename_or_handle, %opts)
##  + wraps saveTextFh(); INHERITED from DiaCollocDB::Persistent

## $bool = $mp->saveTextFh($fh)
##  + save text representation to a filehandle (guts)
sub saveTextFh {
  my ($mp,$fh,%opts) = @_;
  my $ps = $mp->{data};
  foreach (sort {$a<=>$b} keys %$ps) {
    $ps->{$_}->saveTextFh($fh, prefix=>$_)
      or $mp->logconfess("saveTextFile() saved for sub-profile with key '$_': $!");
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
##    )
sub saveHtmlFile {
  my ($mp,$file,%opts) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $mp->logconfess("saveHtmlFile(): failed to open '$file': $!") if (!ref($fh));
  $fh->print("<html><body>\n") if ($opts{body}//1);
  $fh->print("<table><tbody>\n") if ($opts{table}//1);
  $fh->print("<tr>",(
		     map {"<th>".htmlesc($_)."</th>"}
		     #'N'
		     qw(f1 f2 f12 score),
		     qw(date),
		     qw(item2)
		    ),
	     "</tr>\n"
	    ) if ($opts{header}//1);
  my $ps = $mp->{data};
  foreach (sort {$a<=>$b} keys %$ps) {
    $ps->{$_}->saveHtmlFile($file, prefix=>$_, table=>0,body=>0,header=>0)
      or $mp->logconfess("saveTextFile() saved for sub-profile with key '$_': $!");
  }
  $fh->print("</tbody><table>\n") if ($opts{table}//1);
  $fh->print("</body></html>\n") if ($opts{body}//1);
  $fh->close() if (!ref($file));
  return $mp;
}

##==============================================================================
## Sub-profile wrappers

## $mp_or_undef = $mp->compile($func)
##  + compile all sub-profiles for score-function $func, one of qw(f mi ld); default='f'
sub compile {
  my ($mp,$func) = @_;
  $_->compile($func) or return undef foreach (values %{$mp->{data}});
  return $mp;
}

## $mp_or_undef = $mp->trim(%opts)
##  + calls $prf->trim(%opts) for each sub-profile $prf
sub trim {
  my $mp = shift;
  $_->trim(@_) or return undef foreach (values %{$mp->{data}});
  return $mp;
}

## $sprf = $prf->stringify( $obj)
## $sprf = $prf->stringify(\@key2str)
## $sprf = $prf->stringify(\&key2str)
## $sprf = $prf->stringify(\%key2str)
##  + returns stringified profile via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
sub stringify {
  my $mp = shift;
  $_->trim(@_) or return undef foreach (values %{$mp->{data}});
  return $mp;
}


##==============================================================================
## Footer
1;

__END__
