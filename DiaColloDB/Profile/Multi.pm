## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Profile::Multi.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, co-frequency profiles, by date


package DiaColloDB::Profile::Multi;
use DiaColloDB::Profile;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## Constructors etc.

## $mp = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ps => \%key2prf,   ##-- ($date => $profile, ...) : profiles by date
##   )
sub new {
  my $that = shift;
  my $mp   = bless({
		    ps=>{},
		    @_
		   }, (ref($that)||$that));
  return $mp;
}

##==============================================================================
## I/O: Text

## $bool = $mp->saveTextFile($filename_or_handle)
sub saveTextFile {
  my ($mp,$file,%opts) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $mp->logconfess("saveTextFile(): failed to open '$file': $!") if (!ref($fh));
  my $ps = $mp->{ps};
  foreach (sort {$a<=>$b} keys %$ps) {
    $ps->{$_}->saveTextFile($file, prefix=>$_)
      or $mp->logconfess("saveTextFile() saved for sub-profile with key '$_': $!");
  }
  $fh->close() if (!ref($file));
  return $mp;
}


##==============================================================================
## I/O: JSON

## $thingy = $mp->TO_JSON()
##   + JSON module wrapper
sub TO_JSON {
  return { %{$_[0]} };
}

##==============================================================================
## Sub-profile wrappers

## $mp_or_undef = $mp->compile($func)
##  + compile all sub-profiles for score-function $func, one of qw(f mi ld); default='f'
sub compile {
  my ($mp,$func) = @_;
  $_->compile($func) or return undef foreach (values %{$mp->{ps}});
  return $mp;
}

## $mp_or_undef = $mp->trim(%opts)
##  + calls $prf->trim(%opts) for each sub-profile $prf
sub trim {
  my $mp = shift;
  $_->trim(@_) or return undef foreach (values %{$mp->{ps}});
  return $mp;
}

## $sprf = $prf->stringify( $obj)
## $sprf = $prf->stringify(\@key2str)
## $sprf = $prf->stringify(\&key2str)
## $sprf = $prf->stringify(\%key2str)
##  + returns stringified profile via $obj->i2s($key2), $key2str->($i2) or $key2str->{$i2}
sub stringify {
  my $mp = shift;
  $_->trim(@_) or return undef foreach (values %{$mp->{ps}});
  return $mp;
}


##==============================================================================
## Footer
1;

__END__
