## -*- Mode: CPerl -*-
## File: DiaColloDB::Persistent.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, persistent objects


package DiaColloDB::Persistent;
use DiaColloDB::Utils qw();
use IO::File;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: Text

## $bool = $obj->saveTextFh($fh, %opts)
##  + save text representation to a filehandle (dummy)
sub saveTextFh {
  $_[0]->logconfess("saveTextFh() not implemented");
}

## $bool = $obj->saveTextFile($filename_or_handle, %opts)
##  + wraps saveTextFh()
sub saveTextFile {
  my ($obj,$file,@args) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $obj->logconfess("saveTextFile(): failed to open '$file': $!") if (!ref($fh));
  my $rc = $obj->saveTextFh($fh,@args);
  $fh->close() if (!ref($file));
  return $rc;
}

## $obj = $CLASS_OR_OBJECT->loadTextFh($fh, %opts)
##  + load object from a text filehandle (dummy)
sub loadTextFh {
  $_[0]->logconfess("loadTextFh() not implemented");
}

## $bool = $CLASS_OR_OBJECT->loadTextFile($filename_or_handle, %opts)
##  + wraps loadTextFh()
sub loadTextFile {
  my ($that,$file,@args) = @_;
  my $fh = ref($file) ? $file : IO::File->new("<$file");
  $that->logconfess("loadTextFile(): failed to open '$file': $!") if (!ref($fh));
  my $obj = $that->loadTextFh($fh,@args);
  $fh->close() if (!ref($file));
  return $obj;
}

##--------------------------------------------------------------
## I/O: JSON

## $thingy = $obj->TO_JSON()
##   + JSON module wrapper; default just returns anonymous HASH-ref
sub TO_JSON {
  return { %{$_[0]} };
}

## $str = $obj->saveJsonString(%opts)
sub saveJsonString {
  return DiaColloDB::Utils::saveJsonString(@_);
}

## $str = $obj->saveJsonFh($fh, %opts)
BEGIN {
  *saveJsonFh = \&saveJsonFile;
}

## $str = $obj->saveJsonFile($filename_or_handle, %opts)
sub saveJsonFile {
  return DiaColloDB::Utils::saveJsonFile(@_);
}

## $obj = $CLASS_OR_OBJECT->loadJsonString( $string,%opts)
## $obj = $CLASS_OR_OBJECT->loadJsonString(\$string,%opts)
sub loadJsonString {
  my $that = shift;
  my $loaded = DiaColloDB::Utils::loadJsonString(@_);
  return bless($loaded,$that) if (!ref($that));
  %$that = %$loaded;
  return $that;
}

## $obj = $CLASS_OR_OBJECT->loadJsonFh($fh,%opts)
BEGIN {
  *loadJsonFh = \&loadJsonFile;
}

## $obj = $CLASS_OR_OBJECT->loadJsonFile($filename_or_handle,%opts)
sub loadJsonFile {
  my $that = shift;
  my $loaded = DiaColloDB::Utils::loadJsonFile(@_);
  return bless($loaded,$that) if (!ref($that));
  %$that = %$loaded;
  return $that;
}

##==============================================================================
## Footer
1;

__END__
