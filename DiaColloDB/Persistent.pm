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
## I/O: Header

## @keys = $obj->headerKeys()
##  + keys to save as header; default implementation returns all keys of all non-references
sub headerKeys {
  return grep {!ref($_[0]{$_})} keys %{$_[0]};
}

## $hdr = $obj->headerData()
##  + returns reference to object header data; default returns anonymous HASH-ref for $obj->headerKeys()
sub headerData {
  return {(map {($_=>$_[0]->{$_})} $_[0]->headerKeys), class=>ref($_[0])};
}

## $filename = $obj->headerFile()
##  + returns header filename; default returns "$obj->{base}.hdr" or "$obj->{dbdir}/header.json"
sub headerFile {
  return undef if (!ref($_[0]));
  return "$_[0]{dbdir}/header.json" if (defined($_[0]{dbdir}));
  return "$_[0]{base}.hdr" if (defined($_[0]{base}));
  return "$_[0]{file}.hdr" if (defined($_[0]{file}));
  return undef;
}

## $str = $obj->saveHeaderString(%opts)
##  + returns JSON string for object header data
sub saveHeaderString {
  return DiaColloDB::Utils::saveJsonString($_[0]->headerData, @_[1..$#_]);
}

## $bool = $obj->saveHeaderFh($fh, %opts)
BEGIN {
  *saveHeaderFh = \&saveHeaderFile;
}

## $bool = $obj->saveHeaderFile($filename_or_handle, %opts)
sub saveHeaderFile {
  return DiaColloDB::Utils::saveJsonFile($_[0]->headerData, @_[1..$#_]);
}

## $bool = $obj->saveHeader()
## $bool = $obj->saveHeader($headerFile,%opts)
##  + wraps $obj->saveHeaderFile($headerFile//$obj->headerFile(), %opts)
sub saveHeader {
  $_[0]->saveHeaderFile(($_[1]//$_[0]->headerFile()), @_[2..$#_]);
}

##--

## $obj = $CLASS_OR_OBJECT->loadHeaderData($data_or_undef)
##  + instantiates header data from $data
##  + default just sets @$obj{keys %$data} = values %$data and clobbers $obj->{class}=ref($obj)
sub loadHeaderData {
  my ($that,$hdr) = @_;
  $that->logconfess("loadHeaderData(): header data undefined") if (!defined($hdr));
  $that = $that->new() if (!ref($that));
  @$that{keys %$hdr} = values %$hdr;
  $that->{class} = ref($that);
  return $that;
}

## $obj = $CLASS_OR_OBJECT->loadJsonString( $string,%opts)
## $obj = $CLASS_OR_OBJECT->loadJsonString(\$string,%opts)
##  + loads header data from JSON string $string
##  + wraps $CLASS_OR_OBJECT->loadHeaderData()
sub loadHeaderString {
  return $_[0]->loadHeaderData(DiaColloDB::Utils::loadJsonString(@_[1..$#_]));
}

## $obj = $CLASS_OR_OBJECT->loadHeaderFh($fh, %opts)
BEGIN {
  *loadHeaderFh = \&loadHeaderFile;
}

## $obj = $CLASS_OR_OBJECT->loadHeaderFile($filename_or_handle, %opts)
##  + wraps $CLASS_OR_OBJECT->loadHeaderData()
sub loadHeaderFile {
  return $_[0]->loadHeaderData(DiaColloDB::Utils::loadJsonFile(@_[1..$#_]));
}

## $bool = $CLASS_OR_OBJECT->loadHeader()
## $bool = $CLASS_OR_OBJECT->loadHeader($headerFile,%opts)
##  + wraps $CLASS_OR_OBJECT->loadHeaderFile($headerFile//$CLASS_OR_OBJ->headerFile())
sub loadHeader {
  return $_[0]->loadHeaderFile(($_[1]//$_[0]->headerFile), @_[2..$#_]);
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

## $bool = $obj->saveJsonFh($fh, %opts)
BEGIN {
  *saveJsonFh = \&saveJsonFile;
}

## $bool = $obj->saveJsonFile($filename_or_handle, %opts)
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


##==============================================================================
## Footer
1;

__END__