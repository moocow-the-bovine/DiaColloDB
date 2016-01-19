## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Vec::MM.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: (temporary) mmaped vec() buffers

package DiaColloDB::Vec::MM;
use DiaColloDB::Logger;
use File::Map;
use File::Temp;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);
our $LOG_DEFAULT = undef;	##-- default log-level (undef: off)

##==============================================================================
## Constructors etc.

## $mmvec = CLASS->new($size, $bits)
##  + %opts, %$mmvec:
##    (
##     log  => $level,      ##-- logging verbosity (default=$LOG_DEFAULT)
##     buf  => $buf,        ##-- guts: real underlying mmap()ed buffer data
##     size => $size,       ##-- number of logical elements
##     bits => $bits,       ##-- number of bits per element
##    )
sub new {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $size = shift;
  my $bits = shift;
  $that->logconfess("Usage: ", __PACKAGE__, "::new(SIZE, BITS)") if (!defined($size) || !defined($bits));
  my $log    = $opts->{log} // $LOG_DEFAULT;
  $file    //= $opts->{file} // 'vecXXXXX';

  ##-- guts
  $that->vlog($log, "CREATE (anonymous): $size elements, $bits bits/element (".($temp ? "TEMP" : "KEEP").")");
  my $mmv = bless({
		   file=>$file,
		   temp=>($temp||0),
		   log =>$log,
		  }, ref($that)||$that);
  my $bufr = \$mmv->{buf};
  File::Map::map_anonymous($$bufr, $size*$bits, 'shared');
  $that->logconfess("new(): map_anonymous failed for ", ($size*$bits), " byte(s): $!") if (!defined($$bufr));
  return $mmv;
}

##==============================================================================
## Accessors

## \$buf = $mmv->bufr()
sub bufr {
  return undef if (!UNIVERSAL::isa($_[0],'HASH'));
  return \$_[0]{buf};
}

## $bits = $mmv->bits()
sub bits { return $_[0]{bits}; }

## $size = $mmv->size()
sub size { return $_[0]{size}; }

##==============================================================================
## Footer
1; ##-- be happy
