## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Compat.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: compatibility modules (all)

package DiaColloDB::Compat;
use DiaColloDB::Compat::v0_04;
use DiaColloDB::Compat::v0_08;
use DiaColloDB::Compat::v0_09;
use Carp;
use strict;

##==============================================================================
## Globals

##==============================================================================
## Utilities

## \&dummyMethodCode = $that->nocompat($methodName)
##   + wrapper for subclasses which do not implement some API methods
sub nocompat {
  my $that   = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : undef;
  my $method = shift;
  return sub {
    $_[0]->logconfess("method $method() not supported by compatibility wrapper");
  };
}

##==============================================================================
## Footer
1; ##-- be happy
