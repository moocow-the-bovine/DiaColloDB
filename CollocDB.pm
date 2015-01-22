## -*- Mode: CPerl -*-
## File: CollocDB.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, top-level

use CollocDB::Logger;
use CollocDB::Enum;
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = 0.01;
our @ISA = qw(CollocDB::Logger);

##==============================================================================
## Constructors etc.

## $cldb = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    dbdir => $dbdir, ##-- database directory; REQUIRED
##   )
sub new {
  my $that = shift;
  my $cldb  = bless({
		     dbdir => undef,
		     @_, ##-- user arguments
		    },
		    ref($that)||$that);
  return defined($cldb->{dbdir}) ? $cldb->open($cldb->{dbdir}) : $cldb;
}

##==============================================================================
## I/O: open/close

## $bool = $cldb->opened()
sub opened {
  my $cldb = shift;
  return defined($cldb->{dbdir});
}

## $cldb_or_undef = $cldb->close()
sub close {
  my $cldb = shift;
  undef $cldb->{dbdir};
  return $cldb;
}

## $cldb_or_undef = $cldb->open($dbdir)
## $cldb_or_undef = $cldb->open()
sub open {
  my ($cldb,$dbdir) = @_;
  $dbdir //= $cldb->{dbdir};
  $cldb->close() if ($cldb->opened);
  
}



##==============================================================================
## Footer
1;

__END__




