## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::Base.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: base class / API

package DiaColloDB::Upgrade::Base;
use DiaColloDB::Logger;
use DiaColloDB::Utils qw(:time);
use Carp;
use strict;
our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## API

## $version = $CLASS_OR_OBJECT->toversion()
##  + returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return $DiaColloDB::VERSION;
}

## $bool = $CLASS_OR_OBJECT->needed($coldb)
##  + returns true iff $coldb needs upgrade
sub needed {
  $_[0]->logconfess("needed() method not implemented");
}

## $bool = $CLASS_OR_OBJECT->upgrade($coldb, \%info)
##  + performs upgrade in-place on $coldb
sub upgrade {
  $_[0]->logconfess("ugprade() method not implemented");
}

## \%uinfo = $CLASS_OR_OBJECT->uinfo($coldb?,%info)
##  + returns a default upgrade-info structure for %info
##  + conventional keys %uinfo =
##    (
##     version_from => $vfrom,    ##-- source version (default='unknown')
##     version_to   => $vto,      ##-- target version (default=$CLASS_OR_OBJECT->_toversion)
##     timestamp    => $time,     ##-- timestamp (default=DiaColloDB::Utils::timestamp(time))
##     by           => $who,      ##-- user or script-name (default=$CLASS)
##    )
sub uinfo {
  my $that = shift;
  my $coldb = UNIVERSAL::isa($_[0],'DiaColloDB') ? shift : undef;
  return {
	  version_from=>($coldb ? $coldb->{version} : 'unknown'),
	  version_to=>$that->toversion,
	  timestamp=>DiaColloDB::Utils::timestamp(time),
	  by=>(ref($that)||$that),
	  @_
	 };
}

## $bool = $CLASS_OR_OBJECT->updateHeader($coldb, \%extra_uinfo)
##  + updates $coldb header
sub updateHeader {
  my ($that,$coldb,$xinfo) = @_;
  my $uinfo = $that->uinfo($coldb, %$xinfo);
  return if (!defined($uinfo)); ##-- silent upgrade

  my $upgraded = ($coldb->{upgraded} //= []);
  unshift(@$upgraded, $uinfo);
  $coldb->{version} = $uinfo->{version_to} if ($uinfo->{version_to});
  return $coldb->saveHeader();
}


##==============================================================================
## Footer
1; ##-- be happy
