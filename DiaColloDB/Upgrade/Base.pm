## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::Base.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: base class / API

package DiaColloDB::Upgrade::Base;
use DiaColloDB::Logger;
use DiaColloDB::Utils qw(:time);
use Carp;
use version;
use strict;
our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## API

## $version = $CLASS_OR_OBJECT->toversion()
##  + returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return $DiaColloDB::VERSION;
}

## $bool = $CLASS_OR_OBJECT->needed($dbdir)
##  + returns true iff local index in $dbdir needs upgrade
##  + default implementation returns true iff $coldb->{version} is less than $CLASS_OR_OBJECT->toversion()
sub needed {
  my ($that,$dbdir) = @_;
  my $header = $that->dbheader($dbdir);
  return version->parse($header->{version}//'0.0.0') < version->parse($that->toversion);
}

## $bool = $CLASS_OR_OBJECT->upgrade($coldb, \%info)
##  + performs upgrade in-place on $coldb
sub upgrade {
  $_[0]->logconfess("ugprade() method not implemented");
}

## \%uinfo = $CLASS_OR_OBJECT->uinfo($dbdir?,%info)
##  + returns a default upgrade-info structure for %info
##  + conventional keys %uinfo =
##    (
##     version_from => $vfrom,    ##-- source version (default='unknown')
##     version_to   => $vto,      ##-- target version (default=$CLASS_OR_OBJECT->_toversion)
##     timestamp    => $time,     ##-- timestamp (default=DiaColloDB::Utils::timestamp(time))
##     by           => $who,      ##-- user or script-name (default=$CLASS)
##    )
sub uinfo {
  my $that  = shift;
  my $dbdir = ((scalar(@_)%2)==0 ? undef : shift);
  my $header = $dbdir ? $that->dbheader($dbdir) : {};
  return {
	  version_from=>($header->{version} // 'unknown'),
	  version_to=>$that->toversion,
	  timestamp=>DiaColloDB::Utils::timestamp(time),
	  by=>(ref($that)||$that),
	  @_
	 };
}

## $bool = $CLASS_OR_OBJECT->updateHeader($dbdir, \%extra_uinfo)
##  + updates header $dbdir/header.json
sub updateHeader {
  my ($that,$dbdir,$xinfo) = @_;
  my $uinfo = $that->uinfo($dbdir, %$xinfo);
  return if (!defined($uinfo)); ##-- silent upgrade

  my $header   = $that->dbheader($dbdir);
  my $upgraded = ($header->{upgraded} //= []);
  unshift(@$upgraded, $uinfo);
  $header->{version} = $uinfo->{version_to} if ($uinfo->{version_to});
  DiaColloDB::Utils::saveJsonFile($header, "$dbdir/header.json")
      or $that->logconfess("updateHeader(): failed to save header data to $dbdir/header.json: $!");
  return $that;
}

##==============================================================================
## Utils

## \%hdr = $CLASS_OR_OBJECT->dbheader($dbdir)
##  + reads $dbdir/header.json
sub dbheader {
  my ($that,$dbdir) = @_;
  my $hdr = DiaColloDB::Utils::loadJsonFile("$dbdir/header.json")
      or $that->logconfess("dbheader(): failed to read header $dbdir/header.json: $!");
  return $hdr;
}


##==============================================================================
## Footer
1; ##-- be happy
