## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade

package DiaColloDB::Upgrade;
use DiaColloDB::Logger;
use DiaColloDB::Utils qw(:time);
use Carp;
use strict;

##==============================================================================
## Globals

our @ISA = qw(DiaColloDB::Logger);

## @upgrades : list of available auto-magic upgrade sub-packages (suffixes)
our @upgrades = (
		 'v0_08_to_v0_09_multimap',
		);

##==============================================================================
## Top-level

## @upgrades = $CLASS_OR_OBJECT->available()
##  + returns list of available upgrade-packages (suffixes)
sub available {
  return @upgrades;
}

## @needed = $CLASS_OR_OBJECT->needed($coldb, @upgrades)
##  + returns list of those upgrades in @upgrades which are needed for $coldb
sub needed {
  my ($that,$coldb,@upgrades) = @_;
  return grep {
    my $pkg = $_;
    $pkg = "DiaColloDB::Upgrade::$pkg" if (!$pkg->can('needed'));
    $that->warn("unknown upgrade package $_") if (!$pkg->can('needed'));
    $pkg->can('needed') && $pkg->needed($coldb)
  } @upgrades;
}

## $bool = $CLASS_OR_OBJECT->upgrade($coldb, @upgrades)
##  + applies upgrades in @upgrades to $coldb
sub upgrade {
  my ($that,$coldb,@upgrades) = @_;
  foreach (@upgrades) {
    my $pkg = $_;
    $pkg = "DiaColloDB::Upgrade::$pkg" if (!$pkg->can('needed'));
    $that->logconfess("unknown upgrade package $_") if (!$pkg->can('upgrade'));
    $that->info("applying upgrade package $_");
    $pkg->upgrade($coldb)
      or $that->logconfess("upgrade via package $pkg failed");
  }
  return $that;
}

##==============================================================================
## Upgrader API

package DiaColloDB::Upgrade::Base;
use DiaColloDB::Logger;
use Carp;
use strict;
our @ISA = qw(DiaColloDB::Logger);

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
## Upgrade: v0_08_to_v0_09_multimap: v0.08.x -> v0.09.x : MultiMapFile format change
package DiaColloDB::Upgrade::v0_08_to_v0_09_multimap;
use version;
use strict;
our @ISA = qw(DiaColloDB::Upgrade::Base);

## $version = $CLASS_OR_OBJECT->toversion()
##  + returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return '0.09.001';
}

## $bool = $CLASS_OR_OBJECT->_needed($coldb)
##  + returns true iff $coldb needs upgrade
sub needed {
  my ($that,$coldb) = @_;
  return version->parse($coldb->{version}) < version->parse($that->toversion);
}

## $bool = $CLASS_OR_OBJECT->_upgrade($coldb, \%info)
##  + performs upgrade
sub upgrade {
  my ($that,$coldb) = @_;

  ##-- convert by attribute
  foreach my $attr (@{$coldb->{attrs}}) {
    my $mmf  = $coldb->{"${attr}2x"};
    my $base = $mmf->{base};
    $that->info("upgrading $base.*");

    ##-- convert
    my %mmopts = (pack_i=>$mmf->{pack_i});
    my $tmp = $DiaColloDB::MMCLASS->new(flags=>'rw', %mmopts)
      or $that->logconfess("upgrade(): failed to create new DiaColloDB::MultiMapFile object for $base.*");
    $tmp->fromArray($mmf->toArray)
      or $that->logconfess("upgrade(): failed to convert data for $base.*");
    $mmf->close();
    $tmp->save($base)
      or $that->logconfess("upgrade(): failed to save new data for $base.*");
    $coldb->{"${attr}2x"} = $tmp;
  }

  ##-- update header
  return $that->updateHeader($coldb);
}




##==============================================================================
## Footer
1; ##-- be happy
