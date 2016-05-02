## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrades: top level

package DiaColloDB::Upgrade;
use DiaColloDB;
use DiaColloDB::Upgrade::Base;
use DiaColloDB::Upgrade::v0_04_dlimits;
use DiaColloDB::Upgrade::v0_09_multimap;
use DiaColloDB::Upgrade::v0_10_x2t;
use Carp;
use strict;

##==============================================================================
## Globals

our @ISA = qw(DiaColloDB::Logger);

## @upgrades : list of available auto-magic upgrade sub-packages (suffixes)
our @upgrades = (
		 'v0_04_dlimits',
		 'v0_09_multimap',
		 'v0_10_x2t',
		);

##==============================================================================
## Top-level

## @upgrades = $CLASS_OR_OBJECT->available()
##  + returns list of available upgrade-packages (suffixes)
sub available {
  return @upgrades;
}

## @needed = $CLASS_OR_OBJECT->needed($dbdir, @upgrades)
##  + returns list of those upgrades in @upgrades which are needed for DB in $dbdir
sub needed {
  my ($that,$dbdir,@upgrades) = @_;
  return grep {
    my $pkg = $_;
    $pkg = "DiaColloDB::Upgrade::$pkg" if (!$pkg->can('needed'));
    $that->warn("unknown upgrade package $_") if (!$pkg->can('needed'));
    $pkg->can('needed') && $pkg->needed($dbdir)
  } @upgrades;
}

## $bool = $CLASS_OR_OBJECT->upgrade($dbdir, @upgrades)
##  + applies upgrades in @upgrades to DB in $dbdir
sub upgrade {
  my ($that,$dbdir,@upgrades) = @_;
  foreach (@upgrades) {
    my $pkg = $_;
    $pkg = "DiaColloDB::Upgrade::$pkg" if (!$pkg->can('needed'));
    $that->logconfess("unknown upgrade package $_") if (!$pkg->can('upgrade'));
    $that->info("applying upgrade package $_ to $dbdir/");
    $pkg->upgrade($dbdir)
      or $that->logconfess("upgrade via package $pkg failed for $dbdir/");
  }
  return $that;
}


##==============================================================================
## Footer
1; ##-- be happy
