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

## @upgrade_pkgs = $CLASS_OR_OBJECT->available()
##  + returns list of available upgrade-packages (suffixes)
sub available {
  return @upgrades;
}

## @needed = $CLASS_OR_OBJECT->needed($dbdir, \%opts?, @upgrade_pkgs)
##  + returns list of those package-names in @upgrade_pkgs which are needed for DB in $dbdir
##  + %opts are passed to upgrade-package new() methods
sub needed {
  my $that  = shift;
  my $dbdir = shift;
  my $opts  = UNIVERSAL::isa($_[0],'HASH') ? shift : {};
  return grep {
    my $pkg = $_;
    $pkg = "DiaColloDB::Upgrade::$pkg" if (!UNIVERSAL::can($pkg,'needed'));
    $that->warn("unknown upgrade package $_") if (!UNIVERSAL::can($pkg,'needed'));
    $pkg->new($dbdir,%$opts)->needed();
  } @_;
}

## $bool = $CLASS_OR_OBJECT->upgrade($dbdir, \%opts?, \@upgrades_or_pkgs)
##  + applies upgrades in @upgrades to DB in $dbdir
##  + %opts are passed to upgrade-package new() methods
sub upgrade {
  my $that  = shift;
  my $dbdir = shift;
  my $opts  = UNIVERSAL::isa($_[0],'HASH') ? shift : {};
  foreach (@_) {
    my $pkg = $_;
    $pkg = "DiaColloDB::Upgrade::$pkg" if (!UNIVERSAL::can($pkg,'upgrade'));
    $that->logconfess("unknown upgrade package $_") if (!UNIVERSAL::can($pkg,'upgrade'));
    $that->info("applying upgrade package $_ to $dbdir/");
    $pkg->new($dbdir,%$opts)->upgrade()
      or $that->logconfess("upgrade via package $pkg failed for $dbdir/");
  }
  return $that;
}


##==============================================================================
## Footer
1; ##-- be happy
