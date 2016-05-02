## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::v0_09_multimap.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: v0.08.x -> v0.09.x: MultiMapFile format

package DiaColloDB::Upgrade::v0_09_multimap;
use DiaColloDB::Upgrade::Base;
use strict;
our @ISA = qw(DiaColloDB::Upgrade::Base);

##==============================================================================
## API
## + Upgrade: v0_09_multimap: v0.08.x -> v0.09.x : MultiMapFile format change

## $version = $CLASS_OR_OBJECT->toversion()
##  + returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return '0.09.001';
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
