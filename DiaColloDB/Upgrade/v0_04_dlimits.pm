## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::v0_04_dlimits.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: v0.04: date-limits @$coldb{qw(xdmin xdmax)}

package DiaColloDB::Upgrade::v0_04_dlimits;
use DiaColloDB::Upgrade::Base;
use strict;
our @ISA = qw(DiaColloDB::Upgrade::Base);

##==============================================================================
## API

## $version = $CLASS_OR_OBJECT->toversion()
##  + returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return '0.04';
}

## $bool = $CLASS_OR_OBJECT->needed($dbdir)
##  + returns true iff local index in $dbdir needs upgrade
sub needed {
  my ($that,$dbdir) = @_;
  my $header = $that->dbheader($dbdir);
  return !defined($header->{xdmin}) || !defined($header->{xdmax});
}

## $bool = $CLASS_OR_OBJECT->_upgrade($dbdir, \%info)
##  + performs upgrade
sub upgrade {
  my ($that,$dbdir) = @_;

  ##-- xdmin, xdmax: from xenum
  my $hdr   = $that->dbheader($dbdir);
  my $xenum = $DiaColloDB::XECLASS->new(base=>"$dbdir/xenum")
    or $that->logconfess("failed to open (tuple+date) enum $dbdir/xenum.*: $!");
  my $pack_xdate  = '@'.(packsize($hdr->{pack_id}) * scalar(@{$hdr->attrs})).$hdr->{pack_date};
  my ($dmin,$dmax,$d) = ('inf','-inf');
  foreach (@{$xenum->toArray}) {
    next if (!$_);
    next if (!defined($d = unpack($pack_xdate,$_))); ##-- strangeness: getting only 9-bytes in $_ for 10-byte values in file and toArray(): why?!
    $dmin = $d if ($d < $dmin);
    $dmax = $d if ($d > $dmax);
  }
  $that->vlog('info', "extracted date-range \"xdmin\":$dmin, \"xdmax\":$dmax");

  ##-- update header
  return $that->updateHeader($dbdir,undef,{xdmin=>$dmin,xdmax=>$dmax});
}


##==============================================================================
## Footer
1; ##-- be happy
