## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::v0_10_x2t.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: v0.09.x -> v0.10.x: x-tuples (+date) -> t-tuples (-date)

package DiaColloDB::Upgrade::v0_10_x2t;
use DiaColloDB::Upgrade::Base;
use DiaColloDB::Utils qw(:pack);
use version;
use strict;
our @ISA = qw(DiaColloDB::Upgrade::Base);

##==============================================================================
## API

## $version = $CLASS_OR_OBJECT->toversion()
##  + returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return '0.10.000';
}

## $bool = $CLASS_OR_OBJECT->_upgrade($dbdir, \%info)
##  + performs upgrade
sub upgrade {
  my ($that,$dbdir) = @_;

  ##-- read header
  my $hdr = $that->dbheader($dbdir);

  ##-- common variables
  no warnings 'portable';
  my $pack_t  = $hdr->{pack_t} = "($hdr->{pack_id})[".scalar(@{$hdr->{attrs}})."]";
  my $len_t   = packsize($pack_t);
  my $pack_xd = '@'.$len_t.$hdr->{pack_date};
  my $nbits_d = packsize($hdr->{pack_date}) * 8;
  my $nbits_t = packsize($hdr->{pack_id}) * 8;

  ##-- convert xenum to tenum
  $that->info("creating $dbdir/tenum.* from $dbdir/xenum.*");
  my $xenum = $DiaColloDB::XECLASS->new(base=>"$dbdir/xenum", pack_s=>$hdr->{pack_x})
    or $that->logconfess("failed to open $dbdir/xenum.*: $!");
  my $xi2s = $xenum->toArray;
  my $xi2t = '';
  my $xi2d = '';
  my $ts2i  = {};
  my $nt    = 0;
  my ($xi,$xs,$xd,$ts,$ti);
  vec($xi2t, $#$xi2s, $nbits_t) = 0;  ##-- $xi2t : [$xi] => $ti
  vec($xi2d, $#$xi2s, $nbits_d) = 0;  ##-- $xi2d : [$xi] => $date
  for ($xi=0; $xi <= $#$xi2s; ++$xi) {
    $xs = $xi2s->[$xi];
    $ts = substr($xs,0,$len_t);
    $ti = $ts2i->{$ts} = $nt++ if (!defined($ti=$ts2i->{$ts}));
    vec($xi2d,$xi,$nbits_d) = unpack($pack_xd,$xs);
    vec($xi2t,$xi,$nbits_t) = $ti;
  }
  my $tenum = $xenum->new(pack_s=>$pack_t, map {($_=>$xenum->{$_})} qw(pack_i pack_o pack_l));
  $tenum->fromHash($ts2i)->save("$dbdir/tenum")
    or $that->logconfess("failed to save $dbdir/tenum.*: $!");

  ##-- convert attribute-wse multimaps
  foreach my $attr (@{$hdr->{attrs}}) {
    $that->info("creating multimap $dbdir/${attr}_2t.* from $dbdir/${attr}_2x.*");
    my $xmm = $DiaColloDB::MMCLASS->new(flags=>'r', base=>"$dbdir/${attr}_2x")
      or $that->logconfess("failed to open $dbdir/${attr}_2x.*");
    my $mma     = $xmm->toArray();

    my $pack_bs = "($xmm->{pack_i})*";
    my ($ai,$tmp);
    for ($ai=0; $ai <= $#$mma; ++$ai) {
      $tmp = undef;
      $mma->[$ai] = pack($pack_bs,
			 map {defined($tmp) && $tmp==$_ ? qw(): ($tmp=$_)}
			 sort {$a<=>$b}
			 map {vec($xi2t,$_,$nbits_t)}
			 unpack($pack_bs, $mma->[$ai])
			);
    }

    my $tmm = $xmm->new(flags=>'rw', (map {($_=>$xmm->{$_})} qw(pack_i)))
      or $that->logconfess("failed to create new multimap for attribute '$attr'");
    $tmm->fromArray($mma)
      or $that->logconfess("failed to convert multimap data for attirbute '$attr'");
    $tmm->save("$dbdir/${attr}_2t")
      or $that->logconfess("failed to save multimap data for attrbute '$attr' to $dbdir/${attr}_2t.*: $!");
  }

  ##-- TODO: convert relations: unigrams

  ##-- TODO: convert relations: cofreqs

  $that->logdie("what now?");

  ##-- update header
  return $that->updateHeader($dbdir);
}


##==============================================================================
## Footer
1; ##-- be happy
