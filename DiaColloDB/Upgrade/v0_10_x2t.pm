## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::v0_10_x2t.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: v0.09.x -> v0.10.x: x-tuples (+date) -> t-tuples (-date)

package DiaColloDB::Upgrade::v0_10_x2t;
use DiaColloDB::Upgrade::Base;
use DiaColloDB::Utils qw(:pack :env :run);
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
  $that->vlog("creating new unigrams index $dbdir/tf.* from $dbdir/xf.*: TODO");

  ##-- convert relations: cofreqs
  $that->vlog("creating new co-frequency index $dbdir/tcof.* from $dbdir/cof.*: TODO");
  my $cof = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof")
    or $that->logconfess("failed to open co-frequency index $dbdir/cof.*: $!");
  my $i2s1 = sub { join("\t", vec($xi2t,$_[0],$nbits_t), vec($xi2d,$_[0],$nbits_d)) };
  my $i2s2 = sub { vec($xi2t,$_[0],$nbits_t) };
  env_push('LC_ALL'=>'C');
  my $sortfh = opencmd("| sort -nk2 -nk3 -nk4 -o \"$dbdir/cof.x2t\"")
    or $that->logconfess("open failed for pipe from sort for $dbdir/cof.x2t: $!");
  binmode($sortfh,':raw');
  $cof->saveTextFh($sortfh, i2s1=>$i2s1, i2s2=>$i2s2)
    or $that->logconfess("failed to create temporary file $dbdir/cof.x2t");
  $sortfh->close()
    or $that->logconfess("failed to close pipe to sort: $!");
  env_pop();

  my $tcof = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/tcof", flags=>'rw', version=>$that->toversion,
						(map {($_=>$cof->{$_})} qw(pack_i pack_f fmin dmax)),
						pack_d => $hdr->{pack_date});
  $tcof->loadTextFile("$dbdir/cof.x2t")
    or $that->logconfess("failed to load co-frequency data from $dbdir/cof.x2t: $!");


  $that->warn("what now?");

  ##-- cleanup
  #CORE::unlink("$dbdir/cof.x2t");

  ##-- update header
  return $that->updateHeader($dbdir);
}


##==============================================================================
## Footer
1; ##-- be happy
