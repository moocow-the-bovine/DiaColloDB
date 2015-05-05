## -*- Mode: CPerl -*-
## File: DiaColloDB::Unigrams.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, unigram database (using DiaColloDB::PackedFile)

package DiaColloDB::DDC;
use DiaColloDB::Relation;
use DiaColloDB::Utils qw(:sort :env :run :pack :file);
use DDC::Client::Distributed;
use Fcntl qw(:seek);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Relation);

##==============================================================================
## Constructors etc.

## $ddc = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- ddc client options
##    ddcServer => "$server:$port",    ##-- ddc server; default="localhost:50000",
##    ddcTimeout => $timeout,          ##-- ddc timeout; default=60
##    ##
##    ##-- low-level data
##    client    => $ddcClient,         ##-- a DDC::Client::Distributed object
##   )
sub new {
  my $that = shift;
  my $rel  = $that->SUPER::new(
			       ddcServer  => 'localhost:50000',
			       ddcTimeout => 60,
			       @_
			      );
  return $rel;
}

##==============================================================================
## Relation API: create

## $ug = $CLASS_OR_OBJECT->create($coldb,$tokdat_file,%opts)
##  + just saves header data
sub create {
  my ($ug,$coldb,$datfile,%opts) = @_;

  ##-- create/clobber
  $ug = $ug->new() if (!ref($ug));
  @$ug{keys %opts} = values %opts;

  ##-- ensure openend
  $ug->opened
    or $ug->open()
      or $ug->logconfess("create(): failed to open unigrams database: $!");

  ##-- populate db
  my $packas = $ug->{packas} // 'N';
  my $cur    = 0;
  my $N      = 0;
  my ($i,$f);
  if (defined($opts{size})) {
    $ug->setsize($opts{size})
      or $ug->logconfess("create(): failed to set size = $opts{size}: $!");
  }
  env_push(LC_ALL=>'C');
  my $cmdfh = opencmd("sort -n $datfile | uniq -c |")
    or $ug->logconfess("create(): failed to open pipe from sort: $!");
  while (defined($_=<$cmdfh>)) {
    chomp;
    ($f,$i) = split(' ',$_,2);
    next if ($i eq ''); ##-- ignore eos
    if ($i==$cur++) {
      $ug->write(pack($packas,$f));
    } else {
      $ug->store($i,$f);
      $cur = $i+1;
    }
    $N += $f;
  }
  $cmdfh->close();
  env_pop();
  $ug->setsize($cur); ##-- set final size
  $ug->{N} = $N;      ##-- store unigram total

  ##-- done
  return $ug;
}

##==============================================================================
## Relation API: union

## $ug = CLASS_OR_OBJECT->union($coldb, \@pairs, %opts)
##  + merge multiple co-frequency indices into new object
##  + @pairs : array of pairs ([$ug,\@xi2u],...)
##    of unigram-objects $ug and tuple-id maps \@xi2u for $ug
##  + %opts: clobber %$ug
##  + implicitly flushes the new index
sub union {
  my ($ug,$coldb,$pairs,%opts) = @_;

  ##-- create/clobber
  $ug = $ug->new() if (!ref($ug));
  @$ug{keys %opts} = values %opts;

  ##-- union guts (in-memory)
  my $N = 0;
  my $udata = [];
  my ($pair,$pug,$pdata,$pi2u,$pxi);
  foreach $pair (@$pairs) {
    ($pug,$pi2u) = @$pair;
    $pi2u  = $pi2u->toArray() if (UNIVERSAL::can($pi2u,'toArray'));
    $pdata = $pug->toArray();
    $pxi   = 0;
    foreach (@$pdata) {
      $udata->[$pi2u->[$pxi++]] += $_;
    }
    $N += $pug->{N};
  }

  ##-- finalize
  $ug->{N} = $N;
  $ug->fromArray($udata)
    or $ug->logconfess("union(): failed to populate from array");
  $ug->flush()
    or $ug->logconfess("union(): failed to flush to disk");

  return $ug;
}

##==============================================================================
## Relation API: default: profiling

## $prf = $ug->subprofile(\@xids, %opts)
##  + get frequency profile for @xids (db must be opened)
##  + %opts:
##     groupby => \&gbsub,  ##-- key-extractor $key2_or_undef = $gbsub->($i2)
sub subprofile {
  my ($ug,$ids,%opts) = @_;
  $ids   = [$ids] if (!UNIVERSAL::isa($ids,'ARRAY'));

  my $fh = $ug->{fh};
  my $packf = $ug->{packas};
  my $reclen = $ug->{reclen};
  my $groupby = $opts{groupby};
  my $pf1 = 0;
  my $pf2 = {};
  my ($i,$f,$key2, $buf);

  foreach $i (@$ids) {
    CORE::seek($fh, $i*$reclen, SEEK_SET) or return undef;
    CORE::read($fh, $buf, $reclen)==$reclen or return undef;
    $f     = unpack($packf,$buf);
    $pf1  += $f;
    $key2  = $groupby ? $groupby->($i) : $i;
    next if (!defined($key2));
    $pf2->{$key2}  += $f
  }

  return DiaColloDB::Profile->new(
				N=>$ug->{N},
				f1=>$pf1,
				f2=>$pf2,
				f12=>{ %$pf2 },
			       );
}

##==============================================================================
## Footer
1;

__END__




