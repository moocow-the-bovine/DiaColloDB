## -*- Mode: CPerl -*-
## File: DiaColloDB::Unigrams.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, unigram database (using DiaColloDB::PackedFile)

package DiaColloDB::Unigrams;
use DiaColloDB::PackedFile;
use DiaColloDB::Utils qw(:sort :env :run);
use Fcntl qw(:seek);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::PackedFile);

##==============================================================================
## Constructors etc.

## $ug = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##   ##-- PackedFile: user options
##   file     => $filename,   ##-- default: undef (none)
##   flags    => $flags,      ##-- fcntl flags or open-mode (default='r')
##   perms    => $perms,      ##-- creation permissions (default=(0666 &~umask))
##   reclen   => $reclen,     ##-- record-length in bytes: (default: guess from pack format if available)
##   packas   => $packas,     ##-- pack-format or array; see DiaColloDB::Utils::packFilterStore();  ##-- OVERRIDE default='N'
##   ##
##   ##-- PackedFile: filters
##   filter_fetch => $filter, ##-- DB_File-style filter for fetch
##   filter_store => $filter, ##-- DB_File-style filter for store
##   ##
##   ##-- PackedFile: low-level data
##   fh       => $fh,         ##-- underlying filehandle
##   ##
##   ##-- Unigrams: high-level data
##   N        => $N,          ##-- total frequency
##   )
sub new {
  my $that = shift;
  my $ug   = $that->SUPER::new(
			       N=>0,
			       packas=>'N',
			       @_
			      );
  return $ug;
}

##==============================================================================
## API: open/close: INHERITED

##==============================================================================
## API: filters: INHERITED

##==============================================================================
## PackedFile API: positioning: INHERITED

##==============================================================================
## PackedFile API: record access: INHERITED

##==============================================================================
## PackedFile API: text I/O: INHERITED

##==============================================================================
## PackedFile API: tie interface: INHERITED

##==============================================================================
## Relation API: create

## $bool = CLASS_OR_OBJECT->create($tokdat_file,%opts)
##  + populates current database from $tokdat_file,
##    a tt-style text file containing 1 token-id perl line with optional blank lines
##  + %opts: clobber %$ug, also:
##    (
##     size=>$size,  ##-- set initial size
##    )
sub create {
  my ($ug,$datfile,%opts) = @_;

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
## Relation API: profile

## $prf = $ug->profile(\@xids, %opts)
##  + get frequency profile for @xids (db must be opened)
##  + %opts:
##     groupby => \&gbsub,  ##-- key-extractor $key2 = $gbsub->($i2)
sub profile {
  use bytes;
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



