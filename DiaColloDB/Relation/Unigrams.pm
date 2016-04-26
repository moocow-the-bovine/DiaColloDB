## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Unigrams.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: unigram database (using DiaColloDB::PackedFile)

package DiaColloDB::Relation::Unigrams;
use DiaColloDB::Relation;
use DiaColloDB::PackedFile;
use DiaColloDB::Utils qw(:sort :env :run :pack :file);
use Fcntl qw(:seek);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::PackedFile DiaColloDB::Relation);

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
  my $ug   = $that->DiaColloDB::PackedFile::new(
						N=>0,
						packas=>'N',
						@_
					       );
  return $ug;
}

##==============================================================================
## Persistent API: disk usage

## @files = $obj->diskFiles()
##  + returns disk storage files, used by du() and timestamp()
sub diskFiles {
  return ($_[0]{file}, "$_[0]{file}.hdr");
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

## $ug = $CLASS_OR_OBJECT->create($coldb,$tokdat_file,%opts)
##  + populates current database from $tokdat_file,
##    a tt-style text file containing 1 token-id perl line with optional blank lines
##  + %opts: clobber %$ug, also:
##    (
##     size=>$size,  ##-- set initial size
##    )
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
## Relation API: dbinfo

## \%info = $rel->dbinfo($coldb)
##  + embedded info-hash for $coldb->dbinfo()
sub dbinfo {
  my $ug = shift;
  my $info = $ug->SUPER::dbinfo();
  $info->{N} = $ug->{N};
  $info->{size} = $ug->size();
  return $info;
}


##==============================================================================
## Relation API: default: profiling

## $prf = $ug->subprofile1(\@xids, %opts)
##  + get frequency profile for @xids (db must be opened)
##  + %opts:
##     groupby => \&gbsub,  ##-- key-extractor $key2_or_undef = $gbsub->($i2)
sub subprofile1 {
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
## Relation API: default: query info

## \%qinfo = $rel->qinfo($coldb, %opts)
##  + get query-info hash for profile administrivia (ddc hit links)
##  + %opts: as for profile(), additionally:
##    (
##     qreqs => \@qreqs,      ##-- as returned by $coldb->parseRequest($opts{query})
##     gbreq => \%groupby,    ##-- as returned by $coldb->groupby($opts{groupby})
##    )
sub qinfo {
  my ($rel,$coldb,%opts) = @_;
  my ($q1strs,$q2strs,$qxstrs,$fstrs) = $rel->qinfoData($coldb,%opts);

  my @qstrs = (@$q1strs, @$q2strs, @$qxstrs);
  @qstrs    = ('*') if (!@qstrs);
  my $qstr = ('('.join(' WITH ', @qstrs).') =1'
	      .' #SEPARATE'
	      .(@$fstrs ? (' '.join(' ',@$fstrs)) : ''),
	     );
  return {
	  fcoef => 1,
	  qtemplate => $qstr,
	 };
}

##==============================================================================
## Pacakge Alias(es)
package DiaColloDB::Unigrams;
use strict;
our @ISA = qw(DiaColloDB::Relation::Unigrams);

##==============================================================================
## Footer
1;

__END__
