## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::v0_12_sliceN.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: v0.11.x -> v0.12.x: allow slice-wise N

package DiaColloDB::Upgrade::v0_12_sliceN;
use DiaColloDB::Upgrade::Base;
use DiaColloDB::Compat::v0_11;
use DiaColloDB::Utils qw(:pack :env :run :file);
use version;
use strict;
our @ISA = qw(DiaColloDB::Upgrade::Base);

##==============================================================================
## API

## $version = $CLASS_OR_OBJECT->toversion()
##  + returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return '0.12.000';
}

## $bool = $CLASS_OR_OBJECT->upgrade()
##  + performs upgrade
sub upgrade {
  my $up = shift;

  ##-- backup
  $up->backup() or return undef;

  ##-- read header
  my $dbdir = $up->{dbdir};
  my $hdr   = $up->dbheader();

  ##-- convert relations: unigrams : TODO

  ##-- convert relations: cofreqs
  {
    my $cof = DiaColloDB::Relation::Cofreqs->new(base=>"$dbdir/cof", logCompat=>'off')
      or $up->logconfess("failed to open co-frequency index $dbdir/cof.*: $!");
    my %cofopts = (map {($_=>$cof->{$_})} qw(pack_i pack_f fmin dmax));
    $up->info("upgrading co-frequency index $dbdir/cof.*");
    $up->warn("co-frequency data in $dbdir/cof.* doesn't seem to be v0.11 format; trying to upgrade anyways")
      if (!$cof->isa('DiaColloDB::Compat::v0_11::Relation::Cofreqs'));

    ##-- extract total counts by date
    my $r2     = $cof->{r2}; ##-- pf: [$end3,$d1,$f1]*   @ end2($i1-1)..(end2($i1+1)-1)
    my $packas = $r2->{packas};
    my ($buf, $end3,$d,$f);
    my %fN = qw();
    for (my $i=0; $i < $cof->{size2}; ++$i) {
      $r2->read(\$buf);
      ($end3,$d,$f) = unpack($packas,$buf);
      $fN{$d} += $f;
    }

    ##-- create $rN by date
    my @dates  = sort {$a<=>$b} keys %fN;
    my $ymin   = $dates[0];
    my $rN     = $cof->{rN} = DiaColloDB::PackedFile->new(file=>"$dbdir/cof.dbaN", flags=>'rw', perms=>$cof->{perms}, packas=>"$cof->{pack_f}");
    $rN->fromArray([@fN{@dates}]);
    $rN->flush();

    ##-- update header
    $cof->{ymin}    = $ymin;
    $cof->{sizeN}   = $rN->size;
    $cof->{version} = $up->toversion;
    $cof->saveHeader()
      or $up->logconfess("failed to save new header for $dbdir/cof.*");
  }

  ##-- cleanup
  if (0 && !$up->{keep}) {
    $up->info("removing temporary file(s)");
  }

  ##-- update header
  return $up->updateHeader();
}

##==============================================================================
## Backup & Revert

## $bool = $up->backup()
##  + perform backup any files we expect to change to $up->backupdir()
sub backup {
  my $up = shift;
  $up->SUPER::backup() or return undef;
  return 1 if (!$up->{backup});

  my $dbdir = $up->{dbdir};
  my $hdr   = $up->dbheader;
  my $backd = $up->backupdir;

  ##-- backup: xenum
  $up->info("backing up $dbdir/xenum.*");
  copyto_a([glob "$dbdir/xenum.*"], $backd)
      or $up->logconfess("backup failed for $dbdir/xenum.*: $!");

  ##-- backup: by attribute: multimaps
  foreach my $base (map {"$dbdir/${_}_2x"} @{$hdr->{attrs}}) {
    $up->info("backing up $base.*");
    copyto_a([glob "$base.*"], $backd)
      or $up->logconfess("backup failed for $base.*: $!");
  }

  ##-- backup: relations
  foreach my $base (map {"$dbdir/$_"} qw(xf cof)) {
    $up->info("backing up $base.hdr");
    copyto_a([glob "$base.hdr"], $backd)
      or $up->logconfess("backup failed for $base.hdr: $!");
  }

  return 1;
}

## @files = $up->revert_created()
##  + returns list of files created by this upgrade, for use with default revert() implementation
sub revert_created {
  my $up  = shift;
  my $hdr = $up->dbheader;

  return (
	  ##-- unigrams
	  #(map {"xf.$_"} qw(dba1 dba1.hdr dba2 dba2.hdr hdr)),

	  ##-- cofreqs
	  (map {"cof.$_"} qw(dbaN dbaN.hdr)),

	  ##-- header
	  'header.json',
	 );
}

## @files = $up->revert_updated()
##  + returns list of files updated by this upgrade, for use with default revert() implementation
sub revert_updated {
  my $up  = shift;
  my $hdr = $up->dbheader;

  return (
	  ##-- unigrams
	  #(map {"xf.$_"} qw(hdr)),

	  ##-- cofreqs
	  (map {"cof.$_"} qw(hdr)),

	  ##-- header
	  'header.json',
	 );
}


##==============================================================================
## Footer
1; ##-- be happy
