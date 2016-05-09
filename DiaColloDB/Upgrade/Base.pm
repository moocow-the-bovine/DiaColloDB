## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Upgrade::Base.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: auto-magic upgrade: base class / API

package DiaColloDB::Upgrade::Base;
use DiaColloDB::Logger;
use DiaColloDB::Utils qw(:time);
use Carp;
use version;
use strict;
our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## API

## $up = $CLASS_OR_OBJECT->new($dbdir?, %opts)
##  + create a new upgrader for local DB directory $dbdir
##  + if $dbdir is specified, it is stored in $up->{dbdir} and its header is loaded to $up->{hdr}
##  + common %opts, %$up:
##    (
##     backup=>$bool,     ##-- perform auto-backup? (default=1)
##     keep => $bool,     ##-- keep temporary files? (default=0)
##     timestamp=>$stamp, ##-- timestamp of this upgrade operation (default:DiaColloDB::Utils::timestamp(time))
##    )
sub new {
  my $that  = shift;
  my $dbdir = scalar(@_)%2==0 ? undef : shift;
  my $up = bless({
		  dbdir=>$dbdir,
		  backup=>1,
		  keep=>0,
		  timestamp=>DiaColloDB::Utils::timestamp(time),
		  @_,
		 }, ref($that)||$that);

  ##-- load header if available
  $up->{hdr} = $up->dbheader($up->{dbdir}) if (defined($up->{dbdir}));
  return $up;
}

## $pkg = $CLASS_OR_OBJECT->label()
##  + returns upgrade package name
sub label {
  return ref($_[0])||$_[0];
}

## $version = $up->toversion()
##  + (reccommonded): returns default target version; default just returns $DiaColloDB::VERSION
sub toversion {
  return $DiaColloDB::VERSION;
}

## $bool = $up->needed()
##  + returns true iff local index in $up->{dbdir} needs upgrade
##  + default implementation returns true iff $coldb->{version} is less than $CLASS_OR_OBJECT->toversion()
sub needed {
  my $up = shift;
  my $header = $up->dbheader();
  return version->parse($header->{version}//'0.0.0') < version->parse($up->toversion);
}

## $bool = $up->upgrade()
##  + performs upgrade in-place on $up->{dbdir}
sub upgrade {
  $_[0]->logconfess("ugprade() method not implemented");
}

##==============================================================================
## Backups

## $bool = $up->backup()
##  + perform backup any files we expect to change to $up->backupdir()
##  + subclasses should call this from $up->upgrade()
##  + default implementation just backs up "$dbdir/header.json", emitting a warning
##    if $up->{backup} is false
sub backup {
  my $up = shift;

  ##-- were backups requested?
  if (!$up->{backup}) {
    $up->warn("backup(): backups disabled by user request");
    return 1;
  }

  ##-- backup db-header
  my $dbdir  = $up->{dbdir};
  my $backd  = $up->backupdir;
  $up->info("using backup directory $backd/");
  $up->info("backing up $dbdir/header.json");
  DiaColloDB::Utils::copyto_a("$dbdir/header.json", $backd)
      or $up->logconfess("updateHeader(): failed to backup header to $backd/: $!");

  return 1;
}

## $dir = $up->backupdir()
##  + returns name of a backup directory for this upgrade
sub backupdir {
  my $up = shift;
  my ($dbdir,$stamp) = @$up{qw(dbdir timestamp)};
  $stamp =~ s/\W//g;
  $stamp =~ s/T/_/;
  (my $suffix = $up->label."_$stamp") =~ s/^DiaColloDB::Upgrade:://;
  return "$dbdir/upgrade_$suffix";
}


##==============================================================================
## Utilities

## \%hdr = $CLASS_OR_OBJECT->dbheader($dbdir?)
##  + reads $dbdir/header.json
##  + default uses cached $CLASS_OR_OBJECT->{hdr} if available
sub dbheader {
  my ($up,$dbdir) = @_;
  $dbdir //= $up->{dbdir} if (ref($up));
  return $up->{hdr}
    if (ref($up) && defined($up->{hdr}) && ($up->{dbdir}//'') eq $dbdir);
  my $hdr = DiaColloDB::Utils::loadJsonFile("$dbdir/header.json")
      or $up->logconfess("dbheader(): failed to read header $dbdir/header.json: $!");
  return $hdr;
}

## \%uinfo = $up->uinfo($dbdir?,%info)
##  + returns a default upgrade-info structure for %info
##  + conventional keys %uinfo =
##    (
##     version_from => $vfrom,    ##-- source version (default='unknown')
##     version_to   => $vto,      ##-- target version (default=$CLASS_OR_OBJECT->_toversion)
##     timestamp    => $time,     ##-- timestamp (default=$up->{timestamp} || DiaColloDB::Utils::timestamp(time))
##     by           => $who,      ##-- user or script-name (default=$CLASS)
##    )
sub uinfo {
  my $up    = shift;
  my $dbdir = ((scalar(@_)%2)==0 ? undef : shift) // $up->{hdr};
  my $header = $up->{hdr} // ($dbdir ? $up->dbheader($dbdir) : {});
  return {
	  version_from=>($header->{version} // 'unknown'),
	  version_to=>$up->toversion,
	  timestamp=>($up->{timestamp} || DiaColloDB::Utils::timestamp(time)),
	  by=>$up->label,
	  @_
	 };
}

## $bool = $up->updateHeader(\%extra_uinfo, \%extra_header_data)
##  + updates header $dbdir/header.json, creating backup if requested
sub updateHeader {
  my ($up,$xinfo,$xhdr) = @_;
  my $dbdir = $up->{dbdir};

  ##-- backup old header if requested
  !$up->{backup}
    or DiaColloDB::Utils::copyto("$dbdir/header.json", $up->backupdir)
    or $up->logconfess("updateHeader(): failed to backup header to ".$up->backupdir.": $!");

  ##-- get upgrade info
  my $uinfo = $up->uinfo($dbdir, %{$xinfo//{}});
  return if (!defined($uinfo)); ##-- silent upgrade

  my $header   = $up->dbheader($dbdir);
  my $upgraded = ($header->{upgraded} //= []);
  unshift(@$upgraded, $uinfo);
  $header->{version} = $uinfo->{version_to} if ($uinfo->{version_to});
  @$header{keys %$xhdr} = values %$xhdr if ($xhdr);
  DiaColloDB::Utils::saveJsonFile($header, "$dbdir/header.json")
      or $up->logconfess("updateHeader(): failed to save header data to $dbdir/header.json: $!");
  return $up;
}


##==============================================================================
## Footer
1; ##-- be happy
