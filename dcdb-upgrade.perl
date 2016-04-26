#!/usr/bin/perl -w

use lib qw(. dclib);
use version;
use DiaColloDB;
use DiaColloDB::Upgrade;
use Getopt::Long qw(:config no_ignore_case);
use utf8;
use strict;

BEGIN {
  select(STDERR); $|=1; select(STDOUT); $|=1;
  binmode(STDOUT,':utf8');
}

##======================================================================
## command-line
my ($help);
my $act = 'upgrade';
my @upgrades = qw();
GetOptions(
	   'help|h' => sub { $act='help' },
	   'list-available|list-all|la|list|all|available|a' => sub { $act='list' },
	   'check|c' => => sub { $act='check' },
	   'upgrade|u' => sub { $act='upgrade' },
	   'force-upgrade|force|fu|f=s' => sub { $act='force'; @upgrades = grep {($_//'') ne ''} split(/[\s\,]+/,$_[1]) },
	  );
if ($act eq 'help' || ($act ne 'list' && @ARGV < 1)) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DBDIR

Options:
  -h, -help       # this help message
  -l, -list       # list all available upgrade packages
  -c, -check      # check applicability of available upgrades
  -u, -upgrade    # apply any applicable upgrades
  -f, -force PKGS # force-apply comma-separated upgrade package(s)

EOF
  exit $help ? 0 : 1;
}

##======================================================================
## MAIN

DiaColloDB->ensureLog();

my $up = 'DiaColloDB::Upgrade';
if ($act eq 'list') {
  ##-- list available upgrades
  print map {"$_\n"} $up->available();
  exit 0;
}

my $timer = DiaColloDB::Timer->start();
my $dbdir = shift;
my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
my (@needed);

if ($act =~ /^(?:check|upgrade)$/) {
  ##-- list required upgrades
  $up->info("checking applicable upgrades for $dbdir");
  @needed = $up->needed($coldb, $up->available);;
  print map {"\t$_\n"} @needed;
  if (!@needed) {
    $up->info("no upgrades applicable for $dbdir");
  }
}

if ($act eq 'upgrade') {
  ##-- apply available upgrades
  $up->upgrade($coldb,@needed)
    or die("$0: upgrade failed");
}
elsif ($act eq 'force') {
  ##-- force-apply selected upgrades
  $up->upgrade($coldb,@upgrades)
    or die("$0: force-upgrade failed");
}

##-- all done
$up->info("operation completed in ", $timer->timestr);
