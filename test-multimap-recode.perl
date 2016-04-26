#!/usr/bin/perl -w

use lib qw(. dclib);
use version;
use DiaColloDB;
use DiaColloDB::Utils qw(:sort :regex);
use DiaColloDB::MultiMapFile;
use Getopt::Long qw(:config no_ignore_case);
use Fcntl qw(:seek);
use utf8;
use strict;

BEGIN {
  select(STDERR); $|=1; select(STDOUT); $|=1;
  binmode(STDOUT,':utf8');
}

##======================================================================
## command-line
my ($help);
my $force = 0;
my @cmdline = @ARGV; ##-- for "upgraded" entry
GetOptions(
	   'help|h' => \$help,
	   'force|f!' => \$force,
	  );
if ($help || @ARGV < 1) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DBDIR

Options:
  -help     # this help message
  -force    # force upgrade even if version checks out ok

EOF
  exit $help ? 0 : 1;
}

##======================================================================
## debug messages
use File::Basename;
our $prog = basename($0);

sub vmsg0 {
  print STDERR @_;
}
sub vmsg {
  vmsg0("$prog: ", @_, "\n");
}

##======================================================================
## MAIN

my $dbdir = shift;

DiaColloDB->ensureLog();
my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");

##-- sanity check
my $vmin_compat = '0.09.001';
if (version->parse($coldb->{version}) >= version->parse($vmin_compat)) {
  my $msg = "stored DB version $coldb->{version}' >= minimum compatibible version $vmin_compat";
  if ($force) {
    warn("$0: $msg, but you requested --force");
  } else {
    print STDERR "$0: stored DB version $coldb->{version}' >= minimum compatibible version $vmin_compat -- no upgrade required\n";
    exit 0;
  }
}

##-- convert by attribute
foreach my $attr (@{$coldb->{attrs}}) {
  my $mmf    = $coldb->{"${attr}2x"};
  my $abase  = $mmf->{base};
  my $abase2 = "${abase}2";
  $coldb->info("creating multimap2 file $abase2");

  ##-- convert
  my %mmopts = (pack_i=>$mmf->{pack_i});
  my $mmf2 = DiaColloDB::MultiMapFile->new(flags=>'rw', %mmopts)
    or die("$0: failed to create new DiaColloDB::MultiMapFile object for base=$abase2");
  $mmf2->fromArray($mmf->toArray)
    or die("$0: failed to convert $abase.* to $abase2.*");
  $mmf2->save($abase2)
    or die("$0: failed to save new $abase2.*");
}

##-- update header
$coldb->info("updating header to version $DiaColloDB::VERSION");
my $upgraded = ($coldb->{upgraded} //= []);
unshift(@$upgraded, {
		     version_from=>$coldb->{version},
		     version_to=>$DiaColloDB::VERSION,
		     timestamp=>DiaColloDB::Utils::timestamp(time),
		     by=>basename($0),
		     cmdline=>\@cmdline,
		    });
$coldb->{version} = $DiaColloDB::VERSION;
$coldb->saveHeader()
  or $coldb->logconfess("failed to update header");

##-- all done
exit 0;
