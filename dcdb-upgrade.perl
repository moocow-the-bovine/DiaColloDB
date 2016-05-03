#!/usr/bin/perl -w

use lib qw(. dclib);
use version;
use DiaColloDB;
use DiaColloDB::Upgrade;
use File::Basename qw(basename);
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use utf8;
use strict;

BEGIN {
  select(STDERR); $|=1; select(STDOUT); $|=1;
  binmode(STDOUT,':utf8');
}

##======================================================================
## Globals

##-- program vars
our $prog  = basename($0);

##======================================================================
## command-line
my $act = 'upgrade';
my @upgrades = qw();
GetOptions(
	   'help|h' => sub { $act='help' },
	   'list-available|list-all|la|list|all|available' => sub { $act='list' },
	   'check|c' => => sub { $act='check' },
	   'upgrade|u' => sub { $act='upgrade' },
	   'force-apply|fa|apply|a=s' => sub { $act='apply'; @upgrades = grep {($_//'') ne ''} split(/[\s\,]+/,$_[1]) },
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($act eq 'help');
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBDIR specified!"}) if ($act ne 'list' && @ARGV < 1);
warn("$prog: WARNING: too many arguments for -list mode") if ($act eq 'list' && @ARGV);

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
$dbdir    =~ s{/+$}{};
my (@needed);

if ($act =~ /^(?:check|upgrade)$/) {
  ##-- list required upgrades
  $up->info("checking applicable upgrades for $dbdir");
  @needed = $up->needed($dbdir, $up->available);;
  print map {"\t$_\n"} @needed;
  if (!@needed) {
    $up->info("no upgrades applicable for $dbdir");
  }
}

if ($act eq 'upgrade') {
  ##-- apply available upgrades
  $up->upgrade($dbdir,@needed)
    or die("$0: upgrade failed for $dbdir");
}
elsif ($act eq 'apply') {
  ##-- force-apply selected upgrades
  $up->upgrade($dbdir,@upgrades)
    or die("$0: force-apply upgrade(s) failed");
}

##-- all done
$up->info("operation completed in ", $timer->timestr);

__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

dcdb-upgrade.perl - upgrade a DiaColloDB directory in-place

=head1 SYNOPSIS

 dcdb-upgrade.perl -list
 dcdb-upgrade.perl [OPTIONS] DBDIR

 Options:
   -h, -help       # this help message
   -l, -list       # list all available upgrade packages
   -c, -check      # check applicability of available upgrades for DBDIR
   -u, -upgrade    # apply any applicable upgrades to DBDIR (default)
   -a, -apply PKGS # force-apply comma-separated upgrade package(s) to DBDIR

=cut

###############################################################
## DESCRIPTION
###############################################################
=pod

=head1 DESCRIPTION

dcdb-upgrade.perl
checks for & applies automatic upgrades to a L<DiaColloDB|DiaColloDB>
database directory, using the L<DiaColloDB::Upgrade|DiaColloDB::Upgrade> API.
The DBDIR database is altered in-place, so it is safest
to make a backup of DBDIR before upgrading.

=cut

###############################################################
## OPTIONS AND ARGUMENTS
###############################################################
=pod

=head1 OPTIONS AND ARGUMENTS

=cut

###############################################################
# Arguments
###############################################################
=pod

=head2 Arguments

=over 4

=item DBDIR

L<DiaColloDB|DiaColloDB> database directory to be checked and/or upgraded.

=back

=cut

###############################################################
# Options
###############################################################
=pod

=head2 Options

=over 4

=item -h, -help

Display a brief help message and exit.

=item -l, -list

List all known L<DiaColloDB::Upgrade|DiaColloDB::Upgrade> packages.

=item -c, -check

Check applicability of available upgrades to C<DBDIR>.

=item -u, -upgrade

Apply any applicable upgrades to F<DBDIR>;
this is the default mode of operation.
It is safest to make a backup of F<DBDIR> before upgrading.

=item -a, -apply PKGS

Force-apply the comma- or space-separated list of
L<DiaColloDB::Upgrade|DiaColloDB::Upgrade>-compliant packages
C<PKGS> to F<DBDIR>.
Use with caution, no applicability checking is performed in this mode.

=back

=cut


###############################################################
# Bugs and Limitations
###############################################################
=pod

=head1 BUGS AND LIMITATIONS

Probably many.

=cut


###############################################################
# Footer
###############################################################
=pod

=head1 ACKNOWLEDGEMENTS

Perl by Larry Wall.

=head1 AUTHOR

Bryan Jurish E<lt>moocow@cpan.orgE<gt>

=head1 SEE ALSO

L<DiaColloDB::Upgrade(3pm)|DiaColloDB::Upgrade>,
L<DiaColloDB(3pm)|DiaColloDB>,
L<dcdb-info.perl(1)|dcdb-info.perl>,
perl(1).

=cut
