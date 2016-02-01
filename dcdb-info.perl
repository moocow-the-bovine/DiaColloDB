#!/usr/bin/perl -w

use lib qw(. lib);
use DiaColloDB;
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use File::Basename qw(basename);
use strict;

#use DiaColloDB::Relation::TDF; ##-- DEBUG

##----------------------------------------------------------------------
## Globals
##----------------------------------------------------------------------

##-- program vars
our $prog       = basename($0);
our ($help,$version);

our $dburl      = undef;
our $outdir     = undef;
our %cli        = (flags=>'r');

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,
	   #'verbose|v=i' => \$verbose,
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBURL specified!"}) if (!@ARGV);

if ($version) {
  print STDERR "$prog version $DiaColloDB::VERSION by Bryan Jurish\n";
  exit 0 if ($version);
}


##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------

##-- setup logger
DiaColloDB::Logger->ensureLog();

##-- open colloc-db
$dburl = shift(@ARGV);
my ($cli);
if ($dburl !~ m{^[a-zA-Z]+://}) {
  ##-- hack for local directory URLs without scheme
  $cli = DiaColloDB->new(dbdir=>$dburl,%cli);
} else {
  ##-- use client interface for any URL with a scheme
  $cli = DiaColloDB::Client->new($dburl,%cli);
}
die("$prog: failed to create new DiaColloDB::Client object for $dburl: $!") if (!$cli);

##-- get info
my $info = $cli->dbinfo()
  or die("$prog: dbinfo() failed for '$dburl': $cli->{error}");

##-- cleanup
$cli->close();

##-- dump info
DiaColloDB::Utils::saveJsonFile($info,'-');

__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

dcdb-info.perl - get administrative info from a DiaColloDB object

=head1 SYNOPSIS

 dcdb-info.perl [OPTIONS] DBDIR

 General Options:
   -help
   -version

=cut

###############################################################
## OPTIONS
###############################################################
=pod

=head1 OPTIONS

=cut

###############################################################
# General Options
###############################################################
=pod

=head2 General Options

=over 4

=item -help

Display a brief help message and exit.

=item -version

Display version information and exit.

=item -verbose LEVEL

Set verbosity level to LEVEL.  Default=1.

=back

=cut


###############################################################
# Other Options
###############################################################
=pod

=head2 Other Options

=over 4

=item -someoptions ARG

Example option.

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

perl(1).

=cut
