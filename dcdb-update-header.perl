#!/usr/bin/perl -w

use lib '.';
use DiaColloDB;
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use File::Basename qw(basename);
use strict;

##----------------------------------------------------------------------
## Globals
##----------------------------------------------------------------------

##-- program vars
our $prog       = basename($0);
our ($help,$version);

our $dbdir      = undef;
our $outdir     = undef;
our %coldb      = (flags=>'r');
our $dotime     = 1; ##-- report timing?

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,
	   #'verbose|v=i' => \$verbose,
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBDIR specified!"}) if (!@ARGV);

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
$dbdir = shift(@ARGV);
$dbdir =~ s{/$}{};
my $coldb = DiaColloDB->new(%coldb)
  or die("$prog: failed to create new DiaColloDB object: $!");
$coldb->open($dbdir)
  or die("$prog: DiaColloDB::open() failed for '$dbdir': $!");

##-- save header
$coldb->saveHeader()
  or die("$prog: saveHeader() failed for '$dbdir': $!");

##-- cleanup
$coldb->close();

__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

dcdb-update-header.perl - update header for a DiaColloDB index (open + saveHeader + close)

=head1 SYNOPSIS

 dcdb-update-header.perl [OPTIONS] DBDIR

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
