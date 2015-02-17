#!/usr/bin/perl -w

use lib '.';
use DiaColloDB;
use DiaColloDB::Utils qw(:json);
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use File::Basename qw(basename);
use strict;

BEGIN {
  select STDERR; $|=1; select STDOUT;
}

##----------------------------------------------------------------------
## Globals
##----------------------------------------------------------------------

##-- program vars
our $prog       = basename($0);
our $verbose    = 1;
our ($help,$version);

our %log        = (level=>'TRACE', rootLevel=>'FATAL');
our $dbdir      = undef;
our %coldb      = (flags=>'r');

our $rel = 'cof';
our %profile = (
		lemma =>'',	##-- selected lemma(ta)
		date  =>undef,  ##-- selected date(s)
		slice =>1,      ##-- date slice
		score =>'f',    ##-- score func
		kbest =>undef,  ##-- k-best items per date
		cutoff =>undef, ##-- minimum score cutoff
		strings => 1,    ##-- debug: want strings?
	       );

our $outfmt  = 'text'; ##-- output format: 'text' or 'json'
our $pretty  = 1;

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,
	   'verbose|v=i' => \$verbose,

	   ##-- general
	   'log-level|level|ll=s' => sub { $log{level} = uc($_[1]); },
	   'option|O=s%' => \%coldb,

	   ##-- local
	   'collocations|collocs|cofreqs|cof|co' => sub { $rel='cof' },
	   'unigrams|ug|u' => sub { $rel='xf' },
	   'date|d=s'   => \$profile{date},
	   'date-slice|ds=s'  => \$profile{slice},
	   'mutual-information|mi'    => sub {$profile{score}='mi'},
	   'log-dice|logdice|ld|dice' => sub {$profile{score}='ld'},
	   'frequency|freq|f'         => sub {$profile{score}='f'},
	   'k-best|kbest|k=i' => \$profile{kbest},
	   'no-k-best|nokbest|nok' => sub {$profile{kbest}=undef},
	   'cutoff|C=f' => \$profile{cutoff},
	   'no-cutoff|nocutoff|noc' => sub {$profile{cutoff}=undef},
	   'strings|S!' => \$profile{strings},

	   ##-- I/O
	   'text|t' => sub {$outfmt='text'},
	   'json|j' => sub {$outfmt='json'},
	   'pretty|p!' => sub {$pretty=$_[1]},
	   'null|noout' => sub {$outfmt=''},
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBDIR specified!"}) if (@ARGV<1);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no LEMMA(s) specified!"}) if (@ARGV<2);

if ($version || $verbose >= 2) {
  print STDERR "$prog version $DiaColloDB::VERSION by Bryan Jurish\n";
  exit 0 if ($version);
}


##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------

##-- setup logger
DiaColloDB::Logger->ensureLog(%log);

##-- open colloc-db
$dbdir = shift(@ARGV);
my $coldb = DiaColloDB->new(%coldb)
  or die("$prog: failed to create new DiaColloDB object: $!");
$coldb->open($dbdir)
  or die("$prog: DiaColloDB::open() failed for '$dbdir': $!");

##-- get profile
$profile{lemma} = join(' ',@ARGV);
my $mp = $coldb->profile($rel, %profile)
  or die("$prog: profile() failed for relation '$rel', lemma(s) '$profile{lemma}': $!");

##-- dump stringified profile
if ($outfmt eq 'text') {
  $mp->trace("saveTextFile()");
  $mp->saveTextFile('-');
}
elsif ($outfmt eq 'json') {
  $mp->trace("saveJsonFile()");
  DiaColloDB::Utils::saveJsonFile($mp, '-', utf8=>0,pretty=>$pretty,canonical=>$pretty);
}
#$coldb->trace("done.");


__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

coldb-profile.perl - get a frequency profile from a DiaColloDB

=head1 SYNOPSIS

 coldb-profile.perl [OPTIONS] DBDIR LEMMA(S)...

 General Options:
   -help
   -version
   -verbose LEVEL

 DiaColloDB Options:
   -log-level LEVEL     # set minimum DiaColloDB log-level
   -O KEY=VALUE         # set DiaColloDB option

 Profiling Options:
   -collocs , -unigrams # select profile type (collocations or unigrams; default=-collocs)
   -date DATES          # set target DATE or /REGEX/ or MIN-MAX
   -slice SLICE         # set target date slice (default=1)
   -freq , -mi , -ld    # set scoring function (default=-f)
   -kbest KBEST         # return only KBEST items per date-slice (default=all)
   -nokbest             # disable k-best pruning
   -cutoff CUTOFF       # set minimum score for returned items (default=none)
   -nocutoff            # disable cutoff pruning
   -[no]strings         # debug: do/don't stringify returned profile (default=do)

 I/O Options:
   -text		# use text output (default)
   -json                # use json output
   -[no]pretty          # do/don't pretty-print json output (default=do)
   -null                # don't output profile at all

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
