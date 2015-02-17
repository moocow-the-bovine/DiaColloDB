#!/usr/bin/perl -w

use lib '.';
use CollocDB;
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use File::Basename qw(basename);
use strict;

##----------------------------------------------------------------------
## Globals
##----------------------------------------------------------------------

##-- program vars
our $prog       = basename($0);
our $verbose    = 1;
our ($help,$version);

our $dbdir        = undef;

our $globargs = 1; ##-- glob @ARGV?
our $listargs = 0; ##-- args are file-lists?
our %corpus   = (dclass=>'DDCTabs');
our %coldb    = (pack_id=>'N', pack_date=>'n', pack_f=>'N', pack_off=>'N', pack_len=>'n', dmax=>5, cfmin=>2, keeptmp=>0);

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
sub pack64 {
  $coldb{$_}=($_[1] ? 'Q>' : 'N') foreach qw(pack_id pack_f pack_off);
  $coldb{pack_len}=($_[1] ? 'n' : 'N');
}
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,
	   'verbose|v=i' => \$verbose,

	   ##-- I/O
	   'glob|g!' => \$globargs,
	   'list|l!' => \$listargs,
	   'document-class|dclass|dc=s' => \$corpus{dclass},
	   'output|outdir|od|o=s' => \$dbdir,

	   ##-- logging
	   'log-level|level|ll=s' => sub { $CollocDB::Logger::MIN_LEVEL = uc($_[1]); },

	   ##-- coldb options
	   'max-distance|maxd|dmax|n=i' => \$coldb{dmax},
	   'min-cofrequency|min-cf|mincf|cfmin=i' => \$coldb{cfmin},
	   #'index-words|words|iw!' => \$coldb{index_w},
	   #'index-lemmata|index-lemmas|lemmata|lemmas|il!' => \$coldb{index_l},
	   'keeptmp|keep' => \$coldb{keeptmp},
	   'nofilters|F' => sub { $coldb{$_}=undef foreach (qw(pgood pbad wgood wbad lgood lbad)); },
	   'option|O=s%' => \%coldb,
	   '64bit|64|quad|Q!'   => sub { pack64( $_[1]); },
	   '32bit|32|long|L|N!' => sub { pack64(!$_[1]); },
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);

if ($version || $verbose >= 2) {
  print STDERR "$prog version $CollocDB::VERSION by Bryan Jurish\n";
  exit 0 if ($version);
}


##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------

##-- setup logger
CollocDB::Logger->ensureLog();

##-- setup corpus
push(@ARGV,'-') if (!@ARGV);
my $corpus = CollocDB::Corpus->new(%corpus);
$corpus->open(\@ARGV, 'glob'=>$globargs, 'list'=>$listargs)
  or die("$prog: failed to open corpus: $!");

##-- create colloc-db
my $coldb = CollocDB->new(%coldb)
  or die("$prog: failed to create new CollocDB object: $!");
$coldb->create($corpus, dbdir=>$dbdir, flags=>'rw')
  or die("$prog: CollocDB::create() failed: $!");


__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

coldb-create.perl - create a CollocDB collocation database from a corpus dump

=head1 SYNOPSIS

 coldb-create.perl [OPTIONS] [INPUT(s)...]

 General Options:
   -help
   -version
   -verbose LEVEL

 Corpus Options:
   -list , -nolist      ##-- INPUT(s) are/aren't file-lists (default=no)
   -glob , -noglob      ##-- do/don't glob INPUT(s) argument(s) (default=do)
   -dclass CLASS        ##-- set corpus document class (default=DDCTabs)

 CollocDB Options:
   #-[no]index-w         ##-- do/don't index words (default=don't) : OBSOLETE
   #-[no]index-l         ##-- do/don't index lemmata (default=do)  : OBSOLETE
   -[no]keep            ##-- do/ton't keep temporary files (default=don't)
   -nofilters           ##-- disable default regex-filters
   -64bit               ##-- use 64-bit quads where available
   -32bit               ##-- use 32-bit integers where available
   -dmax DIST           ##-- maximum distance for collocation-frequencies (default=5)
   -cfmin CFMIN         ##-- minimum relation co-occurrence frequency (default=2)
   -option OPT=VAL      ##-- set arbitrary CollocDB option, e.g.
                        ##   pack_id=PACKFMT    # pack-format for IDs
                        ##   pack_f=PACKFMT     # pack-format for frequencies
                        ##   pack_date=PACKFMT  # pack-format for dates
                        ##   bos=STR            # bos string
                        ##   eos=STR            # eos string
                        ##   (p|w|l)good=REGEX  # positive regex for (postags|words|lemmata)
                        ##   (p|w|l)bad=REGEX   # negative regex for (postags|words|lemmata)

 I/O and Logging Options:
   -log-level LEVEL     ##-- set log-level (default=TRACE)
   -output DIR          ##-- output directory (required)

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
