#!/usr/bin/perl -w

use lib qw(. lib dclib);
use DiaColloDB;
use DiaColloDB::Utils qw(:si);
use Getopt::Long qw(:config no_ignore_case);
use Pod::Usage;
use File::Basename qw(basename);
use strict;

use DiaColloDB::Relation::Vsem; ##-- DEBUG

##----------------------------------------------------------------------
## Globals
##----------------------------------------------------------------------

##-- program vars
our $prog       = basename($0);
our ($help,$version);

our %log        = (level=>'TRACE', rootLevel=>'FATAL');
our $dbdir      = undef;

our $globargs = 1; ##-- glob @ARGV?
our $listargs = 0; ##-- args are file-lists?
our $union    = 0; ##-- args are db-dirs?
our $dotime   = 1; ##-- report timing?
our %corpus   = (dclass=>'DDCTabs', dopts=>{});
our %coldb    = (pack_id=>'N', pack_date=>'n', pack_f=>'N', pack_off=>'N', pack_len=>'n', dmax=>5, cfmin=>2, keeptmp=>0, vsopts=>{});

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
	   #'verbose|v=i' => \$verbose,

	   ##-- corpus options
	   'glob|g!' => \$globargs,
	   'list|l!' => \$listargs,
	   'union|u|merge!' => \$union,
	   'document-class|dclass|dc=s' => \$corpus{dclass},
	   'document-option|docoption|do=s%' => \$corpus{dopts},
	   'by-sentence|bysentence' => sub { $corpus{dopts}{eosre}='^$' },
	   'by-paragraph|byparagraph' => sub { $corpus{dopts}{eosre}='^%%\$DDC:BREAK\.p=' },
	   'by-doc|bydoc|by-file|byfile' => sub { $corpus{dopts}{eosre}='' },

	   ##-- coldb options
	   'index-attributes|attributes|attrs|a=s' => \$coldb{attrs},
	   'max-distance|maxd|dmax|n=i' => \$coldb{dmax},
	   'min-cofrequency|min-cf|mincf|cfmin=i' => \$coldb{cfmin},
	   'index-vsem|vsem!' => \$coldb{index_vsem},
	   'vsem-break|vbreak|vb=s' => \$coldb{vbreak},
	   'vsem-option|vsopt|vso|vopt|vo|vO=s%' => \$coldb{vsopts},
	   'keeptmp|keep' => \$coldb{keeptmp},
	   'nofilters|F' => sub { $coldb{$_}=undef foreach (qw(pgood pbad wgood wbad lgood lbad vsmgood vsmbad)); },
	   'option|O=s%' => \%coldb,
	   '64bit|64|quad|Q!'   => sub { pack64( $_[1]); },
	   '32bit|32|long|L|N!' => sub { pack64(!$_[1]); },

	   ##-- I/O and logging
	   'timing|times|time|t!' => \$dotime,
	   'log-level|level|ll=s' => sub { $log{level} = uc($_[1]); },
	   'log-option|logopt|lo=s' => \%log,
	   'output|outdir|od|o=s' => \$dbdir,
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);

if ($version) {
  print STDERR "$prog version $DiaColloDB::VERSION by Bryan Jurish\n";
  exit 0 if ($version);
}


##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------

##-- setup logger
DiaColloDB::Logger->ensureLog(%log);

##-- setup corpus
push(@ARGV,'-') if (!@ARGV);
my $corpus = DiaColloDB::Corpus->new(%corpus);
$corpus->open(\@ARGV, 'glob'=>$globargs, 'list'=>$listargs)
  or die("$prog: failed to open corpus: $!");

##-- create colloc-db
my $coldb = DiaColloDB->new(%coldb)
  or die("$prog: failed to create new DiaColloDB object: $!");
my $timer = DiaColloDB::Timer->start();
if ($union) {
  ##-- union: create from dbdirs
  $coldb->union($corpus->{files}, dbdir=>$dbdir, flags=>'rw')
    or die("$prog: DiaColloDB::union() failed: $!");
} else {
  ##-- !union: create from corpus
  $coldb->create($corpus, dbdir=>$dbdir, flags=>'rw', attrs=>($coldb{attrs}||'l,p'))
    or die("$prog: DiaColloDB::create() failed: $!");
}

##-- cleanup
#my $du = si_str($coldb->du());
$coldb->close();

##-- timing
if ($dotime) {
  (my $du = `du -h "$dbdir"`) =~ s/\s.*\z//s;
  $coldb->info("operation completed in ", $timer->timestr, "; db size = ${du}B");
}

__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

dcdb-create.perl - create a DiaColloDB collocation database from a corpus dump

=head1 SYNOPSIS

 dcdb-create.perl [OPTIONS] [INPUT(s)...]

 General Options:
   -help
   -version
   -[no]time            ##-- do/don't report execution time

 Corpus Options:
   -list , -nolist      ##-- INPUT(s) are/aren't file-lists (default=no)
   -glob , -noglob      ##-- do/don't glob INPUT(s) argument(s) (default=do)
   -dclass CLASS        ##-- set corpus document class (default=DDCTabs)
   -dopt OPT=VAL        ##-- set corpus document option, e.g.
                        ##   eosre=EOSRE        # eos regex (default='^$'; alt. '^%%\$DDC:PAGE=' or '^%%\$DDC:BREAK\.p=')
   -bysent              ##-- track collocations by sentence (default)
   -byparagraph         ##-- track collocations by paragraph
   -bypage              ##-- track collocations by page
   -bydoc               ##-- track collocations by document

 Indexing Options:
   -attrs ATTRS         ##-- select index attributes (default=l,p)
                        ##   known attributes: l, p, w, doc.title, ...
   -[no]keep            ##-- do/ton't keep temporary files (default=don't)
   -nofilters           ##-- disable default regex-filters
   -64bit               ##-- use 64-bit quads where available
   -32bit               ##-- use 32-bit integers where available
   -dmax DIST           ##-- maximum distance for collocation-frequencies (default=5)
   -cfmin CFMIN         ##-- minimum relation co-occurrence frequency (default=2)
   -option OPT=VAL      ##-- set arbitrary DiaColloDB option, e.g.
                        ##   pack_id=PACKFMT       # pack-format for IDs
                        ##   pack_f=PACKFMT        # pack-format for frequencies
                        ##   pack_date=PACKFMT     # pack-format for dates
                        ##   bos=STR               # bos string
                        ##   eos=STR               # eos string
                        ##   (p|w|l|vsm)good=REGEX # positive regex for (postags|words|lemmata|vsem-metadata)
                        ##   (p|w|l|vsm)bad=REGEX  # negative regex for (postags|words|lemmata|vsem-metadata)
                        ##   ddcServer=HOST:PORT   # server for ddc relations
                        ##   ddcTimeout=SECONDS    # timeout for ddc relations

 I/O and Logging Options:
   -log-level LEVEL     ##-- set log-level (default=TRACE)
   -log-option OPT=VAL  ##-- set log option (e.g. logdate, logtime, file, syslog, stderr, ...)
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
