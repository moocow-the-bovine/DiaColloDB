#!/usr/bin/perl -w

use lib qw(. ./blib/lib ./blib/arch lib lib/blib/lib lib/blib/arch);
use DiaColloDB;
use DiaColloDB::Corpus::Compiled;
use DiaColloDB::Utils qw(:si);
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

our %log        = (level=>'TRACE', rootLevel=>'FATAL');

our $globargs   = 0; ##-- glob input corpus @ARGV?
our $listargs   = 0; ##-- input corpus args are file-lists?
our $dotime     = 1; ##-- report timing?

our $outbase    = undef; ##-- required

our %icorpus    = (dclass=>'DDCTabs', dopts=>{});
our %filters    =
  (
   pgood => $DiaColloDB::PGOOD_DEFAULT,
   pbad  => $DiaColloDB::PBAD_DEFAULT,
   wgood => $DiaColloDB::WGOOD_DEFAULT,
   wbad  => $DiaColloDB::WBAD_DEFAULT,
   lgood => $DiaColloDB::LGOOD_DEFAULT,
   lbad  => $DiaColloDB::LBAD_DEFAULT,
   (map {("${_}file"=>undef)} qw(pgood pbad wgood wbad lgood lbad)),
  );
our %ocorpus    = (
                   base    => undef,
                   njobs   => 0,
                   filters => \%filters,
                  );

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
foreach (@ARGV) { utf8::decode($_) if (!utf8::is_utf8($_)); }
GetOptions(##-- general
	   'h|help' => \$help,
	   'V|version' => \$version,
	   #'verbose|v=i' => \$verbose,
           'j|jobs|njobs|nj=i' => \$ocorpus{njobs},

	   ##-- input corpus options
	   'g|glob!' => \$globargs,
	   'l|list!' => \$listargs,
	   'c|document-class|dclass|dc=s' => \$icorpus{dclass},
	   'd|document-option|docoption|dopt|do|dO=s%' => \$icorpus{dopts},
	   'by-sentence|bysentence' => sub { $icorpus{dopts}{eosre}='^$' },
	   'by-paragraph|byparagraph' => sub { $icorpus{dopts}{eosre}='^%%\$DDC:BREAK\.p=' },
	   'by-doc|bydoc|by-file|byfile' => sub { $icorpus{dopts}{eosre}='' },

	   ##-- filter options
           'f|filter=s%' => \%filters,
           'F|nofilters|no-filters|all|A|no-prune|noprune|use-all-the-data' => sub { %filters = qw() },

	   ##-- I/O and logging
           'o|out|output|output-corpus=s' => \$outbase,
	   't|timing|times|time!' => \$dotime,
           'lf|log-file|logfile=s' => \$log{file},
	   'll|log-level|level=s' => sub { $log{level} = uc($_[1]); },
	   'lo|log-option|logopt=s' => \%log,
	  );

if ($version) {
  print STDERR "$prog version $DiaColloDB::VERSION by Bryan Jurish\n";
  exit 0 if ($version);
}
pod2usage({-exitval=>0,-verbose=>0}) if ($help);
die("$prog: ERROR: no output corpus basename specified: use the -output (-o) option!\n") if (!defined($outbase));


##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------

##-- setup logger
DiaColloDB::Logger->ensureLog(%log);
my $logger = 'DiaColloDB::Logger';
my $timer  = DiaColloDB::Timer->start();

##-- open input corpus
push(@ARGV,'-') if (!@ARGV);
my $icorpus = DiaColloDB::Corpus->new(%icorpus);
$icorpus->open(\@ARGV, 'glob'=>$globargs, 'list'=>$listargs)
  or die("$prog: failed to open input corpus: $!");

##-- compile input corpus
my $ocorpus = $icorpus->compile($outbase, %ocorpus)
  or die("$prog: failed to compile output corpus '$outbase'.* from input corpus");

##-- cleanup
$icorpus->close() if ($icorpus);
$ocorpus->close() if ($ocorpus);

##-- timing
if ($dotime) {
  (my $du = `du -h "$outbase.hdr" "$outbase.d"`) =~ s/\s.*\z//s;
  $logger->info("operation completed in ", $timer->timestr, "; compiled corpus size = ${du}B");
}

__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

dcdb-corpus-compile.perl - pre-compile a DiaColloDB corpus

=head1 SYNOPSIS

 dcdb-corpus-compile.perl [OPTIONS] [INPUT(s)...]

 General Options:
   -h, -help            # this help message
   -V, -version         # report version information and exit
   -j, -jobs NJOBS      # set number of worker threads (default=0: pure serial)

 Input Corpus Options:
   -l, -[no]list        # INPUT(s) are/aren't file-lists (default=no)
   -g, -[no]glob        # do/don't glob INPUT(s) argument(s) (default=don't)
   -c, -dclass CLASS    # set corpus document class (default=DDCTabs)
   -d, -dopt OPT=VAL    # set corpus document option, e.g.
                        #   eosre=EOSRE  # eos regex (default='^$')
                        #   foreign=BOOL # disable D*-specific heuristics
       -bysent          # default split by sentences (default)
       -byparagraph     # default split by paragraphs
       -bypage          # default split by page
       -bydoc           # default split by document

 Content Filter Options:
   -f, -filter KEY=VAL  # set filter option for KEY = (p|w|l)(bad|good)(_file)?
                        #   (p|w|l)good=REGEX      # positive regex for (postags|words|lemmata)
                        #   (p|w|l)bad=REGEX       # negative regex for (postags|words|lemmata)
                        #   (p|w|l)goodfile=FILE   # positive list-file for (postags|words|lemmata)
                        #   (p|w|l)badfile=FILE    # negative list-file for (postags|words|lemmata)
   -F, -nofilters       # clear all filter options

 I/O and Logging Options:
   -ll, -log-level LVL  # set log-level (default=TRACE)
   -lo, -log-option K=V # set log option (e.g. logdate, logtime, file, syslog, stderr, ...)
   -t,  -[no]times      # do/don't report operating timing (default=do)
   -o,  -output OUTBASE # set output corpus basename (required)

=cut

###############################################################
## DESCRIPTION
###############################################################
=pod

=head1 DESCRIPTION

dcdb-corpus-compile.perl pre-compiles a L<DiaColloDB::Corpus::Compiled|DiaColloDB::Corpus::Compiled>
from a tokenized and annotated input corpus represented as a L<DiaColloDB::Corpus|DiaColloDB::Corpus>
object, optionally applying content filters such as stopword lists etc.
The resulting compiled corpus can be used with L<dcdb-create.perl(1)|dcdb-create.perl>
to compile a L<DiaColloDB|DiaColloDB> collocation database.


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

=item INPUT(s)

File(s), glob(s), or file-list(s) to be compiled.
Interpretation depends on the L<-glob|/-glob> and L<-list|/-list>
options.

=back

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

=item -jobs NJOBS

Run C<NJOBS> parallel compilation threads.
Default (0) runs only a single thread.

=back

=cut


###############################################################
# Input Corpus Options
=pod

=head2 Input Corpus Options

=over 4

=item -list

=item -nolist

Do/don't treat INPUT(s) as file-lists rather than corpus data files.
Default=don't.

=item -glob

=item -noglob

Do/don't expand wildcards in INPUT(s).
Default=do.

=item -dclass CLASS

Set corpus document class (default=DDCTabs).
See L<DiaColloDB::Document/SUBCLASSES> for a list
of supported input formats.
If you are using the default L<DDCTabs|DiaColloDB::Document::DDCTabs> document class
on your own (non-D*) corpus, you may also want to specify
L<C<-dopt foreign=1>|/"-dopt OPT=VAL">.

Aliases: -c, -document-class, -dclass, -dc

=item -dopt OPT=VAL

Set corpus document option, e.g.
L<C<-dopt eosre=EOSRE>|DDCTabs/new> sets the end-of-sentence regex
for the default L<DDCTabs|DiaColloDB::Document::DDCTabs> document class,
and L<C<-dopt foreign=1>|DDCTabs/new> disables D*-specific hacks.

Aliases: -d, -document-option, -docoption, -dopt, -do, -dO

=item -bysent

Split corpus (-> track collocations in compiled database) by sentence (default).

=item -byparagraph

Split corpus (-> track collocations in compiled database) by paragraph.

=item -bypage

Split corpus (-> track collocations in compiled database) by page.

=item -bydoc

Split corpus (-> track collocations in compiled database) by document.

=back

=cut


###############################################################
# Filter Options
=pod

=head2 Filter Options

=over 4

=item -use-all-the-data

Disables all content-filter options,
inspired by Mark Lauersdorf; equivalent to:

 -f=pgood='' \
 -f=wgood='' \
 -f=lgood='' \
 -f=pbad='' \
 -f=wbad='' \
 -f=lbad=''

Aliases: -F, -nofilters, -A, -all, -noprune

=back

=cut

###############################################################
# I/O and Logging Options
=pod

=head2 I/O and Logging Options

=over 4

=item -log-level LEVEL

Set L<DiaColloDB::Logger|DiaColloDB::Logger> log-level (default=TRACE).

Aliases: -ll, -log-level, -level

=item -log-option OPT=VAL

Set arbitrary L<DiaColloDB::Logger|DiaColloDB::Logger> option (e.g. logdate, logtime, file, syslog, stderr, ...).

Aliases: -lo, -log-option, -logopt

=item -[no]times

Do/don't report operating timing (default=do)

Aliases: -t, -timing, -times, -time

=item -output OUTBASE

Output basename for compiled corpus (required).

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

L<DiaColloDB(3pm)|DiaColloDB>,
L<DiaColloDB::Corpus(3pm)|DiaColloDB::Corpus>,
L<DiaColloDB::Corpus::Compiled(3pm)|DiaColloDB::Corpus::Compiled>,
L<dcdb-create.perl(1)|dcdb-create.perl>,
L<perl(1)|perl>.

=cut
