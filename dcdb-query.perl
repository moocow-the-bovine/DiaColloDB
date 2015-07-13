#!/usr/bin/perl -w

use lib '.';
use DiaColloDB;
use DiaColloDB::Utils qw(:json :time);
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
our ($help,$version);

our %log        = (level=>'TRACE', rootLevel=>'FATAL');
our $dburl      = undef;
our %cli        = (opts=>{});
our $http_user  = undef;

our $rel  = 'cof';
our %query = (
	      query =>'',	##-- target query, common
	      date  =>undef,    ##-- target date(s), common
	      slice =>1,        ##-- date slice, common
	      ##
	      #aquery=>'',	##-- target query(ta), arg1
	      adate  =>undef,	##-- target date(s), arg1
	      aslice =>undef,	##-- date slice, arg1
	      ##
	      bquery =>'',	##-- target query, arg2
	      bdate  =>undef,	##-- target date(s), arg2
	      bslice =>undef,	##-- date slice, arg2
	      ##
	      groupby=>'l',     ##-- result aggregation (empty:all available attributes, no restrictions)
	      ##
	      eps => 0,		##-- smoothing constant
	      score =>'ld',	##-- score func
	      diff=>'abs-diff', ##-- diff-op
	      kbest =>10,	##-- k-best items per date
	      cutoff =>undef,	##-- minimum score cutoff
	      global =>0,       ##-- trim globally (vs. slice-locally)?
	      strings => 1,	##-- debug: want strings?
	     );
our %save = (format=>undef);

our $outfmt  = 'text'; ##-- output format: 'text' or 'json'
our $pretty  = 1;
our $dotime  = 1; ##-- report timing?
our $niters  = 1; ##-- number of benchmark iterations

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,

	   ##-- general
	   'log-level|level|ll=s' => sub { $log{level} = uc($_[1]); },
	   'client-option|db-option|do|O=s%' => \$cli{opts},

	   ##-- query options
	   #'difference|diff|D|compare|comp|cmp!' => \$diff,
	   #'profile|prof|prf|P' => sub { $diff=0 },
	   'collocations|collocs|collo|col|cofreqs|cof|co|f12|f2|12|2' => sub { $rel='cof' },
	   'unigrams|ug|u|f1|1' => sub { $rel='xf' },
	   'ddc' => sub { $rel='ddc' },
	   ##
	   (map {("${_}date|${_}d=s"=>\$query{"${_}date"})} ('',qw(a b))), 				  ##-- date,adate,bdate
	   (map {("${_}date-slice|${_}ds|${_}slice|${_}sl|${_}s=s"=>\$query{"${_}slice"})} ('',qw(a b))), ##-- slice,aslice,bslice
	   ##
	   'group-by|groupby|group|gb|g=s' => \$query{groupby},
	   ##
	   'difference|diff|D|compare|comp|cmp=s' => \$query{diff},
	   'epsilon|eps|e=f'  => \$query{eps},
	   'mutual-information|mi'    => sub {$query{score}='mi'},
	   'log-dice|logdice|ld|dice' => sub {$query{score}='ld'},
	   'frequency|freq|f'         => sub {$query{score}='f'},
	   'frequency-per-million|fpm|fm'  => sub {$query{score}='fm'},
	   'log-frequency|logf|lf' => sub { $query{score}='lf' },
	   'log-frequency-per-million|logfm|lfm' => sub { $query{score}='lfm' },
	   'k-best|kbest|k=i' => \$query{kbest},
	   'no-k-best|nokbest|nok' => sub {$query{kbest}=undef},
	   'cutoff|C=f' => \$query{cutoff},
	   'no-cutoff|nocutoff|noc' => sub {$query{cutoff}=undef},
	   'global|G!' => \$query{global},
	   'local|L!' => sub { $query{global}=!$_[1]; },
	   'strings|S!' => \$query{strings},

	   ##-- I/O
	   'user|U=s' => \$http_user,
	   'text|t' => sub {$outfmt='text'},
	   'json|j' => sub {$outfmt='json'},
	   'html' => sub {$outfmt='html'},
	   'pretty|p!' => \$pretty,
	   'ugly!' => sub {$pretty=!$_[1]},
	   'null|noout' => sub {$outfmt=''},
	   'score-format|sf|format|fmt=s' => \$save{format},
	   'timing|times|time|T!' => \$dotime,
	   'bench|n-iterations|iterations|iters|i=i' => \$niters,
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBURL specified!"}) if (@ARGV<1);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no QUERY specified!"}) if (@ARGV<2);

if ($version) {
  print STDERR "$prog version $DiaColloDB::VERSION by Bryan Jurish\n";
  exit 0 if ($version);
}


##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------

##-- setup logger
DiaColloDB::Logger->ensureLog(%log);

##-- parse user options
if ($http_user) {
  my ($user,$pass) = split(/:/,$http_user,2);
  $pass //= '';
  if ($pass eq '') {
    print STDERR "Password: ";
    $pass = <STDIN>;
    chomp $pass;
  }
  @{$cli{opts}}{qw(user password)} = @cli{qw(user password)} = ($user,$pass),
}

##-- open db client
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

##-- client query
do { utf8::decode($_) if (!utf8::is_utf8($_)) } foreach (@ARGV);
our $isDiff = (@ARGV > 1);
$query{query}  = shift;
$query{bquery} = @ARGV ? shift : $query{query};
$rel  = "d$rel" if ($isDiff);

if ($niters != 1) {
  $cli->info("performing $niters query iterations");
}
my $timer = DiaColloDB::Timer->start();
foreach my $iter (1..$niters) {
  my $mp = $cli->query($rel, %query)
    or die("$prog: query() failed for relation '$rel', query '$query{query}'".($isDiff ? " - '$query{bquery}'" : '').": $cli->{error}");

  ##-- dump stringified query
  my $outfile = ($iter==1 ? '-' : '/dev/null');
  if ($outfmt eq 'text') {
    $mp->trace("saveTextFile()");
    $mp->saveTextFile($outfile,%save);
  }
  elsif ($outfmt eq 'json') {
    $mp->trace("saveJsonFile()");
    $mp->saveJsonFile($outfile, pretty=>$pretty,canonical=>$pretty); #utf8=>0
  }
  elsif ($outfmt eq 'html') {
    $mp->trace("saveHtmlFile()");
    $mp->saveHtmlFile($outfile,%save);
  }
}

##-- cleanup
$cli->close();

##-- timing
if ($dotime || $niters > 1) {
  $cli->info("operation completed in ", $timer->timestr,
	     ($niters > 1 ? sprintf(" (%.2f iter/sec)", $niters/$timer->elapsed) : qw()),
	    );
}


__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

dcdb-query.perl - query a DiaColloDB

=head1 SYNOPSIS

 dcdb-query.perl [OPTIONS] DBURL QUERY1 [QUERY2]

 General Options:
   -help
   -version
   -[no]time             # do/don't report operation timing (default=do)
   -iters NITERS         # benchmark NITERS iterations of query

 DiaColloDB Options:
   -log-level LEVEL      # set minimum DiaColloDB log-level
   -O KEY=VALUE          # set DiaColloDB::Client option

 Query Options:
   -col , -ug , -ddc     # select profile type (collocations, unigrams, or ddc client; default=-col)
   -(a|b)?date DATES     # set target DATE or /REGEX/ or MIN-MAX
   -(a|b)?slice SLICE    # set target date slice (default=1)
   -groupby GROUPBY      # set result aggregation (default=l)
   -(l)f(m) , -mi , -ld  # set scoring function (default=-ld)
   -kbest KBEST          # return only KBEST items per date-slice (default=10)
   -nokbest              # disable k-best pruning
   -cutoff CUTOFF        # set minimum score for returned items (default=none)
   -nocutoff             # disable cutoff pruning
   -[no]global           # do/don't trim profiles globally (vs. locally by date-slice; default=don't)
   -[no]strings          # debug: do/don't stringify returned profile (default=do)

 I/O Options:
   -user USER[:PASSWD]   # user credentials for HTTP queries
   -text		 # use text output (default)
   -json                 # use json output
   -null                 # don't output profile at all
   -[no]pretty           # do/don't pretty-print json output (default=do)

 Arguments:
   DBURL                # DB URL (file://, http://, or list:// ; query part sets local options)
   QUERY1               # space-separated target1 string(s) LIST or /REGEX/ or DDC-query
   QUERY2               # space-separated target2 string(s) LIST or /REGEX/ or DDC-query (for diff profiles)

 Grouping and Filtering:
   GROUPBY is a space- or comma-separated list of the form ATTR1[:FILTER1] ..., where:
   - ATTR is the name or alias of a supported attribute (e.g. 'lemma', 'pos', etc.), and
   - FILTER is either a |-separated LIST of literal values or a /REGEX/[gimsadlu]*

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
