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
our ($help,$version);

our %log        = (level=>'TRACE', rootLevel=>'FATAL');
our $dburl      = undef;
our %cli        = (opts=>{});
our $http_user  = undef;

our $diff = undef;
our $rel  = 'cof';
our %query = (
	      lemma =>'',	##-- selected lemma(ta), common
	      date  =>undef,    ##-- selected date(s), common
	      slice =>1,        ##-- date slice, common
	      having=>{},       ##-- result filters, common
	      ##
	      #alemma=>'',	##-- selected lemma(ta), arg1
	      adate  =>undef,	##-- selected date(s), arg1
	      aslice =>undef,	##-- date slice, arg1
	      ahaving=>{},         ##-- result filters, arg1
	      ##
	      blemma =>'',	##-- selected lemma(ta), arg2
	      bdate  =>undef,	##-- selected date(s), arg2
	      bslice =>undef,	##-- date slice, arg2
	      bhaving=>{},         ##-- result filters, arg2
	      ##
	      groupby=>'l',     ##-- result aggregation (empty:all available attributes)
	      ##
	      eps => 0,		##-- smoothing constant
	      score =>'ld',	##-- score func
	      kbest =>10,	##-- k-best items per date
	      cutoff =>undef,	##-- minimum score cutoff
	      strings => 1,	##-- debug: want strings?
	     );
our %save = (format=>undef);

our $outfmt  = 'text'; ##-- output format: 'text' or 'json'
our $pretty  = 1;
our $dotime  = 1; ##-- report timing?

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,

	   ##-- general
	   'log-level|level|ll=s' => sub { $log{level} = uc($_[1]); },
	   'client-option|co|db-option|do|O=s%' => \$cli{opts},

	   ##-- query options
	   'difference|diff|D|compare|comp|cmp!' => \$diff,
	   'profile|prof|prf|P' => sub { $diff=0 },
	   'collocations|collocs|cofreqs|cof|co|f12|f2|12|2' => sub { $rel='cof' },
	   'unigrams|ug|u|f1|1' => sub { $rel='xf' },
	   ##
	   (map {("${_}date|${_}d=s"=>\$query{"${_}date"})} ('',qw(a b))), 				  ##-- date,adate,bdate
	   (map {("${_}date-slice|${_}ds|${_}slice|${_}sl|${_}s=s"=>\$query{"${_}slice"})} ('',qw(a b))), ##-- slice,aslice,bslice
	   (map {("${_}filter|${_}F|${_}having|${_}has|${_}H=s%"=>\$query{"${_}has"})} ('',qw(a b))),     ##-- has,ahas,bhas
	   ##
	   'group-by|groupby|group|gb|g=s' => \$query{groupby},
	   ##
	   'epsilon|eps|e=f'  => \$query{eps},
	   'mutual-information|mi'    => sub {$query{score}='mi'},
	   'log-dice|logdice|ld|dice' => sub {$query{score}='ld'},
	   'frequency|freq|f'         => sub {$query{score}='f'},
	   'normalized-frequency|nf|frequency-per-million|fpm|fm'  => sub {$query{score}='fm'},
	   'k-best|kbest|k=i' => \$query{kbest},
	   'no-k-best|nokbest|nok' => sub {$query{kbest}=undef},
	   'cutoff|C=f' => \$query{cutoff},
	   'no-cutoff|nocutoff|noc' => sub {$query{cutoff}=undef},
	   'strings|S!' => \$query{strings},

	   ##-- I/O
	   'user|U=s' => \$http_user,
	   'text|t' => sub {$outfmt='text'},
	   'json|j' => sub {$outfmt='json'},
	   'html|H' => sub {$outfmt='html'},
	   'pretty|p!' => \$pretty,
	   'ugly!' => sub {$pretty=!$_[1]},
	   'null|noout' => sub {$outfmt=''},
	   'score-format|sf|format|fmt=s' => \$save{format},
	   'timing|times|time|T!' => \$dotime,
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBURL specified!"}) if (@ARGV<1);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no LEMMA1 specified!"}) if (@ARGV<2);

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
$diff //= @ARGV > 1;
$query{lemma}  = shift;
$query{blemma} = @ARGV ? shift : $query{lemma};
$rel  = "d$rel" if ($diff);
my $timer = DiaColloDB::Timer->start();
my $mp = $cli->query($rel, %query)
  or die("$prog: query() failed for relation '$rel', lemma(s) '$query{lemma}'".($diff ? " - '$query{blemma}'" : '').": $cli->{error}");

##-- dump stringified query
if ($outfmt eq 'text') {
  $mp->trace("saveTextFile()");
  $mp->saveTextFile('-',%save);
}
elsif ($outfmt eq 'json') {
  $mp->trace("saveJsonFile()");
  $mp->saveJsonFile('-', pretty=>$pretty,canonical=>$pretty); #utf8=>0
}
elsif ($outfmt eq 'html') {
  $mp->trace("saveHtmlFile()");
  $mp->saveHtmlFile('-',%save);
}

##-- cleanup
$cli->close();

##-- timing
$cli->info("operation completed in ", $timer->timestr) if ($dotime);


__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

dcdb-query.perl - query a DiaColloDB

=head1 SYNOPSIS

 dcdb-query.perl [OPTIONS] DBURL LEMMA1 [LEMMA2]

 General Options:
   -help
   -version
   -[no]time             # do/don't report operation timing (default=do)

 DiaColloDB Options:
   -log-level LEVEL      # set minimum DiaColloDB log-level
   -O KEY=VALUE          # set DiaColloDB::Client option

 Query Options:
   -profile , -diff      # select profile operation (default=-profile)
   -collocs , -unigrams  # select profile type (collocations or unigrams; default=-collocs)
   -(a|b)?date DATES     # set target DATE or /REGEX/ or MIN-MAX
   -(a|b)?slice SLICE    # set target date slice (default=1)
   -(a|b)?has ATTR=VAL   # set collocate filter for LIST or /REGEX/ on attribute ATTR
   -groupby ATTRS        # set result aggregation (default=l)
   -f , -fm , -mi , -ld  # set scoring function (default=-ld)
   -kbest KBEST          # return only KBEST items per date-slice (default=10)
   -nokbest              # disable k-best pruning
   -cutoff CUTOFF        # set minimum score for returned items (default=none)
   -nocutoff             # disable cutoff pruning
   -[no]strings          # debug: do/don't stringify returned profile (default=do)

 I/O Options:
   -user USER[:PASSWD]   # user credentials for HTTP queries
   -text		 # use text output (default)
   -json                 # use json output
   -null                 # don't output profile at all
   -[no]pretty           # do/don't pretty-print json output (default=do)

 Arguments:
   DBURL                # DB URL (file://, http://, or list:// ; query part sets local options)
   LEMMA1               # space-separated target1 string(s) list or /REGEX/
   LEMMA2               # space-separated target2 string(s) list or /REGEX/

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
