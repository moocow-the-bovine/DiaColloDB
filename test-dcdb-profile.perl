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

our $dbdir      = undef;
our %coldb      = (flags=>'r');

our $want_lemmata = 1;
our $want_dates   = 1;
our $want_strings = undef; ##-- $want_lemmata || $want_dates
our $want_mi = 1;
our $want_ld = 1;
our $outfmt  = 'text';

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,
	   'verbose|v=i' => \$verbose,

	   ##-- local
	   'strings|s!' => sub { $want_dates=$want_lemmata=$_[1]; },
	   'lemmata|lemmas|lemma-strings|ls!' => \$want_lemmata,
	   'dates|date-strings|ds!' => \$want_dates,
	   'mi|m!' => \$want_mi,
	   'logdice|dice|ld|d!' => \$want_ld,
	   'scores|S!' => sub {$want_mi=$want_ld=$_[1]},
	   'text|t' => sub {$outfmt='text'},
	   'json|j' => sub {$outfmt='json'},
	   'null|noout' => sub {$outfmt=''},
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBDIR specified!"}) if (@ARGV<1);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no LEMMA specified!"}) if (@ARGV<2);

if ($version || $verbose >= 2) {
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

##-- profile: get tuple-IDs
$coldb->trace("profile: get tuple-IDs");
my @lemmas = @ARGV;
my ($l,$li,$lxids,@xids);
my $lenum = $coldb->{lenum};
foreach $l (@lemmas) {
  if (!defined($li=$lenum->s2i($l))) {
    warn("$0: ignoring unknown lemma '$l'");
    next;
  }
  push(@xids, @{$coldb->{l2x}->fetch($li)});
}
@xids = sort {$a <=> $b} @xids;

##-- profile: get co-frequency profile
$coldb->trace("profile: get co-frequency profile");
my $prf = $coldb->{cof}->profile(\@xids);

##-- stringify profile
my $sprf  = $prf;
if ($want_lemmata || $want_dates) {
  $coldb->trace("profile: stringify [", ($want_dates ? "+" : "-"), "dates, ", ($want_lemmata ? "+" : "-"), "lemmata]");
  $sprf  = $prf->new(N=>$prf->{N},f1=>$prf->{f1});
  my $xenum = $coldb->{xenum};
  my $pack_x = $coldb->{pack_x};
  my ($xi2,$li2,$d2,$l2,$key2);
  foreach my $xi2 (keys %{$prf->{f2}}) {
    ($li2,$d2) = unpack($pack_x, $xenum->i2s($xi2));
    $l2   = $want_lemmata ? $lenum->i2s($li2) : $li2;
    $key2 = "$d2\t$l2";
    $sprf->{f2}{$key2}  = $prf->{f2}{$xi2};
    $sprf->{f12}{$key2} = $prf->{f12}{$xi2};
  }
}

##-- compile collocation scores
if ($want_mi) {
  $coldb->trace("compile_mi()");
  $sprf->compile_mi();
}
if ($want_ld) {
  $coldb->trace("compile_ld()");
  $sprf->compile_ld();
}

##-- dump stringified profile
if ($outfmt eq 'text') {
  $coldb->trace("saveTextFile()");
  $sprf->saveTextFile('-');
}
elsif ($outfmt eq 'json') {
  $coldb->trace("saveJsonFile()");
  DiaColloDB::Utils::saveJsonFile($sprf, '-');
}
$coldb->trace("done.");


__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

coldb-profile.perl - get a lemma-profile from a DiaColloDB

=head1 SYNOPSIS

 coldb-profile.perl [OPTIONS] DBDIR LEMMA(S)...

 General Options:
   -help
   -version
   -verbose LEVEL

 Local Options:
   -[no]dates		# do/don't include dates in output (default=do)
   -[no]lemmas          # do/don't include lemmata in output (default=do)
   -[no]strings         # alias for -[no]strings -[no]dates
   -[no]mi              # do/dont't compute MI*logf score (default=do)
   -[no]dice            # do/dont't compute log-Dice score (default=do)
   -[no]scores          # alias for -[no]mi -[no]dice

   -text		# use text output (default)
   -json                # use json output
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
