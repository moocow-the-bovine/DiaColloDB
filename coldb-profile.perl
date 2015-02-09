#!/usr/bin/perl -w

use lib '.';
use CollocDB;
use CollocDB::Utils qw(:json);
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

our $dbdir      = undef;
our %coldb      = (flags=>'r');

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,
	   'verbose|v=i' => \$verbose,
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBDIR specified!"}) if (@ARGV<1);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no LEMMA specified!"}) if (@ARGV<2);

if ($version || $verbose >= 2) {
  print STDERR "$prog version $CollocDB::VERSION by Bryan Jurish\n";
  exit 0 if ($version);
}


##----------------------------------------------------------------------
## MAIN
##----------------------------------------------------------------------

##-- setup logger
CollocDB::Logger->ensureLog();

##-- open colloc-db
$dbdir = shift(@ARGV);
$dbdir =~ s{/$}{};
my $coldb = CollocDB->new(%coldb)
  or die("$prog: failed to create new CollocDB object: $!");
$coldb->open($dbdir)
  or die("$prog: CollocDB::open() failed for '$dbdir': $!");

##-- profile: get tuple-IDs
my @lemmas = @ARGV;
my ($l,$li,$lxids,@xids);
my $lenum = $coldb->{lenum};
foreach $l (@lemmas) {
  if (!defined($li=$lenum->{s2i}{data}{$l})) {
    warn("$0: ignoring unknown lemma '$l'");
    next;
  }
  push(@xids, @{$coldb->{l2x}{data}{$li}});
}
@xids = sort {$a <=> $b} @xids;

##-- profile: get co-frequency profile
my $prf = $coldb->{cof}->profile(\@xids);

##-- stringify profile
my $sprf  = $prf->new(N=>$prf->{N},f1=>$prf->{f1});
my $xenum = $coldb->{xenum};
my ($xi2,$wi2,$li2,$d2,$l2,$key2);
foreach my $xi2 (keys %{$prf->{f2}}) {
  ($wi2,$li2,$d2) = (($coldb->{index_w} ? qw() : undef), unpack($coldb->{pack_x}, $xenum->{i2s}{data}{$xi2}));
  $l2   = $lenum->{i2s}{data}{$li2};
  $key2 = "$d2\t$l2";
  $sprf->{f2}{$key2}  = $prf->{f2}{$xi2};
  $sprf->{f12}{$key2} = $prf->{f12}{$xi2};
}

##-- dump stringified profile
#print saveJsonString($sprf);
$sprf->compile_mi();
$sprf->compile_ld();
$sprf->saveTextFile('-');


__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

coldb-profile.perl - get a lemma-profile from a CollocDB

=head1 SYNOPSIS

 coldb-profile.perl [OPTIONS] DBDIR LEMMA(S)...

 General Options:
   -help
   -version
   -verbose LEVEL

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
