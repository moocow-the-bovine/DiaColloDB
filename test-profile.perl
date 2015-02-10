#!/usr/bin/perl -w

use lib '.';
use CollocDB;
use CollocDB::Utils qw(:json);
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

our $want_strings = 0;
our $want_mi = 0;
our $want_ld = 0;

##----------------------------------------------------------------------
## Command-line processing
##----------------------------------------------------------------------
GetOptions(##-- general
	   'help|h' => \$help,
	   'version|V' => \$version,
	   'verbose|v=i' => \$verbose,

	   ##-- local
	   'strings|s!' => \$want_strings,
	   'mi|m!' => \$want_mi,
	   'logdice|dice|ld|d!' => \$want_ld,
	  );

pod2usage({-exitval=>0,-verbose=>0}) if ($help);
pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no DBDIR specified!"}) if (@ARGV<1);
#pod2usage({-exitval=>1,-verbose=>0,-msg=>"$prog: ERROR: no LEMMAFILE specified!"}) if (@ARGV<2);

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

##-- get lemmas
my $lemmafile = shift(@ARGV) || '-';
open(my $lemmafh,"<$lemmafile") or die("$0: open failed for $lemmafile: $!");
binmode($lemmafh,':utf8');
my @lemmas = map {chomp; $_} grep {$_ !~ /^$/} <$lemmafh>;
close($lemmafh);

##-- guts
my $lenum = $coldb->{lenum};
my $xenum = $coldb->{xenum};
my ($l,$li,$lxids,@xids);
my ($prf,$sprf);
my ($xi2,$wi2,$li2,$d2,$l2,$key2);
my $n = 0;
foreach $l (@lemmas) {
  print STDERR "." if (($n++ % 100)==0);
  $li=$lenum->{s2i}{data}{$l};
  @xids = sort {$a <=> $b} @xids;
  $prf = $coldb->{cof}->profile(\@xids);

  $sprf = $prf;
  if (!$want_strings) {
    $sprf = $prf;
  } else {
    ##-- stringify profile
    $sprf  = $prf->new(N=>$prf->{N},f1=>$prf->{f1});
    foreach my $xi2 (keys %{$prf->{f2}}) {
      ($wi2,$li2,$d2) = (($coldb->{index_w} ? qw() : undef), unpack($coldb->{pack_x}, $xenum->{i2s}{data}{$xi2}));
      $l2   = $lenum->{i2s}{data}{$li2};
      $key2 = "$d2\t$l2";
      $sprf->{f2}{$key2}  = $prf->{f2}{$xi2};
      $sprf->{f12}{$key2} = $prf->{f12}{$xi2};
    }
  }

  ##-- dump stringified profile
  #print saveJsonString($sprf);
  $sprf->compile_mi() if ($want_mi);
  $sprf->compile_ld() if ($want_ld);
  #$sprf->saveTextFile('-');
}
print STDERR " done.\n";


__END__

###############################################################
## pods
###############################################################

=pod

=head1 NAME

test-profile.perl - get a bunch of lemma-profiles from a CollocDB

=head1 SYNOPSIS

 coldb-profile.perl [OPTIONS] DBDIR LEMMAFILE(S)...

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
