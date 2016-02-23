#!/usr/bin/perl -w

use lib qw(. lib);
use DiaColloDB;
use Getopt::Long qw(:config no_ignore_case);

our $mode = 'check';
our $help;
GetOptions(
	   'help|h' => \$help,
	   'check|c' => sub {$mode='check'},
	   'update|u' => sub {$mode='update'},
	  );

if (!@ARGV || $help) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DBDIR_OR_ENUM

 Options:
   -help       # this help message
   -check      # check enum consistency
   -update     # update enum content

EOF
  exit 1;
}

##==============================================================================
## subs

## \%s2i_or_undef = enum_check($enum)
##   + undef: no update required
sub enum_check {
  my $enum = shift;
  $enum->info("checking enum $enum->{base}");

  ##-- get data
  my $i2s = $enum->toArray();
  my %s2i = qw();
  my ($s);
  my $i=0;
  my $ntrimmed=0;
  foreach $s (@$i2s) {
    ++$ntrimmed if ($s =~ s/\s*\(.*$//);
    $s2i{$s} = $i++;
  }
  my $norig = @$i2s;
  my $nnew  = scalar keys %s2i;

  ##-- summarize
  $enum->info("would trim $ntrimmed of $norig safe original key(s), resulting in $nnew new key(s)");

  if (!$ntrimmed) {
    $enum->info("trimming not required");
    return undef;
  }
  elsif ($nnew != $norig) {
    $enum->error("NOT safe to trim!");
    exit 1;
  }
  else {
    $enum->info("safe to trim");
  }

  return \%s2i;
}

sub enum_update {
  my $enum = shift;
  my $s2i  = enum_check($enum);
  if (!defined($s2i)) {
    $enum->info("no update required");
    return;
  }

  my $ebase = $enum->{base};
  $enum->info("updating enum $ebase");
  $enum->close();
  my $e2    = $enum->new->fromHash($s2i);
  $e2->save($ebase)
    or die("$0: failed to save to $ebase.*: $!");
  $e2->info("saved enum $ebase.*");
}

##==============================================================================
## MAIN

DiaColloDB->ensureLog();

##-- open enum
my $arg   = shift(@ARGV);
my $ebase = -e "$arg.hdr" && -e "$arg.eix" && -e "$arg.es" && -e "$arg.esx" ? $arg : "$arg/tdf.d/$arg";
my $enum  = DiaColloDB::EnumFile::MMap->new(base=>$ebase, flags=>'r')
  or die("$0: open failed for enum '$ebase.*': $!");

##-- guts
if ($mode eq 'check') {
  enum_check($enum);
}
elsif ($mode eq 'update') {
  enum_update($enum);
}
else {
  die("$0: unknown operation mode '$mode'");
}

