#!/usr/bin/perl -w

use lib qw(. lib);
use DiaColloDB;
use DiaColloDB::Document::DDCTabs;
use XML::LibXML;
use Getopt::Long qw(:config no_ignore_case);

our $help;
our %docopts = qw();
our $outfile = '-';
GetOptions(
	   'help|h' => \$help,
	   'document-option|doc-option|docopt|do|dO|O=s%' => \%docopts,
	   'output|o=s' => \$outfile,
	  );

if (!@ARGV || $help) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DDC_TABS_DOC

 Options:
   -help       # this help message
   -O OPT=VAL  # set document option
   -o OUTFILE  # set output file

EOF
  exit 1;
}

##==============================================================================
## MAIN

DiaColloDB->ensureLog();

##-- open document
my $infile = shift(@ARGV);
my $doc = DiaColloDB::Document::DDCTabs->fromFile($infile, %docopts)
  or die("$0: failed to load DDCTabs document from '$infile': $!");

##-- create json document
my @keys = grep {$_ ne 'label'} keys %{DiaColloDB::Document->new};
my $data = {};
@$data{@keys} = @$doc{@keys};

##-- dump
DiaColloDB::Utils::saveJsonFile($data,$outfile)
  or $doc->logconfess("failed to save JSON data to '$outfile': $!");
