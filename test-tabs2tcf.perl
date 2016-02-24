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

##-- namespaces
my $nsroot = "http://www.dspin.de/data";
my $nsmeta = "http://www.dspin.de/data/metadata";
my $nscorpus = "http://www.dspin.de/data/textcorpus";

##-- superstructure
my $xdoc = XML::LibXML::Document->new("1.0","UTF-8");
$xdoc->setDocumentElement(my $xroot = $xdoc->createElementNS($nsroot, "D-Spin"));
my $xmeta   = $xroot->addNewChild($nsmeta, "MetaData");
my $xcorpus = $xroot->addNewChild($nscorpus, "TextCorpus");

##-- metadata
my $meta = $doc->{meta};
$meta->{date} //= $doc->{date};
my ($key,$val,$nod);
while (($key,$val) = each %$meta) {
  $nod = $xmeta->addNewChild(undef, "source");
  $nod->setAttribute(type=>"meta:$key");
  $nod->appendText($val);
}

##-- tokens
my $xtokens = $xcorpus->addNewChild(undef, "tokens");
my $xsents  = $xcorpus->addNewChild(undef,"sentences");
my $xlemmas = $xcorpus->addNewChild(undef,"lemmas");
my $xtags   = $xcorpus->addNewChild(undef,"POStags");
my @s = qw();
my $nw = 0;
my $ns = 0;
my ($id);
foreach my $w (@{$doc->{tokens}}) {
  if (!defined($w)) {
    ##-- dump current sentence
    if (@s) {
      $nod = $xsents->addNewChild(undef,"sentence");
      $nod->setAttribute("ID" => "s$ns");
      $nod->setAttribute("tokenIDs" => join(' ',@s));
      ++$ns;
    }
    @s = qw();
  }
  elsif (ref($w)) {
    ##-- add: token
    $nod = $xtokens->addNewChild(undef,"token");
    $nod->setAttribute(ID=>($id=($w->{id}//"w$nw")));
    $nod->appendText($w->{w});
    push(@s,$id);
    ++$nw;

    ##-- add: token: lemma
    if (defined($w->{l})) {
      $nod = $xlemmas->addNewChild(undef,"lemma");
      $nod->setAttribute("tokenIDs" => $id);
      $nod->appendText($w->{l});
    }

    ##-- add: token: tag
    if (defined($w->{p})) {
      $nod = $xtags->addNewChild(undef,"tag");
      $nod->setAttribute("tokenIDs" => $id);
      $nod->appendText($w->{p});
    }
  }
}
##-- add final sentence
if (@s) {
  $nod = $xsents->addNewChild(undef,"sentence");
  $nod->setAttribute("ID" => "s$ns");
  $nod->setAttribute("tokenIDs" => join(' ',@s));
}

##-- dump
if ($outfile eq '-') {
  $xdoc->toFH(\*STDOUT, 1)
    or $doc->logconfess("failed to save TCF document to STDOUT: $!");
} else {
  $xdoc->toFile($outfile,1)
    or $doc->logconfess("failed to save TCF document to '$outfile': $!");
}
