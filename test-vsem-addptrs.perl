#!/usr/bin/perl -w

use lib qw(. dclib);
use DiaColloDB;
use DiaColloDB::Utils qw(:sort :regex);
use DiaColloDB::Relation::Vsem;
use PDL;
use PDL::CCS;
use Data::Dumper;
use Benchmark qw(timethese cmpthese);
use Getopt::Long qw(:config no_ignore_case);
use utf8;
use strict;

BEGIN {
  select(STDERR); $|=1; select(STDOUT); $|=1;
  binmode(STDOUT,':utf8');

  no warnings 'once';
  $PDL::BIGPDL=1;
}

##======================================================================
## command-line
my ($help);
GetOptions(
	   'help|h' => \$help,
	  );
if ($help || @ARGV < 1) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DBDIR

EOF
  exit $help ? 0 : 1;
}

##======================================================================
## debug messages
use File::Basename;
our $prog = basename($0);

sub vmsg0 {
  print STDERR @_;
}
sub vmsg {
  vmsg0("$prog: ", @_, "\n");
}

sub pinfo {
  my $p = shift;
  return $p->type."(".join(',',$p->dims).") [".join(':',$p->minmax)."]"
}

##======================================================================
## MAIN

my $dbdir = shift;

DiaColloDB->ensureLog();
my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
my $vs    = $coldb->{vsem};
my $tdm   = $vs->{tdm};
my $vsdir = "$coldb->{vsem}{base}.d";
my $itype = $vs->itype;
my $vtype = $vs->vtype;
my ($NT,$ND,$Nnz) = ($tdm->dims,$tdm->_nnz);
$vs->info("loaded Vsem relation: NT=$NT, ND=$ND, Nnz=$Nnz");

##-- cache pointers
$vs->info("caching ptr(0)");
my ($ptr0) = $tdm->getptr(0);
$ptr0      = $ptr0->convert($itype) if ($ptr0->type != $itype);
$ptr0->writefraw("$vsdir/tdm.ptr0.pdl")
  or confess("$prog: failed to write $vsdir/tdm.ptr0.pdl: $!");
$vs->info("cached ptr(0) ~ ", pinfo($ptr0));

$vs->info("caching ptr(1), pix1");
my ($ptr1,$pix1) = $tdm->getptr(1);
$ptr1 = $ptr1->convert($itype) if ($ptr1->type != $itype);
$pix1 = $pix1->convert($itype) if ($pix1->type != $itype && pdl($itype,$pix1->nelem)->sclr >= 0); ##-- check for overflow
$ptr1->writefraw("$vsdir/tdm.ptr1.pdl")
  or confess("$prog: failed to write $vsdir/tdm.ptr1.pdl: $!");
$pix1->writefraw("$vsdir/tdm.pix1.pdl")
  or confess("$prog: failed to write $vsdir/tdm.pix1.pdl: $!");
$vs->info("cached ptr(1) ~ ", pinfo($ptr1), " ; pix(1) ~ ", pinfo($pix1));

##-- cache norm
$vs->info("caching vnorm0");
my $vnorm0 = $tdm->vnorm(0);
$vnorm0    = $vnorm0->convert($vtype) if ($vnorm0->type != $vtype);
$vnorm0->writefraw("$vsdir/tdm.vnorm0.pdl")
  or confess("$prog: failed to write $vsdir/tdm.vnorm0.pdl: $!");
$vs->info("cached vnorm0 ~ ", pinfo($vnorm0));
