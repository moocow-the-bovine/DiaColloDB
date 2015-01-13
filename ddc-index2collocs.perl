#!/usr/bin/perl -w

use DB_File;
use Fcntl;
use File::Path qw(mkpath rmtree);
use Getopt::Long qw(:config no_ignore_case);

use PDL;
use PDL::IO::FastRaw;
use PDL::Ngrams;
use File::Map;

use strict;

BEGIN { select(STDERR); $|=1; select(STDOUT); $|=0; }

our ($help);
our $dbdir = undef; ##-- default: INDEX.cld
our $index = 'Token';
our $progress = 100000;
GetOptions(
	   'help|h' => \$help,
	   'db-directory|dbdir|dd|d|output-directory|output|od|o=s' => \$dbdir,
	   'index|i=s' => \$index,
	   'progress|p=i' => \$progress,
	  );


if ($help || !@ARGV) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] PROJECT[.con]

 Options:
   -help         # this help message
   -index INDEX  # select index attribute (default=Token)
   -dbdir DIR    # output directory (default=INDEX.cld)

EOF
  exit 1;
}

##-- command-line:  confile
my $confile = shift;
(my $project = $confile) =~ s/\.con$//i;
$dbdir //= "$index.cld";

##-- create output directory
print STDERR "$0: creating output directory ...";
if (-e $dbdir) {
  rmtree($dbdir)
    or die("$0: failed to remove existing output directory '$dbdir': $!");
}
mkpath($dbdir, {verbose=>0, mode=>0755})
  or die("$0: failed to create $dbdir: $!");
print STDERR " done.\n";

##-- open enum files
print STDERR "$0: creating enum files ...";
my (%i2s,%s2i);
my $dbflags  = O_RDWR|O_CREAT;
my $dbmode   = 0640;
my $i2s = tie(%i2s, 'DB_File', "$dbdir/i2s.db", $dbflags, $dbmode, $DB_BTREE)
  or die("$0: failed to tie $dbdir/i2s.db: $!");
my $s2i = tie(%s2i, 'DB_File', "$dbdir/s2i.db", $dbflags, $dbmode, $DB_BTREE)
  or die("$0: failed to tie $dbdir/s2i.db: $!");
print STDERR " done.\n";

##-- open project index-types file as DB_RECNO
my $ixfile = "${project}._${index}";
my $ixinfo = DB_File::RECNOINFO->new();
$ixinfo->{bval} = "\0";
my (@ptypes);
print STDERR "$0: reading project type file $ixfile ...";
my $ptypesdb = tie(@ptypes, 'DB_File', $ixfile, O_RDONLY, 0640, $ixinfo)
  or die("$0: tie() failed for project index-file '$ixfile': $!");
##
##-- create local enums: process project types
my @pi2s  = @ptypes;
my @li2pi = sort {$pi2s[$a] cmp $pi2s[$b]} (0..$#pi2s); ##-- $pi==$li2pi[$li] iff ($pi2s[$pi] eq $i2s{pack('L',$li)})
my ($li,$pi)=(0,0);
my ($ws,$wi);
foreach $pi (@li2pi) {
  if ($progress && ($li % $progress) == 0) { print STDERR "."; }
  $ws = $pi2s[$pi];
  $wi = pack('L',$li);
  $s2i{$ws} = $wi;
  $i2s{$wi} = $ws;
  ++$li;
}
##
undef $ptypesdb;
untie @ptypes;
print STDERR "done.\n";


##-- open project index-storage file (pdl mmap)
my $longsz = PDL::howbig(PDL::long());
die("$0: sizeof(PDL::long)==$longsz!=4: can't map storage file (need 32-bit integers)!")
  if (PDL::howbig(PDL::long()) != 4);
my $tokfile = "${project}._storage_${index}";

print STDERR "$0: mapping storage file $tokfile ...\n";
my $ntoks   = (-s $tokfile)/4;
my $ptoks   = mapfraw($tokfile, {ReadOnly=>1, Creat=>0, Dims=>[$ntoks], Datatype=>PDL::long});
#warn("$0: negative indices in token-pdl (maybe we have more than 2G types?)\n") if (any($tokpdl<0));

##-- map tokens to local-ids
my $pi2li   = zeroes(long, scalar(@ptypes));
$pi2li->index(pdl(long,\@li2pi)) .= $pi2li->sequence;
my $ltoks   = $ptoks->index($pi2li);

##-- compute: unigrams
print STDERR "$0: computing 1-grams ...";
my (%f1);
my $f1db = tie(%f1, 'DB_File', "$dbdir/f1.db", $dbflags, $dbmode, $DB_BTREE)
  or die("$0: failed to tie $dbdir/f1.db: $!");
##
my ($f1,$f1w) = ng_cofreq( $ltoks->slice("*1,") );
my ($i,$n);
for ($i=0, $n=$f1->nelem; $i<$n; ++$i) {
  if ($progress && ($i % $progress) == 0) { print STDERR "."; }
  $f1db->put(pack('L',$f1w->at(0,$i)), pack('L',$f1->at($i)));
}
print STDERR " done.\n";
##
##-- untie: unigrams
undef $f1db;
untie %f1;

##-- compute: k-grams
my ($f,$fw);
foreach my $k (2..0) {
  print STDERR "$0: processing $k-grams ...";
  my (%f);
  my $fdb = tie(%f, 'DB_File', "$dbdir/f${k}.db", $dbflags, $dbmode, $DB_BTREE)
    or die("$0: failed to tie $dbdir/f${k}.db: $!");
  ##
  ($f,$fw) = ng_cofreq( $ltoks->slice("*$k,") );
  my ($j,$ikey,$jkey,$key,$val);
  for ($i=0, $n=$f->nelem; $i<$n; $i=$j) {
    $ikey = $fw->slice("0:-2,$i");
    $val  = '';
    for ($j=$i; $j<$n && all($fw->slice("0:-2,$j")==$ikey); ++$j) {
      if ($progress && ($j % $progress) == 0) { print STDERR "."; }
      $val .= pack('L*', $fw->at($k-1,$j), $f->at($j));
    }
    $fdb->put(pack('L*',$ikey->list), $val);
  }
  print STDERR " done.\n";
  ##
  ##-- untie: k-grams
  undef $fdb;
  untie %f;
}


##-- untie: tokens
undef $ltoks;
undef $ptoks;

##-- all done
print STDERR "$0: finished.\n";

##-- close safely
undef $i2s;
undef $s2i;
untie %i2s;
untie %s2i;

