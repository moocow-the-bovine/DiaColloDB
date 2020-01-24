#!/usr/bin/perl -w

use lib qw(. ./lib .. ../lib);
use DiaColloDB;
use DiaColloDB::Relation::Cofreqs;
use DiaColloDB::Utils qw(:fcntl :run :env);
use File::Path qw(make_path);
use File::Basename qw(basename dirname);
use t2c_common;
use strict;

##-- command-line
my $infile  = shift || '-';
my $outbase = shift || "cof.d/cof";

##-- logger
DiaColloDB->ensureLog();
my $logger = 'DiaColloDB::Logger';
$logger->info("$0 $infile -> $outbase.*");

##-- timing
my $timer  = DiaColloDB::Timer->start();

##-- dummy context
#open(my $infh, "<$infile")
#  or die("$0: open failed for '$infile': $!");

-d dirname($outbase)
  or make_path(dirname($outbase))
  or die("$0: failed to mkdir ", dirname($outbase), ": $!");
my $flags = fcflags("rw");
my $cof = DiaColloDB::Relation::Cofreqs->new(flags=>$flags,
                                             base=>$outbase,
                                             dmax=>5,
                                             mmap=>1,
                                             fmin=>2,
                                             #keeptmp=>1,
                                            );

##-- close packed-files
$_->close() foreach (@$cof{qw(r1 r2 r3 rN)});

##-- guts: populate packed-file data
runcmd("./cof2bin-cxx32", "-fmin", ($cof->{fmin}+0), $infile, $cof->{base})==0
  or $cof->logconfess("failed to compile native co-frequency index data: $!");

##-- bootstrap: get constants written by XS-compiler
open(my $constfh, "<$outbase.const")
  or $cof->logconfess("failed to open constants file $outbase.const: $!");
@$cof{qw(ymin N)} = map {($_//0)+0} split(' ', scalar(<$constfh>), 2);
close($constfh);

##-- bootstrap: re-open relations in append-mode
foreach (qw(1 2 3 N)) {
  my $r = $cof->{"r$_"};
  $r->open(undef, fcflags('rwa'));
  delete($r->{size});
  $r->flush();
  $cof->{"size$_"} = $r->size();
}

##-- report timing
my $timestr = $timer->timestr;
$logger->info(memoryStatus);
$logger->info("operation completed in $timestr");
