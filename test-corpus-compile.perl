#!/usr/bin/perl -w

use lib qw(. ./lib);
use DiaColloDB;
#use DiaColloDB::Corpus::Compiled; ##-- debug
use strict;

my ($inlist,$outbase,$njobs) = @ARGV;
$outbase ||= 'test-corpus-compile.out';
$njobs   //= 0;

DiaColloDB->ensureLog(); #file=>'test-corpus-compile.log'
my $logger = 'DiaColloDB::Logger';

my $timer = DiaColloDB::Timer->start();

my $icorpus = DiaColloDB::Corpus->open([$inlist], 'glob'=>0, 'list'=>1)
  or $logger->logdie("failed to open corpus: $!");

my $ccorpus = $icorpus->compile($outbase, njobs=>$njobs)
  or $logger->logdie("failed to compile to $outbase.*: $!");

##-- report
my $elapsed = sprintf("%.2f", $timer->elapsed);
$logger->info("operation completed in $elapsed second(s) = ", DiaColloDB::Utils::s2timestr($elapsed));


