#!/usr/bin/perl -w

use lib qw(./blib/lib ./blib/arch);
use DiaColloDB::XS;
use strict;

my $ifile = shift || '-';
my $ofile = shift || '-';
my $dmax  = shift || 5;
my $rc = DiaColloDB::XS::CofUtils::generatePairs($ifile,$ofile,$dmax);
exit $rc;

