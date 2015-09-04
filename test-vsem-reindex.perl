#!/usr/bin/perl -w

use lib qw(. dclib);
use DiaColloDB;
use Getopt::Long qw(:config no_ignore_case);

##--------------------------------------------------------------
## command-line
my ($help);
my %vsopts = (
	      saveMem => 1,
	     );
GetOptions(
	   'help|h' => \$help,
	   'vsem-option|vsopt|vso|vopt|vo|vO=s%' => \%vsopts,
	  );

if ($help || !@ARGV) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DBDIR

Options:
  -vsem-option OPT=VAL   # set vsem option

EOF
  exit ($help ? 0 : 1);
}


##--------------------------------------------------------------
## test: vsem: reindex
sub test_vsem_reindex {
  my $dbdir = shift;

  ##-- open (index_vsem:0)
  my $coldb = DiaColloDB->new()
    or die("$0: failed to create DiaColloDB object");
  $coldb->open($dbdir, index_vsem=>0)
    or die("$0: failed to open DiaColloDB directory $dbdir/:_ $!");

  ##-- set default options
  $coldb->info("creating vector-space model $dbdir/vsem* [vbreak=$coldb->{vbreak}]");
  my %VSOPTS = %DiaColloDB::VSOPTS,
  $coldb->{vsopts}     //= {};
  $coldb->{vsopts}{$_} //= $VSOPTS{$_} foreach (keys %VSOPTS); ##-- vsem: default options

  ##-- clobber local options
  $coldb->{vsopts}{keys %vsopts} = values %vsopts;

  ##-- (re-)tie doctmp array
  -e "$dbdir/doctmp.a"
    && ($coldb->{doctmpa} = [])
    && tie(@{$coldb->{doctmpa}}, 'Tie::File::Indexed::JSON', "$dbdir/doctmp.a", mode=>'ra', temp=>0)
    || $coldb->logconfess("could not tie temporary doc-data array to $dbdir/doctmp.a: $!");

  ##-- re-index
  $coldb->{vsem} = DiaColloDB::Relation::Vsem->create($coldb, undef, base=>"$dbdir/vsem");

  $coldb->vlog('info', "finished re-indexing vector-space model $dbdir/vsem*");
  exit 0;
}
test_vsem_reindex(@ARGV);
