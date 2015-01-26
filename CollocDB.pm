## -*- Mode: CPerl -*-
## File: CollocDB.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, top-level

package CollocDB;
use CollocDB::Logger;
use CollocDB::Enum;
use CollocDB::Corpus;
use CollocDB::Utils qw(:fcntl :json);
use File::Path qw(make_path remove_tree);
use strict;


##==============================================================================
## Globals & Constants

our $VERSION = 0.01;
our @ISA = qw(CollocDB::Logger);

##==============================================================================
## Constructors etc.

## $cldb = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##--options
##    dbdir => $dbdir,    ##-- database directory; REQUIRED
##    flags => $fcflags,  ##-- fcntl flags or open()-style mode string; default='rw'
##    ##
##    ##-- logging
##    logParseFile => $level, ##-- log-level for file-parsing (default='trace')
##    ##
##    ##-- enums
##    wenum => $wenum,    ##-- enum: words  ($dbdir/wenum.*) : $w<=>$wi : A*<=>N
##    lenum => $wenum,    ##-- enum: lemmas ($dbdir/lenum.*) : $l<=>$wi : A*<=>N
##    xenum => $xenum,    ##-- enum: tuples ($dbdir/xenum.*) : [$di,$wi,$li]<=>$xi : nNN<=>N
##    w2x   => $w2x,      ##-- db: word->tuples  ($dbdir/w2x.db) : $wi=>@xis  : N=>N*
##    l2x   => $l2x,      ##-- db: lemma->tuples ($dbdir/l2x.db) : $li=>@xis  : N=>N*
##   )
sub new {
  my $that = shift;
  my $cldb  = bless({
		     ##-- options
		     dbdir => undef,
		     flags => 'rw',

		     ##-- logging
		     logParseFile => 'trace',

		     ##-- enums
		     wenum => CollocDB::Enum->new(),
		     lenum => CollocDB::Enum->new(),
		     xenum => CollocDB::Enum->new(),
		     w2x   => CollocDB::DBFile->new(),
		     l2x   => CollocDB::DBFile->new(),

		     @_, ##-- user arguments
		    },
		    ref($that)||$that);
  return defined($cldb->{dbdir}) ? $cldb->open($cldb->{dbdir}) : $cldb;
}

##==============================================================================
## I/O: open/close : TODO

## $bool = $cldb->opened()
sub opened {
  my $cldb = shift;
  return defined($cldb->{dbdir});
}

## $cldb_or_undef = $cldb->close()
sub close {
  my $cldb = shift;
  undef $cldb->{dbdir};
  $cldb->logconfess("close() not yet implemented");
  return $cldb;
}

## $cldb_or_undef = $cldb->open($dbdir)
## $cldb_or_undef = $cldb->open()
sub open {
  my ($cldb,$dbdir) = @_;
  $dbdir //= $cldb->{dbdir};
  $cldb->close() if ($cldb->opened);
  $cldb->logconfess("open() not yet implemented");
}


##==============================================================================
## Create/compile

## $bool = $coldb->create($corpus,%opts)
##  + %opts:
##     clobber %$coldb
sub create {
  my ($coldb,$corpus,%opts) = @_;
  @$coldb{keys %opts} = values %opts;

  ##-- initialize: output directory
  my $dbdir = $coldb->{dbdir}
    or $coldb->logconfess("create() called but 'dbdir' key not set!");
  !-d $dbdir
    or remove_tree($dbdir)
      or $coldb->logconfess("create(): could not remove stale $dbdir: $!");
  make_path($dbdir)
    or $coldb->logconfess("create(): could not create output directory $dbdir: $!");

  ##-- initialize: enums
  my $wenum = $coldb->{wenum} = CollocDB::Enum->new();
  my $ws2i  = $wenum->{s2i}{data};
  my $nw    = 0;
  #
  my $lenum = $coldb->{lenum} = CollocDB::Enum->new();
  my $ls2i  = $lenum->{s2i}{data};
  my $nl    = 0;
  #
  my $xenum = $coldb->{xenum} = CollocDB::Enum->new();
  my $xs2i  = $xenum->{s2i}{data};
  my $nx    = 0;

  ##-- initialize: corpus token-storage (temporary)
  my $tokfile =  "$dbdir/tokens.bin";
  CORE::open(my $tokfh, ">$tokfile")
    or $coldb->logconfess("$0: open failed for $tokfile: $!");
  #my $tokpack = substr($PDL::Types::pack[$PDL::Types::typehash{PDL_L}{numval}],0,1);
  my $tokpack = 'L';

  ##-- initialize: enums
  $coldb->vlog('info',"processing corpus files");
  my ($doc, $date,$tok,$w,$l,$wi,$li,$x,$xi);
  for ($corpus->ibegin(); $corpus->iok; $corpus->inext) {
    $coldb->vlog($coldb->{logParseFile}, "processing file ", $corpus->ifile);
    $doc  = $corpus->idocument();
    $date = $doc->{date};

    foreach $tok (@{$doc->{tokens}}) {
      ($w,$l) = @$tok;
      $wi = $ws2i->{$w} = ++$nw if (!defined($wi=$ws2i->{$w}));
      $li = $ls2i->{$l} = ++$nl if (!defined($li=$ls2i->{$l}));
      $xi = $xs2i->{$x} = ++$nx if (!defined($xi=$xs2i->{$x=pack('nNN',$date,$wi,$li)}));

      ##-- save to token-fh
      $tokfh->print(pack($tokpack,$xi));
    }
  }

  ##-- close token storage
  $tokfh->close()
    or $corpus->logconfess("create(): failed to close temporary token storage file '$tokfile': $!");

  ##-- compile: wenum
  $coldb->vlog('info', "creating word-enum DB $dbdir/wenum.*\n");
  @{$wenum->{i2s}{data}}{values %$ws2i} = keys %$ws2i;
  $wenum->{size} = $nw;
  $wenum->saveDbFile("$dbdir/wenum")
    or $coldb->logconfess("create(): failed to save $dbdir/wenum: $!");

  ##-- compile: lenum
  $coldb->vlog('info',"creating lemma-enum DB $dbdir/lenum.*\n");
  @{$lenum->{i2s}{data}}{values %$ls2i} = keys %$ls2i;
  $lenum->{size} = $nl;
  $lenum->saveDbFile("$dbdir/lenum")
    or $coldb->logconfess("create(): failed to save $dbdir/lenum: $!");

  ##-- compile: xenum
  $coldb->vlog('info', "creating tuple-enum DB $dbdir/xenum.*\n");
  @{$xenum->{i2s}{data}}{values %$xs2i} = keys %$xs2i;
  $xenum->{size} = $nx;
  $xenum->saveDbFile("$dbdir/xenum")
    or $coldb->logconfess("create(): failed to save $dbdir/xenum: $!");

  ##-- create w2x, l2x
  $coldb->vlog('info',"computing expansion maps w2x, l2x");
  my %w2xi = qw();
  my %l2xi = qw();
  while (($x,$xi)=each %$xs2i) {
    ($wi,$li)   = unpack('@2NN',$x);
    $w2xi{$wi} .= pack('N',$xi);
    $l2xi{$li} .= pack('N',$xi);
  }

  $coldb->vlog('info',"creating $dbdir/w2x.db");
  my $w2xdb = $coldb->{w2x} = CollocDB::DBFile->new(file=>"$dbdir/w2x.db", pack_key=>'N')
    or die("$0: failed to create $dbdir/w2x.db: $!");
  my $w2xdata = $w2xdb->{data};
  $w2xdata->{$_} = $w2xi{$_} foreach (sort {$a<=>$b} keys %w2xi);
  %w2xi = qw();

  $coldb->vlog('info',"creating $dbdir/l2x.db");
  my $l2xdb = $coldb->{l2x} = CollocDB::DBFile->new(file=>"$dbdir/l2x.db", pack_key=>'N')
    or die("$0: failed to create $dbdir/l2x.db: $!");
  my $l2xdata = $l2xdb->{data};
  $l2xdata->{$_} = $l2xi{$_} foreach (sort {$a<=>$b} keys %l2xi);
  %l2xi = qw();

  ##-- all done
  $coldb->info("DB created.");

  ##-- cleanup
  #unlink($tokfile);

  return $coldb;
}


##==============================================================================
## Footer
1;

__END__




