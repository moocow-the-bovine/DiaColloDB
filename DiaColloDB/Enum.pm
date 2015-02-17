## -*- Mode: CPerl -*-
## File: DiaColloDB::Enum.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, symbol<->integer enum, db-based ::: OBSOLETE (prefer EnumFile family)

package DiaColloDB::Enum;
use DiaColloDB::Logger;
use DiaColloDB::DBFile;
use DiaColloDB::Utils qw(:fcntl :json);
use DB_File;
use Fcntl;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## Constructors etc.

## $cldb = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    base => $base,       ##-- database basename; use files "${base}.s2i", "${base}.i2s", "${base}.hdr"
##    perms => $perms,     ##-- default: 0666 & ~umask
##    flags => $flags,     ##-- default: 'rw'
##    pack_i => $pack_i,   ##-- integer pack template (default='N')
##    pack_s => $pack_s,   ##-- string pack template (default=undef)
##    size => $size,       ##-- number of mapped symbols, like scalar(keys %{$s2idb->{data}})
##    s2i => $s2idb,       ##-- a DiaColloDB::DBFile object mapping symbols to integers
##    i2s => $i2sdb,       ##-- a DiaColloDB::DBFile object mapping integers to symbols
##   )
sub new {
  my $that = shift;
  my $enum  = bless({
		     base => undef,
		     perms => (0666 & ~umask),
		     flags => 'ra',
		     size => 0,
		     pack_i => 'N',
		     pack_s => undef,
		     s2i => DiaColloDB::DBFile->new(),
		     i2s => DiaColloDB::DBFile->new(),

		     @_, ##-- user arguments
		    },
		    ref($that)||$that);
  $enum->{class} = ref($enum);
  $enum->{s2i}{data} //= {};
  $enum->{i2s}{data} //= {};
  return defined($enum->{base}) ? $enum->open($enum->{base}) : $enum;
}

sub DESTROY {
  $_[0]->close() if ($_[0]->opened);
}

## $enum = $enum->clear()
sub clear {
  my $enum = shift;
  if ($enum->opened) {
    $enum->{s2i}{tied}->CLEAR();
    $enum->{i2s}{tied}->CLEAR();
  } else {
    %{$enum->{s2i}{data}} = qw();
    %{$enum->{i2s}{data}} = qw();
  }
  $enum->{size} = 0;
  return $enum;
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: open/close (db)

## $enum_or_undef = $enum->open($base,$flags)
## $enum_or_undef = $enum->open($base)
## $enum_or_undef = $enum->open()
sub open {
  my ($enum,$base,$flags) = @_;
  $base  //= $enum->{base};
  $flags //= $enum->{flags};
  $enum->close() if ($enum->opened);
  $enum->{base}  = $base;
  $enum->{flags} = $flags = fcflags($flags);
  if (fcread($flags) && !fctrunc($flags)) {
    $enum->loadHeader()
      or $enum->logconess("failed to load header from '$enum->{base}.hdr': $!");
  }
  $enum->{s2i}->open("$base.s2i", perms=>$enum->{perms}, flags=>$flags, pack_key=>$enum->{pack_s}, pack_val=>$enum->{pack_i})
    or $enum->logconfess("open failed for $base.s2i: $!");
  $enum->{i2s}->open("$base.i2s", perms=>$enum->{perms}, flags=>$flags, pack_key=>$enum->{pack_i}, pack_val=>$enum->{pack_s})
    or $enum->logconfess("open failed for $base.i2s: $!");
  return $enum;
}

## $enum_or_undef = $enum->close()
sub close {
  my $enum = shift;
  if ($enum->opened && fcwrite($enum->{flags})) {
    $enum->saveHeader() or return undef;
  }
  $enum->{s2i}->close() or return undef;
  $enum->{i2s}->close() or return undef;
  $enum->{s2i}{data} //= {};
  $enum->{i2s}{data} //= {};
  undef $enum->{base};
  return $enum;
}

## $bool = $enum->opened()
sub opened {
  my $enum = shift;
  return
    (defined($enum->{base})
     && $enum->{s2i}->opened
     && $enum->{i2s}->opened);
}

## $bool = $enum->sync()
## $bool = $enum->sync($syncflags)
sub sync {
  my $enum = shift;
  return 1 if (!enum->opened);
  return $enum->{s2i}->sync(@_) && $enum->{i2s}->sync(@_) && $enum->saveHeader();
}

## $bool = $enum->setFilters(%opts)
##  + %opts : override @$enum{qw(pack_s pack_i)}
sub setFilters {
  my ($enum,%opts) = @_;
  return undef if (!$enum->opened);
  my $pack_i = exists($opts{pack_i}) ? $opts{pack_i} : $enum->{pack_i};
  my $pack_s = exists($opts{pack_s}) ? $opts{pack_s} : $enum->{pack_s};
  $enum->{s2i}->setFilters(pack_key=>$pack_s, pack_val=>$pack_i);
  $enum->{i2s}->setFilters(pack_key=>$pack_i, pack_val=>$pack_s);
  return $enum;
}

##--------------------------------------------------------------
## I/O: db <-> in-memory enum

## $enum = $enum->loadEnum($e2,%opts)
##  + %opts:
##     append => $bool,  ## if true, append symbols to current enum; otherwise just clone $e2
sub loadEnum {
  my ($e1,$e2,%opts) = @_;
  $e1 = $e1->new() if (!ref($e1));
  if ($opts{append}) {
    ##-- append: ensure all symbols are defined
    $e1->addSymbols([keys %{$e2->{s2i}{data}}]);
  }
  elsif ($e1->opened && !$e2->opened) {
    ##-- clone mem->db : insert in sorted order
    my $e1_s2i = $e1->{s2i}{data};
    my $e2_s2i = $e2->{s2i}{data};
    foreach (sort keys %$e2_s2i) {
      $e1_s2i->{$_} = $e2_s2i->{$_};
    }
    my $e1_i2s = $e1->{i2s}{data};
    my $e2_i2s = $e2->{i2s}{data};
    foreach (sort {$a<=>$b} keys %$e2_i2s) {
      $e1_i2s->{$_} = $e2_i2s->{$_};
    }
    $e1->{size} = $e2->{size};
  }
  else {
    ##-- clone db->* or mem->mem : just assign
    %{$e1->{s2i}{data}} = %{$e2->{s2i}{data}};
    %{$e1->{i2s}{data}} = %{$e2->{i2s}{data}};
    $e1->{size} = $e2->{size};
  }
  return $e1;
}

## $bool = $enum->loadDbFile($base,%opts)
##  + %opts:
##     append => $bool,  ## if true, just append symbols to current enum
sub loadDbFile {
  my ($enum,$base,%opts) = @_;
  $enum  = $enum->new() if (!ref($enum));
  my $e2 = $enum->new(pack_i=>$enum->{pack_i}, pack_s=>$enum->{pack_s});
  $e2->open($base,'r')
    or $enum->logconfess("loadDbFile(): failed to open '$base': $!");
  $enum->loadEnum($e2, %opts)
    or $enum->logconfess("loadDbFile(): failed to load mappings from '$base': $!");
  $e2->close();
  return $enum;
}

## $bool = $enum->saveDbFile($base,%opts)
##  + %opts:
##     append => $bool,  ## if true, append to existing DB
sub saveDbFile {
  my ($enum,$base,%opts) = @_;
  $enum  = $enum->new() if (!ref($enum));
  my $e2 = $enum->new(pack_i=>$enum->{pack_i}, pack_s=>$enum->{pack_s});
  $e2->open($base,'r'.($opts{append} ? 'a' : 'w'))
    or $enum->logconfess("saveDbFile(): failed to open '$base': $!");
  $e2->loadEnum($enum)
    or $enum->logconfess("saveDbFile(): failed to save mappings to '$base': $!");
  $e2->close();
  return $enum;
}

##--------------------------------------------------------------
## I/O: from perl structures

## $enum = $enum->fromArray(\@i2s)
##  + clobbers $enum contents
sub fromArray {
  my ($enum,$i2s) = @_;
  $enum->clear();
  my @is = (0..$#$i2s);
  @{$enum->{i2s}{data}}{@is} = @$i2s;
  @{$enum->{s2i}{data}}{@$i2s} = (@is);
  return $enum;
}

## $enum = $enum->fromHash(\%s2i)
##  + clobbers $enum contents, steals \%s2i if not opened
sub fromHash {
  my ($enum,$s2i) = @_;
  $enum->clear();
  if ($enum->opened) {
    my $es2i = $enum->{s2i}{data};
    my $ei2s = $enum->{i2s}{data};
    my ($i);
    my $imax = -1;
    foreach (sort keys %$s2i) {
      $i = $s2i->{$_};
      $es2i->{$_} = $i;
      $ei2s->{$i} = $_;
      $imax = $i if ($i > $imax);
    }
    $enum->{size} = $imax+1;
  }
  else {
    $enum->{s2i}{data} = $s2i;
    @{$enum->{i2s}{data}}{values %$s2i} = keys %$s2i;
    $enum->{size} = scalar(keys %$s2i);
  }
  return $enum;
}



##--------------------------------------------------------------
## I/O: header

## @keys = $coldb->headerKeys()
##  + keys to save as header
sub headerKeys {
  return grep {!ref($_[0]{$_})} keys %{$_[0]};
}

## $bool = $enum->loadHeader()
sub loadHeader {
  my $enum = shift;
  my $hdr = loadJsonFile("$enum->{base}.hdr");
  if (!defined($hdr) && (fcflags($enum->{flags})&O_CREAT) != O_CREAT) {
    $enum->logconfess("loadHeader() failed to load '$enum->{base}.hdr': $!");
  }
  elsif (defined($hdr)) {
    @$enum{keys %$hdr} = values(%$hdr);
  }
  return $enum;
}

## $bool = $enum->saveHeader()
sub saveHeader {
  my $enum = shift;
  return undef if (!$enum->opened);
  my $hdr  = {map {($_=>$enum->{$_})} $enum->headerKeys};
  saveJsonFile($hdr, "$enum->{base}.hdr")
    or $enum->logconfess("saveHeader() failed to save '$enum->{base}.hdr': $!");
  return $enum;
}

##--------------------------------------------------------------
## I/O: text

## $enum = $CLASS_OR_OBJECT->loadTextFile($filename_or_fh)
##  + loads from text file with lines of the form "ID SYMBOL..."
sub loadTextFile {
  my ($enum,$file) = @_;
  my $fh = ref($file) ? $file : IO::File->new("<$file");
  $enum->logconfess("loadTextFile(): failed to open '$file': $!") if (!$fh);
  my $size = $enum->{size};
  my @rows = qw();
  my ($i,$s);
  while (defined($_=<$fh>)) {
    chomp;
    next if (/^%%/ || /^$/);
    ($i,$s) = split(/\s/,$_,2);
    push(@rows,[$i,$s]);
    $size = $i+1 if ($i >= $size);
  }
  $fh->close() if (!ref($file));

  ##-- append to dbs (or in-memory cache)
  $enum->{s2i}{data} //= {};
  $enum->{i2s}{data} //= {};
  my $s2i = $enum->{s2i}{data};
  my $i2s = $enum->{i2s}{data};
  if ($enum->opened) {
    ##-- db mode: insert in sorted order
    $s2i->{$_->[1]} = $_->[0] foreach (sort {$a->[1] cmp $b->[1]} @rows);
    $i2s->{$_->[0]} = $_->[1] foreach (sort {$a->[0] <=> $b->[0]} @rows);
  } else {
    ##-- in-memory mode: insert in load order
    foreach (@rows) {
      $s2i->{$_->[1]} = $_->[0];
      $i2s->{$_->[0]} = $_->[1];
    }
  }
  $enum->{size} = $size;

  return $enum;
}

## $bool = $enum->saveTextFile($filename_or_fh)
##  + save from text file with lines of the form "ID SYMBOL..."
sub saveTextFile {
  my ($enum,$file) = @_;
  return $enum->{i2s}->saveTextFile($file);
}


##==============================================================================
## Methods: population

## $newsize = $enum->addSymbols(@symbols)
## $newsize = $enum->addSymbols(\@symbols)
##  + adds all symbols in @symbols which don't already exists
sub addSymbols {
  my $enum    = shift;
  my $symbols = UNIVERSAL::isa($_[0],'ARRAY') ? $_[0] : \@_;
  my $n   = $enum->{size};
  my $s2i = $enum->{s2i}{data};
  my $i2s = $enum->{i2s}{data};
  foreach (sort @$symbols) {
    next if (exists $s2i->{$_});
    $s2i->{$_} = $n;
    $i2s->{$n} = $_;
    ++$n;
  }
  return $enum->{size}=$n;
}

## $newsize = $enum->appendSymbols(@symbols)
## $newsize = $enum->appendSymbols(\@symbols)
##  + adds all symbols in @symbols in order, re-mapping them if they already exist
sub appendSymbols {
  my $enum    = shift;
  my $symbols = UNIVERSAL::isa($_[0],'ARRAY') ? $_[0] : \@_;
  my $n   = $enum->{size};
  my $s2i = $enum->{s2i}{data};
  my $i2s = $enum->{i2s}{data};
  foreach (@$symbols) {
    $s2i->{$_} = $n;
    $i2s->{$n} = $_;
    ++$n;
  }
  return $enum->{size}=$n;
}


##==============================================================================
## Footer
1;

__END__




