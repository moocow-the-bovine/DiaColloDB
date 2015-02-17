## -*- Mode: CPerl -*-
## File: DiaColloDB::DBFile.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Descript: collocation db: Berkely DB: tied Files


package DiaColloDB::DBFile;
use DiaColloDB::Utils qw(:fcntl :pack);
use DiaColloDB::Logger;
use DB_File;
use Fcntl;
use IO::File;
use Carp;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

## $DEFAULT_TYPE
##  + default file type if otherwise unspecified
our $DEFAULT_TYPE = 'BTREE';

##==============================================================================
## Constructors etc.

## $dbf = CLASS_OR_OBJECT->new(%opts)
## + %opts, %$doc:
##   ##-- user options
##   file  => $filename,   ##-- default: undef (none)
##   perms  => $perms,     ##-- default: 0666 & ~umask
##   flags  => $flags,     ##-- default: O_RDWR|O_CREAT
##   type   => $type,      ##-- one of 'HASH', 'BTREE', 'RECNO', 'GUESS' (default: 'GUESS' for read, $DEFAULT_TYPE otherwise)
##   dbinfo => \%dbinfo,   ##-- default: "DB_File::${type}INFO"->new();
##   dbopts => \%opts,     ##-- db options (e.g. cachesize,bval,...) -- defaults to none (uses DB_File defaults)
##   pack_key => $packas,  ##-- pack template for db keys (default=undef); see DiaColloDB::Utils::packFilterStore()
##   pack_val => $packas,  ##-- pack template for db values (default=undef); see DiaColloDB::Utils::packFilterStore()
##   ##
##   ##-- low-level data
##   data   => $thingy,    ##-- tied data (hash or array)
##   tied   => $ref,       ##-- reference returned by tie()
sub new {
  my $that = shift;
  my $db = bless({
		  file   => undef,
		  perms   => (0666 & ~umask),
		  flags  => (O_RDWR|O_CREAT),
		  type   => undef, ##-- no default type (guess)
		  dbinfo => undef,
		  dbopts => {
			     cachesize=>'32M',
			     psize=>'64K',
			    },
		  pack_key => undef,
		  pack_val => undef,
		  data   => undef,
		  tied   => undef,
		  @_
		 }, ref($that)||$that);
  return $db->open($db->{file}) if (defined($db->{file}));
  return $db;
}

## undef = $dbf->clear()
##  + clears data (if any)
sub clear {
  my $dbf = shift;
  return if (!$dbf->opened);
  if (uc($dbf->{type}) eq 'RECNO') {
    $dbf->{tied}->splice(0,scalar(@{$dbf->{data}}));
  } else {
    %{$dbf->{data}} = qw();
  }
  return $dbf;
}

##==============================================================================
## Class Methods

## $typeString = $that->guessFileType($filename_or_handle)
##  + attempts to guess type of $filename; returns undef on failure
##  + this will die() if $filename doesn't already exist
##  + magic codes grabbed from /usr/share/file/magic
sub guessFileType {
  my ($that,$file) = @_;
  my $fh  = ref($file) ? $file     : IO::File->new("<$file");
  my $pos = ref($file) ? tell($fh) : undef;
  $that->logconfess("guessFileType(): open failed for '$file': $!") if (!$fh);
  CORE::binmode($fh);
  my ($buf,$typ,$magic,$fmt);
  seek($fh,12,SEEK_SET) or return undef;
  read($fh,$buf,4)==4   or return undef;
  foreach $fmt (qw(L N V)) {
    $magic = unpack($fmt,$buf);
    if    ($magic == 0x00053162) { $typ='BTREE'; last; }
    elsif ($magic == 0x00061561) { $typ='HASH'; last; }
  }
  if (ref($file)) {
    seek($fh,$pos,SEEK_SET); ##-- return handle to original position
  } else {
    $fh->close();
  }
  return $typ;
}


##==============================================================================
## Methods: I/O

## $bool = $dbf->opened()
sub opened {
  return defined($_[0]{tied});
}

## $dbf = $dbf->close()
sub close {
  my $dbf = shift;
  return $dbf if (!$dbf->opened);
  $dbf->{tied} = undef;
  if (uc($dbf->{type}) eq 'RECNO') {
    untie(@{$dbf->{data}});
  } else {
    untie(%{$dbf->{data}});
  }
  delete $dbf->{data};
  return $dbf;
}

## $sizeInt = parseSize($sizeString)
sub parseSize {
  my ($dbf,$str) = @_;
  if (defined($str) && $str =~ /^\s*([\d\.\+\-eE]*)\s*([BKMGT]?)\s*$/i) {
    my ($size,$suff) = ($1,$2);
    $suff = 'B' if (!defined($suff));
    $suff = uc($suff);
    $size *= 1024    if ($suff eq 'K');
    $size *= 1024**2 if ($suff eq 'M');
    $size *= 1024**3 if ($suff eq 'G');
    $size *= 1024**4 if ($suff eq 'T');
    return $size;
  }
  return $str;
}

## \%opts = $dbf->parseOptions(\%opts=$dbf->{dbopts})
sub parseOptions {
  my ($dbf,$dbopts) = @_;
  $dbopts = $dbf->{dbopts} if (!$dbopts);
  $dbopts = $dbf->{dbopts} = {} if (!$dbopts);

  ##-- parse: size arguments
  foreach (qw(cachesize psize)) {
    $dbopts->{$_} = $dbf->parseSize($dbopts->{$_}) if (defined($dbopts->{$_}));
  }

  ##-- delete undef arguments
  delete @$dbopts{grep {!defined($dbopts->{$_})} keys %$dbopts};

  return $dbopts;
}


## $dbf = $dbf->open($file,%opts)
##  + %opts are as for new()
##  + $file defaults to $dbf->{file}
sub open {
  my ($dbf,$file,%opts) = @_;
  $dbf->close() if ($dbf->opened);
  @$dbf{keys %opts} = values(%opts);
  $file = $dbf->{file} if (!defined($file));
  $dbf->{file} = $file;
  $dbf->parseOptions();

  ##-- truncate file here if user specified O_TRUNC, since DB_File doesn't
  my $flags = fcflags($dbf->{flags});
  if (($flags & O_TRUNC) && defined($dbf->{file}) && -e $dbf->{file}) {
    $dbf->truncate()
      or $dbf->logconfess("open(O_TRUNC): could not unlink file '$dbf->{file}': $!");
  }

  ##-- guess file type
  if (!defined($dbf->{type}) || uc($dbf->{type}) eq 'GUESS') {
    if (-e $dbf->{file}) {
      $dbf->{type} = $dbf->guessFileType($dbf->{file}) || 'RECNO';
    }
    $dbf->{type} = $DEFAULT_TYPE if (!defined($dbf->{type}) || uc($dbf->{type}) eq 'GUESS'); ##-- last-ditch effort
  }

  ##-- setup info
  $dbf->{dbinfo} = ("DB_File::".uc($dbf->{type})."INFO")->new();
  @{$dbf->{dbinfo}}{keys %{$dbf->{dbopts}}} = values %{$dbf->{dbopts}};

  if (uc($dbf->{type}) eq 'RECNO') {
    ##-- tie: recno (array)
    $dbf->{dbinfo}{flags} |= R_FIXEDLEN if (defined($dbf->{dbinfo}{reclen}));
    $dbf->{data} = [];
    $dbf->{tied} = tie(@{$dbf->{data}}, 'DB_File', $dbf->{file}, $flags, $dbf->{perms}, $dbf->{dbinfo})
      or $dbf->logconfess("open(): tie() failed for ARRAY file '$dbf->{file}': $!");
  } else {
    ##-- tie: btree or hash (hash)
    $dbf->{data} = {};
    $dbf->{tied} = tie(%{$dbf->{data}}, 'DB_File', $dbf->{file}, $flags, $dbf->{perms}, $dbf->{dbinfo})
      or $dbf->logconfess("open(): tie() failed for $dbf->{type} file '$dbf->{file}': $!");
  }

  ##-- setup db filters
  $dbf->setFilters();
  return $dbf;
}

## undef = nullfilter()
##  + workaround for annoying 'Use of uninitialized value in null operation at DiaColloDB/Enum.pm'
##    if we end up calling filter_fetch_key(undef), which according to the DB_File manpage ought
##    to just un-install the filter, but doesn't
sub nullfilter {}

## $bool = $dbf->setFilters(%opts)
##  + %opts : override (but don't clobber) %$dbf
sub setFilters {
  my ($dbf,%opts) = @_;
  return undef if (!$dbf->opened);
  my $pack_key = exists($opts{pack_key}) ? $opts{pack_key} : $dbf->{pack_key};
  my $pack_val = exists($opts{pack_val}) ? $opts{pack_val} : $dbf->{pack_val};

  ##-- optionally install pack filters
  if (uc($dbf->{type}) ne 'RECNO') {
      $dbf->{tied}->filter_fetch_key(packFilterFetch($pack_key) // \&nullfilter);
      $dbf->{tied}->filter_store_key(packFilterStore($pack_key) // \&nullfilter);
  }
  $dbf->{tied}->filter_fetch_value(packFilterFetch($pack_val) // \&nullfilter);
  $dbf->{tied}->filter_store_value(packFilterStore($pack_val) // \&nullfilter);
  return $dbf;
}

## $bool = $dbf->truncate()
## $bool = $CLASS_OR_OBJ->truncate($file)
##  + actually calls unlink($file)
##  + no-op if $file and $dbf->{file} are both undef
sub truncate {
  my ($dbf,$file) = @_;
  $file = $dbf->{file} if (!defined($file));
  return if (!defined($file));
  unlink($file);
}

## $bool = $dbf->sync()
## $bool = $dbf->sync($flags)
sub sync {
  my $dbf = shift;
  return 1 if (!$dbf->opened);
  return $dbf->{tied}->sync(@_) == 0;
}

##==============================================================================
## Dump

## $bool = $dbf->saveTextFile($filename_or_fh)
##  + save from text file with lines of the form "KEY VALUE(s)..."
sub saveTextFile {
  my ($dbf,$file) = @_;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $dbf->logconfess("saveTextFile(): failed to open '$file': $!") if (!ref($fh));

  my ($key,$val,$status);
  if ($dbf->opened) {
    my $tied = $dbf->{tied};
    for ($status=$tied->seq($key,$val,R_FIRST); $status==0; $status=$tied->seq($key,$val,R_NEXT)) {
      $fh->print($key, "\t", (ref($val) ? join("\t",@$val) : $val), "\n");
    }
  } else {
    my $data = $dbf->{data} // {};
    foreach $key (sort {$a<=>$b} keys %$data) {
      $val = $data->{$key};
      $fh->print($key, "\t", (ref($val) ? join("\t",@$val) : $val), "\n");
    }
  }
  $fh->close() if (!ref($file));

  return $dbf;
}


##==============================================================================
## Footer
1;

__END__
