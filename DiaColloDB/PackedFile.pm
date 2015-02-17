## -*- Mode: CPerl -*-
## File: DiaColloDB::PackedFile.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Descript: collocation db: flat fixed-length record-oriented files

package DiaColloDB::PackedFile;
use DiaColloDB::Utils qw(:fcntl :pack);
use DiaColloDB::Logger;
use Tie::Array;
use Fcntl;
use IO::File;
use Carp;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger Tie::Array);

##==============================================================================
## Constructors etc.

## $pf = CLASS_OR_OBJECT->new(%opts)
## + %opts, %$doc:
##   ##-- user options
##   file     => $filename,   ##-- default: undef (none)
##   flags    => $flags,      ##-- fcntl flags or open-mode (default='r')
##   perms    => $perms,      ##-- creation permissions (default=(0666 &~umask))
##   reclen   => $reclen,     ##-- record-length in bytes: (default: guess from pack format if available)
##   packas   => $packas,     ##-- pack-format or array; see DiaColloDB::Utils::packFilterStore();
##   ##
##   ##-- filters
##   filter_fetch => $filter, ##-- DB_File-style filter for fetch
##   filter_store => $filter, ##-- DB_File-style filter for store
##   ##
##   ##-- low-level data
##   fh       => $fh,         ##-- underlying filehandle
sub new {
  my $that = shift;
  my $pf = bless({
		  file   => undef,
		  flags  => 'r',
		  perms  => (0666 & ~umask),
		  reclen => undef,
		  #packas => undef,

		  ##-- filters
		  #filter_fetch => undef,
		  #filter_store => undef,

		  ##-- low level data
		  #fh     => undef,

		  ##-- user args
		  @_
		 }, ref($that)||$that);
  $pf->{class} = ref($pf);
  return $pf->open() if (defined($pf->{file}));
  return $pf;
}

##==============================================================================
## API: open/close

## $pf = $pf->open()
## $pf = $pf->open($file)
## $pf = $pf->open($file,$flags,%opts)
##  + %opts are as for new()
##  + $file defaults to $pf->{file}
sub open {
  my ($pf,$file,$flags,%opts) = @_;
  $pf->close() if ($pf->opened);
  @$pf{keys %opts} = values(%opts);
  $flags = $pf->{flags} = fcflags($flags // $pf->{flags});
  return undef if (!defined($pf->{file} = $file = ($file // $pf->{file})));
  $pf->{fh} = fcopen($file, $flags, $pf->{perms})
    or return undef;
  binmode($pf->{fh},':raw');
  $pf->setFilters();
  return $pf;
}

## $bool = $pf->opened()
sub opened {
  return defined($_[0]{fh});
}

## $bool = $pf->close()
sub close {
  my $pf = shift;
  my $rc = defined($pf->{fh}) ? CORE::close($pf->{fh}) : 1;
  delete $pf->{fh};
  $pf->{size} = 0;
  return $rc;
}

## $bool = $pf->setsize($nrecords)
sub setsize {
  if ($_[1] > $_[0]->size) {
    ##-- grow
    CORE::seek($_[0]{fh}, $_[1]*$_[0]{reclen}-1, SEEK_SET)
      or $_[0]->logconfess(__PACKAGE__, "::setsize() failed to grow file to $_[1] elements: $!");
    $_[0]{fh}->print("\0");
  }
  else {
    ##-- shrink
    CORE::truncate($_[0]{fh}, $_[1]*$_[0]{reclen})
      or $_[0]->logconfess(__PACKAGE__, "::setsize() failed to shrink file to $_[1] elements: $!");
  }
  return 1;
}

## $bool = $pf->truncate()
##  + truncates $pf->{fh} or $pf->{file}; otherwise a no-nop
sub truncate {
  my $pf = shift;
  if (defined($pf->{fh})) {
    return CORE::truncate($pf->{fh},0) ;
  }
  elsif (defined($pf->{file})) {
    my $fh = fcopen($pf->{file}, (O_WRONLY|O_CREAT|O_TRUNC)) or return undef;
    return CORE::close($fh);
  }
  return undef;
}

## $bool = $pf->flush()
##  + attempt to flush underlying filehandle, may not work
sub flush {
  my $pf = shift;
  return undef if (!$pf->opened);
  return $pf->{fh}->flush() if (UNIVERSAL::can($pf->{fh},'flush'));
  return binmode($pf->{fh},':raw'); ##-- see perlfaq5(1) and 
}


##==============================================================================
## API: filters

## $pf = $pf->setFilters($packfmt)
## $pf = $pf->setFilters([$packfmt, $unpackfmt])
## $pf = $pf->setFilters([\&packsub,\&unpacksub])
##  + %opts : override (but don't clobber) $pf->{packfmt}
sub setFilters {
  my ($pf,$packfmt) = @_;
  $packfmt //= $pf->{packas};
  $pf->{filter_fetch} = packFilterFetch($packfmt);
  $pf->{filter_store} = packFilterStore($packfmt);
  if (!defined($pf->{reclen}) && defined($pf->{filter_store})) {
    ##-- guess record length from pack filter output
    use bytes;
    no warnings;
    local $_ = 0;
    $pf->{filter_store}->();
    $pf->{reclen} = length($_);
  }
  return $pf;
}

##==============================================================================
## API: positioning

## $nrecords = $pf->size()
##  + returns number of records
sub size {
  return undef if (!$_[0]{fh});
  return (-s $_[0]{fh}) / $_[0]{reclen};
}

## $bool = $pf->seek($recno)
##  + seek to record-number $recno
sub seek {
  CORE::seek($_[0]{fh}, $_[1]*$_[0]{reclen}, SEEK_SET);
}

## $recno = $pf->tell()
##  + report current record-number
sub tell {
  return CORE::tell($_[0]{fh}) / $_[0]{reclen};
}

## $bool = $pf->reset();
##   + reset position to beginning of file
sub reset {
  return $_[0]->seek(0);
}

## $bool = $pf->seekend()
##  + seek to end-of file
sub seekend {
  CORE::seek($_[0]{fh}, 0, SEEK_END);
}

## $bool = $pf->eof()
##  + returns true iff current position is end-of-file
sub eof {
  return CORE::eof($_[0]{fh});
}

##==============================================================================
## API: record access

##--------------------------------------------------------------
## API: record access: read

## $bool = $pf->read(\$buf)
##  + read a raw record into \$buf
sub read {
  return CORE::read($_[0]{fh}, ${$_[1]}, $_[0]{reclen})==$_[0]{reclen};
}

## $bool = $pf->readraw(\$buf, $nrecords)
##  + batch-reads $nrecords into \$buf
sub readraw {
  return CORE::read($_[0]{fh}, ${$_[1]}, $_[2]*$_[0]{reclen})==$_[2]*$_[0]{reclen};
}

## $value_or_undef = $pf->get()
##  + get (unpacked) value of current record, increments filehandle position to next record
sub get {
  local $_=undef;
  CORE::read($_[0]{fh}, $_, $_[0]{reclen})==$_[0]{reclen} or return undef;
  $_[0]{filter_fetch}->() if ($_[0]{filter_fetch});
  return $_;
}

## \$buf_or_undef = $pf->getraw(\$buf)
##  + get (packed) value of current record, increments filehandle position to next record
sub getraw {
  CORE::read($_[0]{fh}, ${$_[1]}, $_[0]{reclen})==$_[0]{reclen} or return undef;
  return $_[1];
}

## $value_or_undef = $pf->fetch($index)
##  + get (unpacked) value of record $index
sub fetch {
  local $_=undef;
  CORE::seek($_[0]{fh}, $_[1]*$_[0]{reclen}, SEEK_SET) or return undef;
  CORE::read($_[0]{fh}, $_, $_[0]{reclen})==$_[0]{reclen} or return undef;
  $_[0]{filter_fetch}->() if ($_[0]{filter_fetch});
  return $_;
}

## $buf_or_undef = $pf->fetchraw($index,\$buf)
##  + get (packed) value of record $index
sub fetchraw {
  CORE::seek($_[0]{fh}, $_[1]*$_[0]{reclen}, SEEK_SET) or return undef;
  CORE::read($_[0]{fh}, ${$_[2]}, $_[0]{reclen})==$_[0]{reclen} or return undef;
  return ${$_[2]};
}

##--------------------------------------------------------------
## API: record access: write

## $bool = $pf->write($buf)
##  + write a raw record $buf to current position; increments position
sub write {
  $_[0]{fh}->print($_[1]);
}

## $value_or_undef = $pf->set($value)
##  + set (packed) value of current record, increments filehandle position to next record
sub set {
  local $_=$_[1];
  $_[0]{filter_store}->() if ($_[0]{filter_store});
  $_[0]{fh}->print($_) or return undef;
  return $_[1];
}

## $value_or_undef = $pf->store($index,$value)
##  + store (packed) $value as record-number $index
sub store {
  CORE::seek($_[0]{fh}, $_[1]*$_[0]{reclen}, SEEK_SET) or return undef;
  local $_=$_[2];
  $_[0]{filter_store}->() if ($_[0]{filter_store});
  $_[0]{fh}->print($_) or return undef;
  return $_[2];
}

## $value_or_undef = $pf->push($value)
##  + store (packed) $value at end of record
sub push {
  CORE::seek($_[0]{fh}, 0, SEEK_END) or return undef;
  local $_ = $_[1];
  $_[0]{filter_store}->() if ($_[0]{filter_store});
  $_[0]{fh}->print($_) or return undef;
  return $_[1];
}

##==============================================================================
## API: binary search

## $index_or_undef = $pf->bsearch($key, %opts)
##  + %opts:
##    lo => $ilo,        ##-- index lower-bound for search (default=0)
##    hi => $ihi,        ##-- index upper-bound for search (default=size)
##    packas => $packas, ##-- key-pack template (default=$pf->{packas})
##  + returns the minimum index $i such that unpack($packas,$pf->[$i]) == $key and $ilo <= $j < $i,
##    or undef if no such $i exists.
##  + $key must be a numeric value, and records must be stored in ascending order
##    by numeric value of key (as unpacked by $packas) between $ilo and $ihi
sub bsearch {
  my ($pf,$key,%opts) = @_;
  my $ilo    = $opts{lo} // 0;
  my $ihi    = $opts{hi} // $pf->size;
  my $packas = $opts{packas} // $pf->{packas};

  ##-- binary search guts
  my ($imid,$buf,$keymid);
  while ($ilo < $ihi) {
    $imid = ($ihi+$ilo) >> 1;

    ##-- get item[$imid]
    $pf->fetchraw($imid,\$buf);
    ($keymid) = unpack($packas,$buf);

    if ($keymid < $key) {
      $ilo = $imid + 1;
    } else {
      $ihi = $imid;
    }
  }

  if ($ilo==$ihi) {
    ##-- get item[$ilo]
    $pf->fetchraw($ilo,\$buf);
    ($keymid) = unpack($packas,$buf);
    return $ilo if ($keymid == $key);
  }

  return undef;
}


##==============================================================================
## API: text I/O

## $bool = $pf->saveTextFile($filename_or_fh, %opts)
##  + save from text file with lines of the form "KEY? VALUE(s)..."
##  + %opts:
##      keys=>$bool,  ##-- do/don't save keys (default=true)
sub saveTextFile {
  my ($pf,$file,%opts) = @_;
  $pf->logconfess("saveTextFile(): no packed-file opened!") if (!$pf->opened);

  my $outfh = ref($file) ? $file : IO::File->new(">$file");
  $pf->logconfess("saveTextFile(): failed to open '$file': $!") if (!ref($outfh));

  my $keys = $opts{keys} // 1;
  my $fh   = $pf->{fh};
  my ($i,$val);
  for ($i=0, $pf->reset(); !CORE::eof($fh); ++$i) {
    $val = $pf->get();
    $outfh->print(($keys ? "$i\t" : ''), $val, "\n");
  }

  CORE::close($outfh) if (!ref($file));
  return $pf;
}

## $bool = $pf->loadTextFile($filename_or_fh, %opts)
##  + load from text file with lines of the form "KEY? VALUE(s)..."
##  + %opts:
##      keys=>$bool,     ##-- expect keys in input? (default=true)
##      gaps=>$bool,     ##-- expect gaps or out-of-order elements in input? (default=false; implies keys=>1)
sub loadTextFile {
  my ($pf,$file,%opts) = @_;
  $pf->logconfess("loadTextFile(): no packed-file opened!") if (!$pf->opened);

  my $infh = ref($file) ? $file : IO::File->new("<$file");
  $pf->logconfess("loadTextFile(): failed to open '$file': $!") if (!ref($infh));  $pf->logconfess("loadTextFile(): failed to open '$file': $!") if (!ref($infh));

  $pf->truncate();
  my $gaps = $opts{gaps} // 0;
  my $keys = $gaps || ($opts{keys} // 1);
  my $fh   = $pf->{fh};
  my ($key,$val);
  if ($gaps) {
    ##-- load with keys, possibly out-of-order
    while (defined($_=<$infh>)) {
      chomp;
      next if (/^$/ || /^%%/);
      ($key,$val) = split(' ',$_,2);
      $pf->store($key,$val);
    }
  }
  else {
    ##-- load in serial order, with or without keys (ignored)
    $pf->reset;
    while (defined($_=<$infh>)) {
      chomp;
      next if (/^$/ || /^%%/);
      ($key,$val) = ($keys ? split(' ',$_,2) : (undef,$_));
      $pf->set($val);
    }
  }
  $pf->flush();

  CORE::close($infh) if (!ref($file));
  return $pf;
}

##==============================================================================
## API: tie interface

## $tied = >TIEARRAY($class, $file, $flags, %opts)
sub TIEARRAY {
  my ($that,$file,$flags,%opts) = @_;
  $flags //= 'r';
  return $that->new(%opts,file=>$file,flags=>$flags);
}

BEGIN {
  *FETCH = \&fetch;
  *STORE = \&store;
  *FETCHSIZE = \&size;
  *STORESIZE = \&setsize;
  *EXTEND    = \&setsize;
  *CLEAR     = \&truncate;
}

## $bool = $tied->EXISTS($index)
sub EXISTS {
  return ($_[1] < $_[0]->size);
}

## undef = $tied->DELETE($index)
sub DELETE {
  $_[0]->STORE($_[1], pack("C$_[0]{reclen}"));
}


##==============================================================================
## Footer
1;

__END__
