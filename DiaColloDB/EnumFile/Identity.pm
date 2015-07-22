## -*- Mode: CPerl -*-
## File: DiaColloDB::EnumFile::Identity.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, (integer<->integer) identity enum

package DiaColloDB::EnumFile::Identity;
use DiaColloDB::EnumFile;
use DiaColloDB::EnumFile::Tied;
use DiaColloDB::Logger;
use DiaColloDB::Utils qw(:fcntl :json :regex :pack);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::EnumFile);

##==============================================================================
## Constructors etc.

## $enum = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- EnumFile::Identity new
##    opened => $bool,     ##-- true iff object has been opened
##    ##-- EnumFile: basic options
##    base => $base,       ##-- database basename; use files "${base}.fsx", "${base}.fix", "${base}.hdr"
##    perms => $perms,     ##-- default: 0666 & ~umask
##    flags => $flags,     ##-- default: 'r'
##    #pack_i => $pack_i,   ##-- integer pack template (default='N')
##    ##pack_o => $pack_o,   ##-- file offset pack template (default='N') ; OVERRIDE:unused
##    ##pack_l => $pack_l,   ##-- string-length pack template (default='n'); OVERRIDE:unused
##    #pack_s => $pack_s,   ##-- string pack template for text i/o; OVERRIDE:REQUIRED (default='Nn')
##    size => $size,       ##-- number of mapped symbols, like scalar(@i2s)
##    #utf8 => $bool,       ##-- true iff strings are stored as utf8 (used by re2i()) OVERRIDE: unused
##    ##
##    ##-- EnumFile: in-memory construction
##    #s2i => \%s2i,        ##-- maps symbols to integers
##    #i2s => \@i2s,        ##-- maps integers to symbols
##    dirty => $bool,      ##-- true if in-memory structures are not in-sync with file data
##    loaded => $bool,     ##-- true if file data has been loaded to memory
##    shared => $bool,     ##-- true to avoid closing filehandles on close() or DESTROY() (default=false)
##    ##
##    ##-- EnumFile: pack lengths (after open())
##    #len_i => $len_i,     ##-- packsize($pack_i)
##    ##len_o => $len_o,     ##-- packsize($pack_o) ; OVERRIDE: unused
##    ##len_l => $len_l,     ##-- packsize($pack_l) ; OVERRIDE: unused
##    #len_s => $len_s,     ##-- packsize($pack_s); OVERRIDE: new
##    #len_sx => $len_sx,   ##-- $len_s + $len_i ; OVERRIDE: new value
##    ##
##    ##-- EnumFile: filehandles (after open())
##    ##sfh  => $sfh,        ##-- $base.es  : OVERRIDE: unused
##    #ixfh => $ixfh,       ##-- $base.fix : [$i] => pack("${pack_s}",          $s_with_id_i) : OVERRIDE: new extension, new format
##    #sxfh => $sxfh,       ##-- $base.fsx : [$j] => pack("${pack_s}${pack_i}", $s_with_sortorder_j_and_id_i, $i) : OVERRIDE: new extension, new format
##   )
sub new {
  my $that = shift;
  return $that->SUPER::new(
			   opened=>0,
			   @_, ##-- user arguments
			  )->_idclean;
}

## $enum = $enum->_idclean()
##  + clear non-idenum keys
sub _idclean {
  my $enum = shift;
  delete @$enum{grep {/^(?:utf8$|pack_|s2i$|i2s$|(?:i|s)x?fh$)/} keys %$enum};
  return $enum;
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: open/close (file)

## $enum_or_undef = $enum->open($base,$flags)
## $enum_or_undef = $enum->open($base)
## $enum_or_undef = $enum->open()
##  + opens file(s), clears {loaded} flag
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

  ##-- flags
  $enum->{loaded} = $enum->{opened} = 1;

  return $enum->_idclean();
}

## $enum_or_undef = $enum->close()
sub close {
  my $enum = shift;
  $enum->SUPER::close(@_) or return undef;
  $enum->{opened} = $enum->{size} = 0;
  return $enum->_idclean()
}

## $bool = $enum->opened()
sub opened {
  return $_[0]{opened};
}

## $bool = $enum->dirty()
##  + returns true iff some in-memory structures haven't been flushed to disk
##  + INHERITED

## $bool = $enum->loaded()
##  + returns true iff in-memory structures have been populated from disk
##  + INHERITED

## $bool = $enum->flush()
## $bool = $enum->flush($force)
##  + flush in-memory structures to disk
##  + no-op unless $force or $enum->dirty() is true
##  + clobbers any old disk-file contents with in-memory maps
##  + enum must be opened in write-mode
##  + invalidates any old references to {s2i}, {i2s} (but doesn't empty them if you need to keep a reference)
##  + clears {dirty} flag
sub flush {
  my ($enum,$force) = @_;
  return undef if (!$enum->opened || !fcwrite($enum->{flags}));
  return $enum if (!$force && !$enum->dirty);

  ##-- save header
  $enum->saveHeader()
    or $enum->logconfess("flush(): failed to store header $enum->{base}.hdr: $!");

  ##-- clear in-memory structures (but don't clobber existing references; used for xenum by DiaColloDB::create())
  $enum->{dirty} = 0;
  return $enum->_idclean();
}


##--------------------------------------------------------------
## I/O: memory <-> file

## \@i2s = $enum->toArray()
sub toArray {
  return [0..($_[0]{size}-1)];
}

## $enum = $enum->fromArray(\@i2s)
##  + clobbers $enum contents, implementation does NOT steal \@i2s
sub fromArray {
  my ($enum,$i2s) = @_;
  $enum->{size} = scalar(@$i2s);
  return $enum;
}

## $enum = $enum->fromHash(\%s2i)
##  + clobbers $enum contents, implementation does NOT steal \%s2i
sub fromHash {
  my ($enum,$s2i) = @_;
  my $max = -1;
  foreach (values %$s2i) {
    $max = $_ if (defined($_) && $_ > $max);
  }
  $enum->{size} = $max+1;
  return $enum;
}

## $enum = $enum->fromEnum($enum2)
##  + clobbers $enum contents, does NOT steal $enum2->{i2s}
##  + just copies $enum2->{size}
sub fromEnum {
  my ($e,$e2) = @_;
  $e->{size} = $e2->size();
  return $e;
}

## $bool = $enum->load()
##  + loads files to memory; must be opened
##  + no-op here
sub load {
  $_[0]{loaded} = 1;
  return $_[0];
}

## $enum = $enum->save()
## $enum = $enum->save($base)
##  + saves enum to $base; really just a wrapper for open() and flush()
##  + INHERITED

##--------------------------------------------------------------
## I/O: header

## @keys = $enum->headerKeys()
##  + keys to save as header
sub headerKeys {
  my $enum = shift;
  return grep {!m{^opened$}} $enum->SUPER::headerKeys();
}

## $bool = $enum->loadHeader()
##  + INHERITED

## $bool = $enum->saveHeader()
##  + INHERITED

##--------------------------------------------------------------
## I/O: text
##  + INHERITED (calls fromArray(), toArray())


##==============================================================================
## Methods: population (in-memory only)

## $size = $enum->size()
##  + wraps {size} key
##  + INHERITED

## $newsize = $enum->setsize($newsize)
##  + realy just wraps {size} key
sub setsize {
  $_[0]{dirty}=1;
  return $_[0]{size}=$_[1];
}

## $newsize = $enum->addSymbols(@symbols)
## $newsize = $enum->addSymbols(\@symbols)
##  + adds all symbols in @symbols which don't already exist
##  + enum must be loaded to memory
##  + override assumes @symbols are integers
sub addSymbols {
  my $enum    = shift;
  my $symbols = UNIVERSAL::isa($_[0],'ARRAY') ? $_[0] : \@_;
  my $max     = $enum->{size}-1;
  foreach (@$symbols) {
    $max = $_ if (defined($_) && $_ > $max);
  }
  return $enum->{size} = $max+1;
}


##==============================================================================
## Methods: lookup

## $s_or_undef = $enum->i2s($i)
##   + in-memory cache overrides file contents
sub i2s {
  return $_[1] < $_[0]{size} ? $_[1] : undef;
}

## $i_or_undef = $enum->s2i($s)
## $i_or_undef = $enum->s2i($s, $ilo,$ihi)
##   + binary search; in-memory cache overrides file contents
sub s2i {
  return $_[1] < $_[0]{size} ? $_[1] : undef;
}


## \@is = $enum->re2i($regex, $pack_s)
##  + gets indices for all (packed) strings matching $regex
##  + if $pack_s is specified, is will be used to unpack strings (default=$enum->{pack_s}), only the first unpacked element will be tested
##  + not supported
sub re2i {
  #use bytes; ##-- deprecated in perl v5.18.2
  $_[0]->logconfess("re2i(): not supported");
}

##==============================================================================
## Tied Interface: Global Wrappers
##  + see also DiaColloDB::EnumFile::Tied

## $class = $CLASS_OR_OBJECT->tieArrayClass()
##  + returns class for tied arrays to be returned by tiepair() method
##  + override returns "DiaColloDB::EnumFile::Identity::TiedArray"
sub tieArrayClass {
  return "DiaColloDB::EnumFile::Identity::TiedArray";
}

## $class = $CLASS_OR_OBJECT->tieHashClass()
##  + returns class for tied arrays to be returned by tiepair() method
##  + override returns "DiaColloDB::EnumFile::Identity::TiedHash"
sub tieHashClass {
  return "DiaColloDB::EnumFile::Identity::TiedHash";
}

##==============================================================================
## API: TiedArray

package DiaColloDB::EnumFile::Identity::TiedArray;
use DiaColloDB::EnumFile::Tied;
use Carp;
use strict;
our @ISA = qw(DiaColloDB::EnumFile::TiedArray);

##--------------------------------------------------------------
## API: TiedArray: mandatory methods: overrides

## $val = $tied->FETCH($index)
sub FETCH {
  return $_[1] < ${$_[0]}->{size} ? $_[1] : undef;
}

## $val = $tied->STORE($index,$val)
sub STORE {
  ${$_[0]}->setsize($_[1]+1) if ($_[1] >= ${$_[0]}->{size});
}

## undef = $tied->DELETE($index)
##  + not properly supported; no-op
sub DELETE {
  ;
}

##--------------------------------------------------------------
## API: TiedArray: optional methods

## undef = $tied->CLEAR()
sub CLEAR {
  ${$_[0]}->setsize(0);
}

## $newsize = $tied->PUSH(@list)
sub PUSH {
  ${$_[0]}->setsize(${$_[0]}->{size}+$#_);
}

## $elt = $tied->POP()
sub POP {
  return ${$_[0]}->setsize(--${$_[0]}->{size}) if (${$_[0]}->{size} > 0);
  return undef;
}

## $newsize = $tied->UNSHIFT(LIST)
BEGIN { *UNSHIFT = \&PUSH; }


##==============================================================================
## API: TiedHash

package DiaColloDB::EnumFile::Identity::TiedHash;
use DiaColloDB::EnumFile::Tied;
use Carp;
use strict;
our @ISA = qw(DiaColloDB::EnumFile::TiedHash);

##--------------------------------------------------------------
## API: TiedArray: overrides

## $val = $tied->STORE($key, $value)
sub STORE {
  ${$_[0]}->{dirty} = 1;
  ${$_[0]}->setsize($_[2]+1) if ($_[2] >= ${$_[0]}->size);
  return $_[2];
}

## $val = $tied->FETCH($key)
sub FETCH {
  return $_[1] < ${$_[0]}->{size} ? $_[1] : undef;
}

## $key = $tied->FIRSTKEY()
sub FIRSTKEY {
  return ${$_[0]}->{size}>0 ? 0 : undef;
}

## $key = $tied->NEXTKEY($lastkey)
sub NEXTKEY {
  return ($_[1]+1) < ${$_[0]}->{size} ? ($_[1]+1) : undef;
}

## $bool = $tied->EXISTS($key)
sub EXISTS {
  return $_[1] <= ${$_[0]}->{size};
}

## undef = $tied->DELETE($key)
##  + not properly supported;
sub DELETE {
  ;
}

## undef = $tied->CLEAR()
sub CLEAR {
  ${$_[0]}->setsize(0);
}

##==============================================================================
## Footer
1;

__END__




