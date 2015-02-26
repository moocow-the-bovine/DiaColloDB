## -*- Mode: CPerl -*-
## File: DiaColloDB::MultiMapFile.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, integer->integer* multimap file, e.g. for expansion indices

package DiaColloDB::MultiMapFile;
use DiaColloDB::Logger;
use DiaColloDB::Persistent;
use DiaColloDB::Utils qw(:fcntl :json);
use Fcntl qw(:DEFAULT :seek);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Persistent);

##==============================================================================
## Constructors etc.

## $cldb = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    base => $base,       ##-- database basename; use files "${base}.ma", "${base}.mb", "${base}.hdr"
##    perms => $perms,     ##-- default: 0666 & ~umask
##    flags => $flags,     ##-- default: 'r'
##    pack_i => $pack_i,   ##-- integer pack template (default='N')
##    pack_o => $pack_o,   ##-- file offset pack template (default='N')
##    pack_l => $pack_l,   ##-- string-length pack template (default='n')
##    size => $size,       ##-- number of mapped , like scalar(@data)
##    ##
##    ##-- in-memory construction
##    a2b => \@a2b,        ##-- maps source integers to (packed) target integer-sets: [$a] => pack("${pack_i}*", @bs)
##    ##
##    ##-- pack lengths (after open())
##    len_i => $len_i,     ##-- bytes::length(pack($pack_i,0))
##    len_o => $len_o,     ##-- bytes::length(pack($pack_o,0))
##    len_l => $len_l,     ##-- bytes::length(pack($pack_l,0))
##    ##
##    ##-- filehandles (after open())
##    afh => $afh,         ##-- $base.ma : [$a]      => pack(${pack_o}, $boff_a)
##    bfh => $bfh,         ##-- $base.mb : $boff_a   :  pack("${pack_l}/(${pack_i}*)",  @targets_for_a)
##   )
sub new {
  my $that = shift;
  my $mmf  = bless({
		     base => undef,
		     perms => (0666 & ~umask),
		     flags => 'r',
		     size => 0,
		     pack_i => 'N',
		     pack_o => 'N',
		     pack_l => 'n',

		     a2b=>[],

		     #len_i => undef,
		     #len_o => undef,
		     #len_l => undef,
		     #len_a => undef,

		     #afh =>undef,
		     #bfh =>undef,

		     @_, ##-- user arguments
		    },
		    ref($that)||$that);
  $mmf->{class} = ref($mmf);
  $mmf->{a2b}    //= [];
  return defined($mmf->{base}) ? $mmf->open($mmf->{base}) : $mmf;
}

sub DESTROY {
  $_[0]->close() if ($_[0]->opened);
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: open/close (file)

## $mmf_or_undef = $mmf->open($base,$flags)
## $mmf_or_undef = $mmf->open($base)
## $mmf_or_undef = $mmf->open()
sub open {
  my ($mmf,$base,$flags) = @_;
  $base  //= $mmf->{base};
  $flags //= $mmf->{flags};
  $mmf->close() if ($mmf->opened);
  $mmf->{base}  = $base;
  $mmf->{flags} = $flags = fcflags($flags);
  if (fcread($flags) && !fctrunc($flags)) {
    $mmf->loadHeader()
      or $mmf->logconess("failed to load header from '$mmf->{base}.hdr': $!");
  }

  $mmf->{afh} = fcopen("$base.ma", $flags, $mmf->{perms})
    or $mmf->logconfess("open failed for $base.ma: $!");
  $mmf->{bfh} = fcopen("$base.mb", $flags, $mmf->{perms})
    or $mmf->logconfess("open failed for $base.mb: $!");

  ##-- pack lengths
  use bytes;
  $mmf->{len_i} = length(pack($mmf->{pack_i},0));
  $mmf->{len_o} = length(pack($mmf->{pack_o},0));
  $mmf->{len_l} = length(pack($mmf->{pack_l},0));
  $mmf->{len_a} = $mmf->{len_o} + $mmf->{len_l};

  return $mmf;
}

## $mmf_or_undef = $mmf->close()
sub close {
  my $mmf = shift;
  if ($mmf->opened && fcwrite($mmf->{flags})) {
    $mmf->flush() or return undef;
  }
  !defined($mmf->{afh}) or $mmf->{afh}->close() or return undef;
  !defined($mmf->{bfh}) or $mmf->{bfh}->close() or return undef;
  $mmf->{a2b} //= [],
  undef $mmf->{base};
  return $mmf;
}

## $bool = $mmf->opened()
sub opened {
  my $mmf = shift;
  return
    (
     #defined($mmf->{base}) &&
     defined($mmf->{afh})
     && defined($mmf->{bfh})
    );
}

## $bool = $mmf->dirty()
##  + returns true iff some in-memory structures haven't been flushed to disk
sub dirty {
  return @{$_[0]{a2b}};
}

## $bool = $mmf->flush()
##  + flush in-memory structures to disk
##  + clobbers any old disk-file contents with in-memory maps
##  + file must be opened in write-mode
##  + invalidates any old references to {a2b} (but doesn't empty them if you need to keep a reference)
sub flush {
  my $mmf = shift;
  return undef if (!$mmf->opened || !fcwrite($mmf->{flags}));
  return $mmf if (!$mmf->dirty);

  ##-- save header
  $mmf->saveHeader()
    or $mmf->logconfess("flush(): failed to store header $mmf->{base}.hdr: $!");

  use bytes;
  my ($afh,$bfh) = @$mmf{qw(afh bfh)};
  $afh->seek(0,SEEK_SET);
  $bfh->seek(0,SEEK_SET);

  ##-- dump $base.ma
  my ($a2b,$pack_o,$pack_l,$len_l,$pack_i,$len_i) = @$mmf{qw(a2b pack_o pack_l len_l pack_i len_i)};
  my $off   = 0;
  my $ai    = 0;
  my $bsz;
  foreach (@$a2b) {
    $_ //= '';
    $bsz = length($_);
    $afh->print(pack($pack_o, $off))
      or $mmf->logconfess("flush(): failed to write source record for a=$ai to $mmf->{base}.ma");
    $bfh->print(pack($pack_l, $bsz/$len_i), $_)
      or $mmf->logconfess("flush(): failed to write targets for a=$ai to $mmf->{base}.mb");
    $off += $len_l + $bsz;
    ++$ai;
  }
  CORE::truncate($afh, $afh->tell());
  CORE::truncate($bfh, $bfh->tell());

  ##-- clear in-memory structures (but don't clobber existing references)
  $mmf->{a2b} = [];

  return $mmf;
}


##--------------------------------------------------------------
## I/O: memory <-> file

## \@a2b = $mmf->toArray()
sub toArray {
  my $mmf = shift;
  return $mmf->{a2b} if (!$mmf->opened);
  use bytes;
  my ($pack_l,$len_l,$pack_i,$len_i) = @$mmf{qw(pack_l len_l pack_i len_i)};
  my $bfh    = $mmf->{bfh};
  my @a2b    = qw();
  my ($buf,$bsz);
  for (CORE::seek($bfh,0,SEEK_SET); !eof($bfh); ) {
    CORE::read($bfh, $buf, $len_l)==$len_l
	or $mmf->logconfess("toArray(): read() failed on $mmf->{base}.mb for target-set size at offset ", tell($bfh));
    $bsz = $len_i * unpack($pack_l, $buf);

    CORE::read($bfh, $buf, $bsz)==$bsz
	or $mmf->logconfess("toArray(): read() failed on $mmf->{base}.mb for target-set of $bsz bytes ", tell($bfh));
    push(@a2b, $buf);
  }
  push(@a2b, @{$mmf->{a2b}}[scalar(@a2b)..$#{$mmf->{a2b}}]) if ($mmf->dirty);
  return \@a2b;
}

## $mmf = $mmf->fromArray(\@a2b)
##  + clobbers $mmf contents, steals \@a2b
sub fromArray {
  my ($mmf,$a2b) = @_;
  $mmf->{a2b}  = $a2b;
  $mmf->{size} = scalar(@{$mmf->{a2b}});
  return $mmf;
}

## $bool = $mmf->load()
##  + loads files to memory; must be opened
sub load {
  my $mmf = shift;
  return $mmf->fromArray($mmf->toArray);
}

## $mmf = $mmf->save()
## $mmf = $mmf->save($base)
##  + saves multimap to $base; really just a wrapper for open() and flush()
sub save {
  my ($mmf,$base) = @_;
  $mmf->open($base,'rw') if (defined($base));
  $mmf->logconfess("save(): cannot save un-opened multimap") if (!$mmf->opened);
  $mmf->flush() or $mmf->logconfess("save(): failed to flush to $mmf->{base}: $!");
  return $mmf;
}


##--------------------------------------------------------------
## I/O: header
##  + see also DiaColloDB::Persistent

## @keys = $coldb->headerKeys()
##  + keys to save as header
sub headerKeys {
  return grep {!ref($_[0]{$_}) && $_ !~ m{^(?:flags|perms|base)$}} keys %{$_[0]};
}

## $bool = $CLASS_OR_OBJECT->loadHeader()
##  + wraps $CLASS_OR_OBJECT->loadHeaderFile($CLASS_OR_OBJ->headerFile())
##  + INHERITED from DiaColloDB::Persistent

## $bool = $mmf->loadHeaderData($hdr)
sub loadHeaderData {
  my ($mmf,$hdr) = @_;
  if (!defined($hdr) && (fcflags($mmf->{flags})&O_CREAT) != O_CREAT) {
    $mmf->logconfess("loadHeader() failed to load '$mmf->{base}.hdr': $!");
  }
  elsif (defined($hdr)) {
    return $mmf->SUPER::loadHeaderData($hdr);
  }
  return $mmf;
}

## $bool = $enum->saveHeader()
##  + inherited from DiaColloDB::Persistent

##--------------------------------------------------------------
## I/O: text

## $bool = $obj->loadTextFile($filename_or_handle, %opts)
##  + wraps loadTextFh()
##  + INHERITED from DiaColloDB::Persistent

## $mmf = $CLASS_OR_OBJECT->loadTextFh($filename_or_fh)
##  + loads from text file with lines of the form "ID SYMBOL..."
##  + clobbers multimap contents
sub loadTextFh {
  my ($mmf,$fh,%opts) = @_;
  $mmf = $mmf->new(%opts) if (!ref($mmf));

  my $pack_b = "($mmf->{pack_i})*";
  my @a2b  = qw();
  my ($a,@b);
  while (defined($_=<$fh>)) {
    chomp;
    next if (/^%%/ || /^$/);
    ($a,@b) = split(' ',$_);
    $a2b[$a] = pack($pack_b, @b);
  }

  ##-- clobber multimap
  return $mmf->fromArray(\@a2b);
}

## $bool = $obj->saveTextFile($filename_or_handle, %opts)
##  + wraps saveTextFh()
##  + INHERITED from DiaColloDB::Persistent

## $bool = $mmf->saveTextFh($filename_or_fh,%opts)
##  + save from text file with lines of the form "A B1 B2..."
##  + %opts:
##     b2s=>\&b2s  ##-- stringification code for B items, called as $s=$b2s->($bi)
##     a2s=>\&a2s  ##-- stringification code for B items, called as $s=$a2s->($bi)
sub saveTextFh {
  my ($mmf,$fh,%opts) = @_;

  my $a2s    = $opts{a2s};
  my $b2s    = $opts{b2s};
  my $pack_b = "($mmf->{pack_i})*";
  my $a2b    = $mmf->toArray;
  my $a      = 0;
  foreach (@$a2b) {
    if (defined($_)) {
      $fh->print(($a2s ? $a2s->($a) : $a),
		 "\t",
		 join(' ',
		      ($b2s
		       ? (map {$b2s->($_)} unpack($pack_b,$_))
		       : unpack($pack_b, $_))),
		 "\n");
    }
    ++$a;
  }

  return $mmf;
}


##==============================================================================
## Methods: population (in-memory only)

## $newsize = $mmf->addPairs($a,@bs)
## $newsize = $mmf->addPairs($a,\@bs)
##  + adds mappings $a=>$b foreach $b in @bs
##  + multimap must be loaded to memory
sub addPairs {
  my $mmf = shift;
  my $a   = shift;
  my $bs  = UNIVERSAL::isa($_[0],'ARRAY') ? $_[0] : \@_;
  $mmf->{a2b}[$a] .= pack("$mmf->{pack_i}*", @$bs);
  return $mmf->{size} = scalar(@{$mmf->{a2b}});
}

##==============================================================================
## Methods: lookup

## \@bs_or_undef = $mmf->fetchraw($a)
##  + returns array \@bs of targets for $a, or undef if not found
##  + multimap must be opened
sub fetch {
  my ($mmf,$a) = @_;

  my ($boff,$bsz,$buf);
  CORE::seek($mmf->{afh}, $a*$mmf->{len_o}, SEEK_SET)
      or $mmf->logconfess("fetch(): seek() failed on $mmf->{base}.ma for a=$a");
  CORE::read($mmf->{afh},$buf,$mmf->{len_o})==$mmf->{len_o}
      or $mmf->logconfess("fetch(): read() failed on $mmf->{base}.ma for a=$a");
  $boff = unpack($mmf->{pack_o},$buf);

  CORE::seek($mmf->{bfh}, $boff, SEEK_SET)
      or $mmf->logconfess("fetch(): seek() failed on $mmf->{base}.mb to offset $boff for a=$a");
  CORE::read($mmf->{bfh}, $buf,$mmf->{len_l})==$mmf->{len_l}
      or $mmf->logconfess("fetch(): read() failed on $mmf->{base}.mb for target-set length at offset $boff for a=$a");
  $bsz = $mmf->{len_i} * unpack($mmf->{pack_l},$buf);

  CORE::read($mmf->{bfh}, $buf, $bsz)==$bsz
      or $mmf->logconfess("fetch(): read() failed on $mmf->{base}.mb for target-set of size $bsz bytes at offset $boff for a=$a");

  return [unpack("($mmf->{pack_i})*", $buf)];
}

##==============================================================================
## Footer
1;

__END__




