## -*- Mode: CPerl -*-
## File: DiaColloDB::Cofreqs.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, co-frequency database (using pair of DiaColloDB::PackedFile)

package DiaColloDB::Cofreqs;
use DiaColloDB::Profile;
use DiaColloDB::PackedFile;
use DiaColloDB::Utils qw(:fcntl :env :run :json);
use Fcntl;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## Constructors etc.

## $cof = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##   ##-- user options
##   class    => $class,      ##-- optional, useful for debugging from header file
##   base     => $basename,   ##-- file basename (default=undef:none); use files "${base}.dba1", "${base}.dba2", "${base}.hdr"
##   flags    => $flags,      ##-- fcntl flags or open-mode (default='r')
##   perms    => $perms,      ##-- creation permissions (default=(0666 &~umask))
##   dmax     => $dmax,       ##-- maximum distance for co-occurrences (default=5)
##   fmin     => $fmin,       ##-- minimum pair frequency (default=0)
##   pack_i   => $pack_i,     ##-- pack-template for IDs (default='N')
##   pack_f   => $pack_f,     ##-- pack-template for IDs (default='N')
##   keeptmp  => $bool,       ##-- keep temporary files? (default=false)
##   ##
##   ##-- size info (after open() or load())
##   size1    => $size1,      ##-- == $r1->size()
##   size2    => $size2,      ##-- == $r2->size()
##   ##
##   ##-- low-level data
##   r1 => $r1,               ##-- pf: [$end2,$f1] @ $i1
##   r2 => $r2,               ##-- pf: [$i2,$f12]  @ end2($i1-1)..(end2($i1)-1)
##   N  => $N,                ##-- sum($f12)
##   )
sub new {
  my $that = shift;
  my $cof  = bless({
		    base  =>undef,
		    flags =>'r',
		    perms =>(0666 & ~umask),
		    dmax  =>5,
		    fmin  =>0,
		    pack_i=>'N',
		    pack_f=>'N',
		    r1 => DiaColloDB::PackedFile->new(),
		    r2 => DiaColloDB::PackedFile->new(),
		    N  => 0,
		    @_
		   }, (ref($that)||$that));
  $cof->{class} = ref($cof);
  return $cof->open() if (defined($cof->{base}));
  return $cof;
}

sub DESTROY {
  $_[0]->close() if ($_[0]->opened);
}

##==============================================================================
## I/O

##--------------------------------------------------------------
## I/O: open/close

## $cof_or_undef = $cof->open($base,$flags)
## $cof_or_undef = $cof->open($base)
## $cof_or_undef = $cof->open()
sub open {
  my ($cof,$base,$flags) = @_;
  $base  //= $cof->{base};
  $flags //= $cof->{flags};
  $cof->close() if ($cof->opened);
  $cof->{base}  = $base;
  $cof->{flags} = $flags = fcflags($flags);
  if (fcread($flags) && !fctrunc($flags)) {
    $cof->loadHeader()
      or $cof->logconess("failed to load header from '$cof->{base}.hdr': $!");
  }
  $cof->{r1}->open("$base.dba1", $flags, perms=>$cof->{perms}, packas=>"$cof->{pack_i}$cof->{pack_f}")
    or $cof->logconfess("open failed for $base.dba1: $!");
  $cof->{r2}->open("$base.dba2", $flags, perms=>$cof->{perms}, packas=>"$cof->{pack_i}$cof->{pack_f}")
    or $cof->logconfess("open failed for $base.dba2: $!");
  $cof->{size1} = $cof->{r1}->size;
  $cof->{size2} = $cof->{r2}->size;
  return $cof;
}

## $cof_or_undef = $cof->close()
sub close {
  my $cof = shift;
  if ($cof->opened && fcwrite($cof->{flags})) {
    $cof->saveHeader() or return undef;
  }
  $cof->{r1}->close() or return undef;
  $cof->{r2}->close() or return undef;
  undef $cof->{base};
  return $cof;
}

## $bool = $cof->opened()
sub opened {
  my $cof = shift;
  return
    (defined($cof->{base})
     && $cof->{r1}->opened
     && $cof->{r2}->opened);
}

##--------------------------------------------------------------
## I/O: header

## @keys = $cof->headerKeys()
##  + keys to save as header
sub headerKeys {
  return grep {!ref($_[0]{$_}) && $_ !~ m{^(?:base|flags|perms)$}} keys %{$_[0]};
}

## $bool = $cof->loadHeader()
sub loadHeader {
  my $cof = shift;
  my $hdr = loadJsonFile("$cof->{base}.hdr");
  if (!defined($hdr) && (fcflags($cof->{flags})&O_CREAT) != O_CREAT) {
    $cof->logconfess("loadHeader() failed to load '$cof->{base}.hdr': $!");
  }
  elsif (defined($hdr)) {
    @$cof{keys %$hdr} = values(%$hdr);
  }
  return $cof;
}

## $bool = $cof->saveHeader()
sub saveHeader {
  my $cof = shift;
  return undef if (!$cof->opened);
  my $hdr  = {map {($_=>$cof->{$_})} $cof->headerKeys()};
  saveJsonFile($hdr, "$cof->{base}.hdr")
    or $cof->logconfess("saveHeader() failed to save '$cof->{base}.hdr': $!");
  return $cof;
}

##--------------------------------------------------------------
## I/O: text

## $cof = $cof->loadTextFile($filename_or_fh,%opts)
##  + loads from text file as saved by saveTextFile()
##  + %opts: clobber %$cof
sub loadTextFile {
  my ($cof,$file,%opts) = @_;
  if (!ref($cof)) {
    $cof = $cof->new(%opts);
  } else {
    @$cof{keys %opts} = values %opts;
  }
  $cof->logconfess("loadTextFile(): cannot load unopened database!") if (!$cof->opened);

  my $infh = ref($file) ? $file : IO::File->new("<$file");
  $cof->logconfess("loadTextFile(): failed to open '$file': $!") if (!ref($infh));
  binmode($infh,':raw');

  ##-- common variables
  my $pack_f   = $cof->{pack_f};
  my $pack_i   = $cof->{pack_i};
  my $pack_r1  = "${pack_i}${pack_f}"; ##-- $r1 : [$end2,$f1] @ $i1
  my $pack_r2  = "${pack_i}${pack_f}"; ##-- $r2 : [$i2,$f12]  @ end2($i1-1)..(end2($i1)-1)
  my $fmin     = $cof->{fmin} // 0;
  my ($r1,$r2) = @$cof{qw(r1 r2)};
  $r1->truncate();
  $r2->truncate();
  my ($fh1,$fh2) = ($r1->{fh},$r2->{fh});

  ##-- iteration variables
  my ($pos1,$pos2,$pos2_prev) = (0,0,0);
  my ($i1_cur,$f1_cur) = (-1,0);
  my ($f12,$i1,$i2);
  my $N = 0;

  ##-- guts
  while (defined($_=<$infh>)) {
    ($f12,$i1,$i2) = split(' ',$_,3);
    next if ($f12 < $fmin);
    if ($i1 != $i1_cur) {
      if ($i1_cur != -1) {
	##-- dump record for $i1_cur
	if ($i1_cur != $pos1) {
	  ##-- we've skipped one or more $i1 because it had no collocates (e.g. kern01 i1=287123="Untier/1906")
	  $fh1->print( pack($pack_r1,$pos2_prev,0) x ($i1_cur-$pos1) );
	}
	$fh1->print(pack($pack_r1, $pos2,$f1_cur));
	$pos1      = $i1_cur+1;
	$pos2_prev = $pos2;
      }
      $i1_cur   = $i1;
      $f1_cur   = 0;
    }

    ##-- track marginal f($i1) and N
    $f1_cur += $f12;
    $N      += $f12;

    ##-- dump record to $r2
    $fh2->print(pack($pack_r2, $i2,$f12));
    ++$pos2;
  }

  ##-- dump final record for $i1_cur
  if ($i1_cur != -1) {
    if ($i1_cur != $pos1) {
      ##-- we've skipped one or more $i1 because it had no collocates (e.g. kern01 i1=287123="Untier/1906")
      $fh1->print( pack($pack_r1,$pos2_prev,0) x ($i1_cur-$pos1) );
    }
    $fh1->print(pack($pack_r1, $pos2,$f1_cur));
  }

  ##-- adopt final $N and sizes
  $cof->{N} = $N;
  $cof->{size1} = $r1->size;
  $cof->{size2} = $r2->size;

  ##-- cleanup
  CORE::close($infh) if (!ref($file));
  return $cof;
}

## $bool = $cof->saveTextFile($filename_or_fh,%opts)
##  + save from text file with lines of the form "FREQ ID1 ID2"
##  + %opts:
##      i2s => \&CODE,   ##-- code-ref for formatting indices; called as $s=CODE($i)
sub saveTextFile {
  my ($cof,$file,%opts) = @_;
  $cof->logconfess("saveTextFile(): cannot save unopened DB") if (!$cof->opened);

  my $outfh = ref($file) ? $file : IO::File->new(">$file");
  $cof->logconfess("saveTextFile(): failed to open output file '$file': $!") if (!ref($outfh));
  binmode($outfh,':raw');

  ##-- common variables
  my ($r1,$r2)   = @$cof{qw(r1 r2)};
  my $pack_r1    = $r1->{packas};
  my $pack_r2    = $r2->{packas};
  my $i2s        = $opts{i2s};
  my ($fh1,$fh2) = ($r1->{fh},$r2->{fh});

  ##-- iteration variables
  my ($buf1,$i1,$f1,$end2);
  my ($buf2,$off2,$i2,$f12);

  ##-- ye olde loope
  for ($r1->seek($i1=0), $r2->seek($off2=0); !$r1->eof(); ++$i1) {
    $r1->read(\$buf1) or $cof->logconfess("saveTextFile(): failed to read record $i1 from $r1->{file}: $!");
    ($end2,$f1) = unpack($pack_r1,$buf1);

    for ( ; $off2 < $end2 && !$r2->eof(); ++$off2) {
      $r2->read(\$buf2) or $cof->logconfess("saveTextFile(): failed to read record $off2 from $r2->{file}: $!");
      ($i2,$f12) = unpack($pack_r2,$buf2);
      $outfh->print($f12, "\t",
		    ($i2s
		     ? ($i2s->($i1), "\t", $i2s->($i2))
		     : ($i1, "\t", $i2)),
		    "\n");
    }
  }

  ##-- cleanup
  $outfh->close() if (!ref($file));
  return $cof;
}

##==============================================================================
## Relation API: create

## $bool = CLASS_OR_OBJECT->create($tokdat_file,%opts)
##  + populates current database from $tokdat_file,
##    a tt-style text file containing 1 token-id perl line with optional blank lines
##  + %opts: clobber %$ug, also:
##    (
##     size=>$size,  ##-- set initial size (number of types)
##    )
sub create {
  my ($cof,$tokfile,%opts) = @_;

  ##-- create/clobber
  $cof = $cof->new() if (!ref($cof));
  @$cof{keys %opts} = values %opts;

  ##-- ensure openend
  $cof->opened
    or $cof->open(undef,'rw')
      or $cof->logconfess("create(): failed to open co-frequency database '", ($cof->{base}//'-undef-'), "': $!");

  ##-- token reader fh
  CORE::open(my $tokfh, "<$tokfile")
    or $cof->logconfess("create(): open failed for token-file '$tokfile': $!");
  binmode($tokfh,':raw');

  ##-- sort filter
  env_push(LC_ALL=>'C');
  my $tmpfile = "$cof->{base}.dat";
  my $sortfh = opencmd("| sort -n -k1 -k2 | uniq -c - $tmpfile")
    or $cof->logconfess("create(): open failed for pipe to sort|uniq: $!");
  binmode($sortfh,':raw');

  ##-- stage1: generate pairs
  my $n = $cof->{dmax} // 1;
  $cof->vlog('trace', "create(): stage1: generate pairs (dmax=$n)");
  my (@sent,$i,$j,$wi,$wj);
  while (!eof($tokfh)) {
    @sent = qw();
    while (defined($_=<$tokfh>)) {
      chomp;
      last if (/^$/ );
      push(@sent,$_);
    }
    next if (!@sent);

    ##-- get pairs
    foreach $i (0..$#sent) {
      $wi = $sent[$i];
      print $sortfh
	(map {"$wi\t$sent[$_]\n"}
	 grep {$_>=0 && $_<=$#sent && $_ != $i}
	 (($i-$n)..($i+$n))
	);
    }
  }
  $sortfh->close()
    or $cof->logconfess("create(): failed to close pipe to sort|uniq: $!");
  env_pop();

  ##-- stage2: load pair-frequencies
  $cof->vlog('trace', "create(): stage2: load pair frequencies (fmin=$cof->{fmin})");
  $cof->loadTextFile($tmpfile)
    or $cof->logconfess("create(): failed to load pair frequencies from $tmpfile: $!");

  ##-- stage3: header
  $cof->saveHeader()
    or $cof->logconfess("create(): failed to save header: $!");

  ##-- unlink temp file
  unlink($tmpfile) if (!$cof->{keeptmp});

  ##-- done
  return $cof;
}

##==============================================================================
## Relation API: lookup

## $f = $cof->f1( @ids)
## $f = $cof->f1(\@ids)
##  + get total marginal unigram frequency (db must be opened)
sub f1 {
  my $cof = shift;
  my $ids = UNIVERSAL::isa($_[0],'ARRAY') ? @{$_[0]} : \@_;
  my $r1  = $cof->{r1};
  my $f   = 0;
  foreach (@$ids) {
    $f += $r1->fetch($_)->[1];
  }
  return $f;
}

## $f12 = $cof->f12($id1,$id2)
##  + return joint frequency for pair ($id1,$id2)
##  + UNUSED
sub f12 {
  my ($cof,$i1,$i2) = @_;
  my $beg2 = ($i1==0 ? 0 : $cof->{r1}->fetch($i1-1)->[0]);
  my $end2 = $cof->{r1}->fetch($i1)->[0];
  my $pos2 = $cof->{r2}->bsearch($i2, lo=>$beg2, hi=>$end2, packas=>$cof->{pack_i});
  return defined($pos2) ? $cof->{r2}->fetch($pos2)->[1] : 0;
}

##==============================================================================
## Relation API: profilung

## $prf = $cof->profile(\@xids, %opts)
##  + get co-frequency profile for @xids (db must be opened)
##  + %opts:
##     groupby => \&gbsub,  ##-- key-extractor $key2 = $gbsub->($i2)
sub profile {
  use bytes;
  my ($cof,$ids,%opts) = @_;
  $ids   = [$ids] if (!UNIVERSAL::isa($ids,'ARRAY'));
  my $r1 = $cof->{r1};
  my $r2 = $cof->{r2};
  my $pack1 = $r1->{packas};
  my $pack2 = $r2->{packas};
  my $pack1i = $cof->{pack_i};
  my $pack1f = "@".length(pack($cof->{pack_i},0)).$cof->{pack_f};
  my $size1  = $cof->{size1} // ($cof->{size1}=$r1->size);
  my $size2  = $cof->{size2} // ($cof->{size2}=$r2->size);
  my $groupby = $opts{groupby};
  my $pf1 = 0;
  my $pf2 = {};
  my $pf12 = {};
  my ($i1,$i2,$key2, $beg2,$end2,$pos2, $f1,$f12, $buf);

  foreach $i1 (@$ids) {
    next if ($i1 >= $size1);
    $beg2       = ($i1==0 ? 0 : unpack($pack1i,$r1->fetchraw($i1-1,\$buf)));
    ($end2,$f1) = unpack($pack1, $r1->fetchraw($i1,\$buf));

    $pf1       += $f1;
    next if ($beg2 >= $size2);
    for ($r2->seek($beg2), $pos2=$beg2; $pos2 < $end2; ++$pos2) {
      $r2->getraw(\$buf) or last;
      ($i2,$f12)    = unpack($pack2, $buf);
      $key2         = $groupby ? $groupby->($i2) : $i2;
      $pf2->{$key2}  += unpack($pack1f, $r1->fetchraw($i2,\$buf));
      $pf12->{$key2} += $f12;
    }
  }
  return DiaColloDB::Profile->new(
				N=>$cof->{N},
				f1=>$pf1,
				f2=>$pf2,
				f12=>$pf12,
			       );
}


##==============================================================================
## Footer
1;

__END__
