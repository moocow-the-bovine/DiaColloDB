## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model (native)

package DiaColloDB::Relation::Vsem;
use DiaColloDB::Relation;
use DiaColloDB::Relation::Vsem::Query;
use DiaColloDB::Profile::Pdl;
use DiaColloDB::Profile::PdlDiff;
use DiaColloDB::Utils qw(:pack :fcntl :file :math :json :list :pdl :temp :env :run);
use DiaColloDB::PackedFile;
use DiaColloDB::Temp::Hash;
#use DiaColloDB::Temp::Array;
use DiaColloDB::PDL::MM;
use DiaColloDB::PDL::Utils;
use File::Path qw(make_path remove_tree);
use PDL;
use PDL::IO::FastRaw;
use PDL::CCS;
use PDL::CCS::IO::FastRaw;
use Fcntl qw(:DEFAULT SEEK_SET SEEK_CUR SEEK_END);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Relation);
BEGIN {
  no warnings 'once';
  $PDL::BIGPDL = 1; ##-- avoid 'Probably false alloc of over 1Gb PDL' errors
}

##==============================================================================
## Constructors etc.

## $vs = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##   ##-- user options
##   base   => $basename,   ##-- relation basename
##   flags  => $flags,      ##-- i/o flags (default: 'r')
##   #dcopts => \%dcopts,    ##-- options for DocClassify::Mapper->new()
##   #dcio   => \%dcio,      ##-- options for DocClassify::Mapper->saveDir()
##   mgood  => $regex,      ##-- positive filter regex for metadata attributes
##   mbad   => $regex,      ##-- negative filter regex for metadata attributes
##   submax => $submax,     ##-- choke on requested tdm cross-subsets if dense subset size ($NT_sub * $ND_sub) > $submax; default=2**29 (512M)
##   ##
##   ##-- logging options
##   logvprofile => $level, ##-- log-level for vprofile() (default=undef:none)
##   logvpslice => $level,  ##-- log-level for vpslice() (default=undef:none)
##   logio => $level,       ##-- log-level for low-level I/O operations (default=undef:none)
##   ##
##   ##-- modelling options (formerly via DocClassify)
##   minFreq    => $fmin,   ##-- minimum total term-frequency for model inclusion (default=undef:use $coldb->{tfmin})
##   minDocFreq => $dfmin,  ##-- minimim "doc-frequency" (#/docs per term) for model inclusion (default=4)
##   minDocSize => $dnmin,  ##-- minimum doc size (#/tokens per doc) for model inclusion (default=4; formerly $coldb->{vbnmin})
##   maxDocSize => $dnmax,  ##-- maximum doc size (#/tokens per doc) for model inclusion (default=inf; formerly $coldb->{vbnmax})
##   smoothf    => $f0,     ##-- smoothing constant to avoid log(0); default=1
##   vtype      => $vtype,  ##-- PDL::Type for storing compiled values (default=float)
##   itype      => $itype,  ##-- PDL::Type for storing compiled integers (default=long)
##   ##
##   ##-- guts: aux: info
##   N => $tdm0Total,       ##-- total number of (doc,term) frequencies counted
##   ##
##   ##-- guts: aux: term-tuples ($NA:number of term-attributes, $NT:number of term-tuples)
##   attrs  => \@attrs,       ##-- known term attributes
##   tvals  => $tvals,        ##-- pdl($NA,$NT) : [$apos,$ti] => $avali_at_term_ti
##   tsorti => $tsorti,       ##-- pdl($NT,$NA) : [,($apos)]  => $tvals->slice("($apos),")->qsorti
##   tpos   => \%a2pos,       ##-- term-attribute positions: $apos=$a2pos{$aname}
##   ##
##   ##-- guts: aux: metadata ($NM:number of metas-attributes, $NC:number of cats (source files))
##   meta => \@mattrs         ##-- known metadata attributes
##   meta_e_${ATTR} => $enum, ##-- metadata-attribute enum
##   mvals => $mvals,         ##-- pdl($NM,$NC) : [$mpos,$ci] => $mvali_at_ci
##   msorti => $msorti,       ##-- pdl($NC,$NM) : [,($mpos)]  => $mvals->slice("($mpos),")->qsorti
##   mpos  => \%m2pos,        ##-- meta-attribute positions: $mpos=$m2pos{$mattr}
##   ##
##   ##-- guts: model (formerly via DocClassify dcmap=>$dcmap)
##   tdm => $tdm,             ##-- term-doc matrix: PDL::CCS::Nd ($NT,$ND): [$ti,$di] -> log(f($ti,$di)+$f0)*w($ti)
##   tf  => $tf_pdl,          ##-- term-freq pdl:   dense:       ($NT)    : [$ti]     -> f($ti)
##   tw  => $tw_pdl,          ##-- term-weight pdl: dense:       ($NT)    : [$ti]     -> ($wRaw=0) + ($wCooked=1)*w($ti)
##                            ##   + where w($t) = 1 - H(Doc|T=$t) / H_max(Doc) ~ DocClassify termWeight=>'max-entropy-quotient'
##   c2date => $c2date,       ##-- cat-dates   : dense ($NC)   : [$ci]   -> $date
##   c2d    => $c2d,          ##-- cat->doc map: dense (2,$NC) : [*,$ci] -> [$di_off,$di_len]
##   d2c    => $d2c,          ##-- doc->cat map: dense ($ND)   : [$di]   -> $ci
##   #...
##   )
sub new {
  my $that = shift;
  my $vs   = $that->SUPER::new(
			       flags => 'r',
			       mgood => $DiaColloDB::VSMGOOD_DEFAULT,
			       mbad  => $DiaColloDB::VSMBAD_DEFAULT,
			       submax => 2**29,
			       minFreq => undef,
			       minDocFreq => 4,
			       minDocSize => 4,
			       maxDocSize => 'inf',
			       smoothf => 1,
			       vtype => 'float',
			       itype => 'long',
			       meta  => [],
			       attrs => [],
			       ##
			       logvprofile  => 'trace',
			       logvpslice => 'trace',
			       logio => 'trace',
			       ##
			       @_
			      );
  return $vs->open() if ($vs->{base});
  return $vs;
}

##==============================================================================
## Vsem API: Utils

## $vtype = $vs->vtype()
##  + get PDL::Type for storing compiled values
sub vtype {
  return $_[0]{vtype} if (UNIVERSAL::isa($_[0]{vtype},'PDL::Type'));
  return $_[0]{vtype} = (PDL->can($_[0]{vtype}//'double') // PDL->can('double'))->();
}

## $itype = $vs->itype()
##  + get PDL::Type for storing indices
sub itype {
  return $_[0]{itype} if (UNIVERSAL::isa($_[0]{vtype},'PDL::Type'));
  foreach ($_[0]{itype}, 'indx', 'long') {
    return $_[0]{itype} = PDL->can($_)->() if (defined($_) && PDL->can($_));
  }
}

##==============================================================================
## Persistent API: disk usage

## @files = $obj->diskFiles()
##  + returns disk storage files, used by du() and timestamp()
sub diskFiles {
  return ("$_[0]{base}.hdr", "$_[0]{base}.d");
}

##==============================================================================
## Persistent API: header

## @keys = $obj->headerKeys()
##  + keys to save as header; default implementation returns all keys of all non-references
sub headerKeys {
  my $obj = shift;
  return (qw(meta attrs vtype itype), grep {$_ !~ m/(?:flags|perms|base|log)/} $obj->SUPER::headerKeys);
}

## $hdr = $obj->headerData()
##  + returns reference to object header data; default returns anonymous HASH-ref for $obj->headerKeys()
##  + override stringifies {vtype}, {itype}
sub headerData {
  my $obj = shift;
  my $hdr = $obj->SUPER::headerData(@_);
  $hdr->{vtype} = "$hdr->{vtype}" if (ref($hdr->{vtype}));
  $hdr->{itype} = "$hdr->{itype}" if (ref($hdr->{itype}));
  return $hdr;
}


##==============================================================================
## Relation API: open/close

## $vs_or_undef = $vs->open($base)
## $vs_or_undef = $vs->open($base,$flags)
## $vs_or_undef = $vs->open()
sub open {
  my ($vs,$base,$flags) = @_;
  $base  //= $vs->{base};
  $flags //= $vs->{flags};
  $vs->close() if ($vs->opened);
  $vs->{base}  = $base;
  $vs->{flags} = $flags = fcflags($flags);

  if (fcread($flags) && !fctrunc($flags)) {
    $vs->loadHeader()
      or $vs->logconess("failed to load header from '$vs->{base}.hdr': $!");
  }

  ##-- open maybe create directory
  my $vsdir = "$vs->{base}.d";
  if (!-d $vsdir) {
    $vs->logconfess("open(): no such directory '$vsdir'") if (!fccreat($flags));
    make_path($vsdir)
      or $vs->logconfess("open(): could not create relation directory '$vsdir': $!");
  }

  ##-- load: model data
  my %ioopts = (ReadOnly=>!fcwrite($flags), mmap=>1, log=>$vs->{logIO});
  defined($vs->{tdm} = readPdlFile("$vsdir/tdm", class=>'PDL::CCS::Nd', %ioopts))
    or $vs->logconfess("open(): failed to load term-document matrix from $vsdir/tdm.*: $!");
  defined($vs->{tf}  = readPdlFile("$vsdir/tf.pdl", %ioopts))
    or $vs->logconfess("open(): failed to load term-frequencies from $vsdir/tf.pdl: $!");
  defined($vs->{tw}  = readPdlFile("$vsdir/tw.pdl", %ioopts))
    or $vs->logconfess("open(): failed to load term-weights from $vsdir/tw.pdl: $!");

  defined(my $ptr0 = $vs->{ptr0} = readPdlFile("$vsdir/tdm.ptr0.pdl", %ioopts))
    or $vs->logwarn("open(): failed to load Harwell-Boeing pointer from $vsdir/tdm.ptr0.pdl: $!");
  defined(my $ptr1 = $vs->{ptr1} = readPdlFile("$vsdir/tdm.ptr1.pdl", %ioopts))
    or $vs->logwarn("open(): failed to load Harwell-Boeing pointer from $vsdir/tdm.ptr1.pdl: $!");
  defined(my $pix1 = $vs->{pix1} = readPdlFile("$vsdir/tdm.pix1.pdl", %ioopts))
    or $vs->logwarn("open(): failed to load Harwell-Boeing indices from $vsdir/tdm.pix1.pdl: $!");
  defined($vs->{vnorm0} = readPdlFile("$vsdir/tdm.vnorm0.pdl", %ioopts))
    or $vs->logwarn("open(): failed to load term-vector magnitudes from $vsdir/tdm.vnorm0.pdl: $!");
  $vs->{tdm}->setptr(0, $ptr0)        if (defined($ptr0));
  $vs->{tdm}->setptr(1, $ptr1,$pix1)  if (defined($ptr1) && defined($pix1));

  ##-- load: aux data: piddles
  foreach (qw(tvals tsorti mvals msorti d2c c2d c2date)) {
    defined($vs->{$_}=readPdlFile("$vsdir/$_.pdl", %ioopts))
      or $vs->logconfess("open(): failed to load piddle data from $vsdir/$_.pdl: $!");
  }

  ##-- load: metadata: enums
  my %efopts = (flags=>$vs->{flags}); #, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len}
  foreach my $mattr (@{$vs->{meta}}) {
    $vs->{"meta_e_$mattr"} = $DiaColloDB::ECLASS->new(base=>"$vsdir/meta_e_$mattr", %efopts)
      or $vs->logconfess("open(): failed to open metadata enum $vsdir/meta_e_$mattr: $!");
  }

  return $vs;
}

## $vs_or_undef = $vs->close()
sub close {
  my $vs = shift;
  if ($vs->opened && fcwrite($vs->{flags})) {
    $vs->saveHeader() or return undef;
#   $vs->{dcmap}->saveDir("$vs->{base}_map.d", %{$vs->{dcio}//{}})
#     or $vs->logconfess("close(): failed to save mapper data to $vs->{base}_map.d: $!");
  }
  delete @$vs{qw(base N tdm tw attrs tvals tsorti tpos meta mvals msorti mpos tdm tw)};
  return $vs;
}

## $bool = $obj->opened()
sub opened {
  my $vs = shift;
  return UNIVERSAL::isa($vs->{tdm},'PDL::CCS::Nd');
}

##==============================================================================
## Relation API: create

## $vs = $CLASS_OR_OBJECT->create($coldb,$tokdat_file,%opts)
##  + populates current database for $coldb
##  + implementation used (temporary, tied) doc-arrays @$coldb{qw(docmeta docoff)}
##  + %opts: clobber %$vs, also:
##    (
##     size=>$size,  ##-- set initial size
##    )
sub create {
  my ($vs,$coldb,$datfile,%opts) = @_;

  ##-- create/clobber
  $vs = $vs->new() if (!ref($vs));
  $vs->{$_} = $coldb->{"vs$_"} foreach (grep {exists $coldb->{"vs$_"}} qw(mgood mbad));
  @$vs{keys %{$coldb->{vsopts}//{}}} = values %{$coldb->{vsopts}//{}};
  @$vs{keys %opts} = values %opts;

  ##-- sanity check(s)
  my $docmeta = $coldb->{docmeta};
  my $docoff  = $coldb->{docoff};
  my $base    = $vs->{base};
  my $logCreate = $vs->{logCreate} // $coldb->{logCreate} // 'trace';
  $vs->logconfess("create(): no source document array {docmeta} in parent DB") if (!UNIVERSAL::isa($coldb->{docmeta},'ARRAY'));
  $vs->logconfess("create(): no source document offsets {docoff} in parent DB") if (!UNIVERSAL::isa($coldb->{docoff},'ARRAY'));
  $vs->logconfess("create(): no 'base' key defined") if (!$base);

  ##-- open packed token-attribute file
  my $vtokfile = "$coldb->{dbdir}/vtokens.bin";
  CORE::open(my $vtokfh, "<:raw", $vtokfile)
      or $vs->logconfess("create(): could not open temporary token file $vtokfile: $!");

  ##-- initialize: output directory
  my $vsdir = "$base.d";
  $vsdir =~ s{/$}{};
  !-d $vsdir
    or remove_tree($vsdir)
      or $vs->logconfess("create(): could not remove stale $vsdir: $!");
  make_path($vsdir)
    or $vs->logconfess("create(): could not create Vsem directory $vsdir: $!");

  ##-- initialize: logging
  my $nfiles    = scalar(@$docmeta);
  my $logFileN  = $coldb->{logCorpusFileN} // max2(1,int($nfiles/10));

  ##-- initialize: metadata
  my %meta = qw(); ##-- ( $meta_attr => {n=>$nkeys, s2i=>\%s2i, vals=>$pdl}, ... )
  my $mgood = $vs->{mgood} ? qr{$vs->{mgood}} : undef;
  my $mbad  = $vs->{mbad}  ? qr{$vs->{mbad}}  : undef;

  ##-- create temp file: tdm0.dat (sorted via system sort command)
  my $NA      = scalar(@{$coldb->{attrs}});
  my $NC      = $nfiles;
  my $itype   = $vs->itype;
  my $vtype   = $vs->vtype;
  my $pack_w  = $coldb->{pack_w};
  my $len_w   = packsize($pack_w);
  my $pack_ix = $PDL::Types::pack[ $itype->enum ];
  (my $pack_ix1 = $pack_ix) =~ s/\*$//;
  my $len_ix  = packsize($pack_ix,0);
  my $pack_nz = $PDL::Types::pack[ $vtype->enum ];
  my $pack_date = $PDL::Types::pack[ ushort->enum ];
  my $len_date  = packsize($pack_date,0);
  my %tmpargs   = (UNLINK=>!$coldb->{keeptmp});
  my $tdm0file  = "$vsdir/tdm0.dat";   # txt ~ "$ai0 $ai1 ... $aiN $doci $f"
  my $tdm0fh    = opencmd("|-:raw", 'sort', (map {"-nk$_"} (1..($NA+1))), "-o", $tdm0file)
    or $vs->logconfess("create(): failed to create pipe to sort for tempfile $tdm0file: $!");

  ##-- create cat-wise piddle files c2date.pdl, c2d.pdl
  my $c2datefile = "$vsdir/c2date.pdl";					##-- c2date ($NC): [$ci]   -> $date
  CORE::open(my $c2datefh, ">:raw", $c2datefile)
    or $vs->logconfess("create(): failed to create piddle file $c2datefile: $!");
  writePdlHeader("$c2datefile.hdr", ushort, 1, $NC)
    or $vs->logconfess("create(): failed to write piddle header $c2datefile.hdr: $!");
  my $c2dfile = "$vsdir/c2d.pdl";						##-- c2d  (2,$NC): [0,$ci] => $di_off, [1,$ci] => $di_len
  CORE::open(my $c2dfh, ">:raw", $c2dfile)
      or $vs->logconfess("create(): failed to create piddle file $c2dfile: $!");
  writePdlHeader("$c2dfile.hdr", $itype, 2, 2,$NC)
    or $vs->logconfess("create(): failed to write piddle header $c2dfile.hdr: $!");

  ##-- create: simulate DocClassify::Mapper::trainCorpus(): populate tdm0.*
  $vs->vlog($logCreate, "create(): processing input documents [NA=$NA, NC=$nfiles]");
  my $json   = DiaColloDB::Utils->jsonxs();
  my $minDocSize = $vs->{minDocSize} = max2(($vs->{minDocSize}//0),1);
  my $maxDocSize = $vs->{maxDocSize} = min2(($vs->{maxDocSize}//'inf'),'inf');
  my ($doc,$filei,$doclabel,$docid);
  my ($mattr,$mval,$mdata,$mvali,$mvals);
  my ($ts,$ti,$f, $sigi_in,$sigj_in,$sigi_out0,$sigi_out, $toki,$tokj,%sig,$sign,$buf);
  my ($tmp);
  $sigi_in = $sigi_out = (0,0);
  foreach $doc (@$docmeta) {
    $doclabel = $doc->{file} // $doc->{meta}{basename} // $doc->{meta}{file_} // $doc->{label};
    $vs->vlog($coldb->{logCorpusFile}, sprintf("create(): processing signatures [%3.0f%%]: %s", 100*($filei-1)/$nfiles, $doclabel))
      if ($logFileN && ($filei++ % $logFileN)==0);

    $docid = $doc->{id} // ++$docid;

    #$vs->debug("c2date: id=$docid/$NC ; doc=$doclabel");
    $c2datefh->seek($docid*$len_date, SEEK_SET);
    $c2datefh->print(pack($pack_date, $doc->{date}));

    $c2dfh->seek($docid*$len_ix*2, SEEK_SET);
    $c2dfh->print(pack($pack_ix1, $sigi_out0=$sigi_out));

    ##-- parse metadata
    #$vs->debug("meta: id=$docid/$NC ; doc=$doclabel");
    while (($mattr,$mval) = each %{$doc->{meta}//{}}) {
      next if ((defined($mgood) && $mattr !~ $mgood) || (defined($mbad) && $mattr =~ $mbad));
      if (!defined($mdata=$meta{$mattr})) {
	#$mdata = $meta{$mattr} = {n=>1, s2i=>{''=>0}, vals=>mmtemp("$vsdir/mvals_$mattr.tmp",$itype,$NC)} ;
	$mdata = $meta{$mattr} = {
				  n=>1,
				  s2i=>tmphash("$vsdir/ms2i_${mattr}", utf8keys=>1, %tmpargs),
				  vals=>tmparrayp("$vsdir/mvals_$mattr", $pack_ix1, %tmpargs),
				 };
	$mdata->{s2i}{''} = 0;
      }
      $mvali = ($mdata->{s2i}{$mval} //= $mdata->{n}++);
      $mdata->{vals}[$docid] = $mvali;
    }

    ##-- parse document signatures into $ix0file, $nz0file, $t0file
    #$vs->debug("sigs: id=$docid/$NC ; doc=$doclabel");
    $sigj_in = $sigi_in + $doc->{nsigs};
    for ( ; $sigi_in < $sigj_in; ++$sigi_in) {
      $toki = $docoff->[$sigi_in];
      $tokj = $docoff->[$sigi_in+1];

      $vs->logconfess("create(): bad offset in $vtokfile") if ($vtokfh->tell != $toki*$len_w); ##-- DEBUG

      ##-- parse signature
      %sig  = qw();
      $sign = $tokj - $toki;
      for ( ; $toki < $tokj; ++$toki) {
	CORE::read($vtokfh, $buf, $len_w)
	    or $vs->logconfess("create(): read() failed on $vtokfile: $!");
	++$sig{$buf};
      }
      next if ($sign <= $minDocSize || $sign >= $maxDocSize);

      ##-- populate tdm0.dat
      while (($ts,$f) = each %sig) {
	$tdm0fh->print(join(' ', unpack($pack_w,$ts), $sigi_out, $f),"\n");
      }
      ++$sigi_out;
    }

    ##-- update c2d (length)
    $c2dfh->print(pack($pack_ix1, $sigi_out - $sigi_out0));
  }

  ##-- cleanup
  $c2dfh->close() or $vs->logconfess("create(): close failed for tempfile $c2dfile: $!");
  $c2datefh->close() or $vs->logconfess("create(): close failed for tempfile $c2datefile: $!");
  $tdm0fh->close() or $vs->logconfess("create(): close failed for tempfile $tdm0file: $!");
  tied(@{$_->{vals}})->flush() foreach (values %meta);

  ##-- create: filter: by term-frequency (default: use coldb term-filtering only)
  $vs->{minFreq} //= 0;
  my ($wbad);
  if ($vs->{minFreq} > 0) {
    my $fmin = $vs->{minFreq};
    $vs->vlog($logCreate, "create(): filter: by term-frequency (minFreq=$vs->{minFreq})");
    $wbad = tmphash("$vsdir/wbad", %tmpargs);
    CORE::open($tdm0fh, "<:raw", $tdm0file)
	or $vs->logconfess("create(): re-open failed for $tdm0file: $!");
    my ($w,$f);
    my ($wcur,$fcur) = ('INITIAL','inf');
    my $NT0 = 0;
    my $NT1 = 0;
    while (defined($_=<$tdm0fh>)) {
      ($w,$f) = /^(.*) [0-9]+ ([0-9]+)$/;
      if ($w eq $wcur) {
	$fcur += $f;
      } else {
	++$NT0;
	if ($fcur < $fmin) {
	  $wbad->{$wcur} = undef;
	} else {
	  ++$NT1;
	}
	($wcur,$fcur)  = ($w,$f);
      }
    }
    ++$NT0;
    if ($fcur < $fmin) {
      $wbad->{$wcur} = undef;
    } else {
      ++$NT1;
    }
    CORE::close($tdm0fh);

    my $nwbad = ($NT0-$NT1);
    my $pwbad = $NT0 ? sprintf("%.2f%%", 100*$nwbad/$NT0) : 'nan%';
    $vs->vlog($logCreate, "create(): filter: will prune $nwbad of $NT0 term tuple type(s) ($pwbad)");
  }

  ##-- create: filter: by doc-frequency
  $vs->{minDocFreq} //= 0;
  if ($vs->{minDocFreq} > 0) {
    $vs->vlog($logCreate, "create(): filter: by doc-frequency (minDocFreq=$vs->{minDocFreq})");
    env_push(LC_ALL=>'C');
    my $cmdfh = opencmd("cut -d\" \" -f-$NA $tdm0file | uniq -c |")
      or $vs->logconfess("create(): failed to open pipe from sort for doc-frequency filter");
    $wbad //= tmphash("$vsdir/wbad", %tmpargs);
    my $fmin = $vs->{minDocFreq};
    my $NT0  = 0;
    my $NT1  = 0;
    my ($f,$w);
    while (defined($_=<$cmdfh>)) {
      chomp;
      ($f,$w) = split(' ',$_,2);
      ++$NT0;
      if ($f < $fmin) {
	$wbad->{$w} = undef;
      } else {
	++$NT1;
      }
    }
    env_pop();
    CORE::close($cmdfh);

    my $nwbad = ($NT0-$NT1);
    my $pwbad = $NT0 ? sprintf("%.2f%%", 100*$nwbad/$NT0) : 'nan%';
    $vs->vlog($logCreate, "create(): filter: will prune $nwbad of $NT0 term tuple type(s) ($pwbad)");
  }

  ##-- create: term-enum $tvals
  $vs->vlog($logCreate, "create(): extracting term tuples");
  my $NT   = 0;
  my $NT0  = 0;
  my $ttxtfh = opencmd("cut -d\" \" -f-$NA $tdm0file | uniq |")
    or $vs->logconfess("creat(): open failed for pipe from sort for term-values: $!");
  my $tvalsfile = "$vsdir/tvals.pdl";
  CORE::open(my $tvalsfh, ">:raw", $tvalsfile)
    or $vs->logconfess("create(): open failed for term-values piddle $tvalsfile: $!");
  my $ts2i = tmphash("$vsdir/ts2i", %tmpargs);

  ##-- create: alwasy include "null" term
  {
    my @tnull = map {0} (1..$NA);
    $ts2i->{join(' ', @tnull)} = 0;
    $tvalsfh->print(pack($pack_ix, @tnull));
  }

  ##-- create: enumerate "normal" terms
  while (defined($_=<$ttxtfh>)) {
    chomp;
    ++$NT0;
    next if ($wbad && exists($wbad->{$_}));
    $tvalsfh->print(pack($pack_ix, split(' ',$_)));
    $ts2i->{$_} = ++$NT;
  }
  ++$NT;
  $tvalsfh->close()
    or $vs->logconfess("create(): failed to close term-values piddle file $tvalsfile: $!");
  $ttxtfh->close()
    or $vs->logconfess("create(): failed to close term-values sort pipe: $!");
  writePdlHeader("$tvalsfile.hdr", $itype, 2, $NA,$NT)
    or $vs->logconfess("create(): failed to write term-values header $tvalsfile.hdr: $!");
  defined(my $tvals = readPdlFile($tvalsfile))
    or $vs->logconfess("create(): failed to mmap term-values file $tvalsfile: $!");
  my $pprunet = $NT0 ? sprintf("%.2f%%", 100*($NT0-$NT)/$NT0) : 'nan%';
  $vs->vlog($logCreate, "create(): extracted $NT of $NT0 unique term tuples ($pprunet pruned)");

  ##-- create: tdm0: ccs
  my $ND  = $sigi_out;
  $vs->vlog($logCreate, "create(): creating raw term-document matrix $vsdir/tdm.* (NT=$NT, ND=$ND)");
  my $ixfile = "$vsdir/tdm.ix";
  CORE::open(my $ixfh, ">:raw", $ixfile)
      or $vs->logconfess("create(): open failed for tdm index file $ixfile: $!");
  my $nzfile = "$vsdir/tdm.nz";
  CORE::open(my $nzfh, ">:raw", $nzfile)
      or $vs->logconfess("create(): open failed for tdm value file $nzfile: $!");
  CORE::open($tdm0fh, "<:raw", $tdm0file)
      or $vs->logconfess("create(): re-open failed for tdm text file $tdm0file: $!");
  my ($di);
  my $nnz0 = 0;
  my $nnz  = 0;
  my ($w);
  while (defined($_=<$tdm0fh>)) {
    ++$nnz0;
    ($w,$di,$f) = m{^(.*) ([0-9]+) ([0-9]+)$};
    next if (!defined($ti=$ts2i->{$w}));
    ++$nnz;
    $ixfh->print(pack($pack_ix,$ti,$di));
    $nzfh->print(pack($pack_nz,$f));
  }
  $nzfh->print(pack($pack_nz,0)); ##-- include "missing" value
  CORE::close($nzfh)
      or $vs->logconfess("create(): close failed for tdm value file $nzfile: $!");
  CORE::close($ixfh)
      or $vs->logconfess("create(): close failed for tdm index file $ixfile: $!");
  CORE::close($tdm0fh);
  my $density  = sprintf("%.2g%%", $nnz / ($ND*$NT));
  my $pprunenz = $nnz0 ? sprintf("%.2f%%", 100*($nnz0-$nnz)/$nnz0) : 'nan%';
  $vs->vlog($logCreate, "create(): created raw term-document matrix (density=$density, $pprunenz pruned)");

  ##-- tdm0: read in as piddle
  writePdlHeader("$vsdir/tdm.ix.hdr", $itype, 2, 2,$nnz)
    or $vs->logconfess("create(): failed to save tdm index header $vsdir/tdm.ix.hdr: $!");
  writePdlHeader("$vsdir/tdm.nz.hdr", $vtype, 1, $nnz+1)
    or $vs->logconfess("create(): failed to save tdm value header $vsdir/tdm.nz.hdr: $!");
  writeCcsHeader("$vsdir/tdm.hdr", $itype,$vtype,[$NT,$ND])
    or $vs->logconfess("create(): failed to save CCS header $vsdir/tdm.hdr: $!");
  defined(my $tdm = readPdlFile("$vsdir/tdm", class=>'PDL::CCS::Nd'))
    or $vs->logconfess("create(): failed to map CCS term-document matrix from $vsdir/tdm.*");

  ##-- create: tw (term-weights): max-entropy-quotient: see e.g. Berry(1995); Nakov, Popova, & Mateev (2001)
  $vs->vlog($logCreate, "create(): computing term weights");
  my $ix = $tdm->_whichND;
  my $nz = $tdm->_vals;
  $nz->slice("0:-2")->indadd($ix->slice("(0),"),			##-- tf  :  pdl: [$ti] -> f($ti) : keep
			     my $tf=mmzeroes("$vsdir/tf.pdl", $vtype, $NT));
  $nz->slice("0:-2")->divide($tf->index($ix->slice("(0),")),		##-- twp :  pdl: $nzi~[$ti,$di] ->     p($di|$ti)
			     my $twp=mmtemp("$vsdir/twp.tmp", $vtype, $nnz), 0);
  $twp->log(my $twh=mmtemp("$vsdir/twh.tmp", $vtype, $nnz));		##-- twh :  pdl: $nzi~[$ti,$di] ->  ln p($di|$ti)
  $twh /= log(2);							##                              -> log p($di|$ti)
  $twh *= $twp;								##                              ->    -h($di|$ti) = p($di|$ti) * log p($di|$ti)
  undef $twp;
  $twh->indadd($ix->slice("(0),"),
	       my $tw=mmzeroes("$vsdir/tw.pdl", $vtype, $NT));		##-- tw  : pdl: [$ti] ->  -H(Doc|T=$ti) : keep
  undef $twh;
  $tw  /= log($ND)/log(2);						##                    ->  -H(DOc|T=$ti)/Hmax(Doc)
  $tw  += 1;								##                    -> 1-H(DOc|T=$ti)/Hmax(Doc)
  #$vs->{tw} = $tw;
  #$vs->{tf} = $tf;
  $vs->{N} = $tf->sum;

  ##-- tw: cleanup
  #undef $tw;
  undef $twh;
  undef $twp;
  undef $tf;

  ##-- create: construct final tfidf matrix
  my %ioopts = (mmap=>1, log=>$vs->{logIO});
  $vs->{smoothf} //= 1;
  $vs->vlog($logCreate, "create(): constructing final tfidf matrix (NT=$NT, ND=$ND, Nnz=$nnz; smoothf=$vs->{smoothf})");

  ##-- create: tfidf: setup $nz values: 		   $nzi~[$ti,$di] ->      f($ti,$di)
  $nz += $vs->{smoothf};						# ->      f($ti,$di)+$eps
  $nz->inplace->log;							# ->   ln(f($ti,$di)+$eps)
  $nz /= log(2);							# -> log2(f($ti,$di)+$eps)
  ($tmp=$nz->slice("0:-2")) *= $tw->index($ix->slice("(0),"));		# -> log2(f($ti,$di)+$eps) * tw($ti)
  $nz->set($nnz=>0) if (!$vs->{smoothf});				# : avoid missing() = -inf

  ##-- create: tfidf: cleanup
  undef $tw;
  undef $nz;
  undef $ix;

  ##-- create: tfidf: pointers & norms
  $vs->vlog($logCreate, "create(): creating tfidf matrix Harwell-Boeing pointers");
  my ($ptr0) = $tdm->getptr(0);
  $ptr0      = $ptr0->convert($itype) if ($ptr0->type != $itype);
  $ptr0->writefraw("$vsdir/tdm.ptr0.pdl")
    or $vs->logconfess("create(): failed to write $vsdir/tdm.ptr0.pdl: $!");
  undef $ptr0;

  my ($ptr1,$pix1) = $tdm->getptr(1);
  $ptr1 = $ptr1->convert($itype) if ($ptr1->type != $itype);
  $pix1 = $pix1->convert($itype) if ($pix1->type != $itype && pdl($itype,$pix1->nelem)->sclr >= 0); ##-- check for overflow
  $ptr1->writefraw("$vsdir/tdm.ptr1.pdl")
    or $vs->logconfess("create(): failed to write $vsdir/tdm.ptr1.pdl: $!");
  $pix1->writefraw("$vsdir/tdm.pix1.pdl")
    or $vs->logconfess("create(): failed to write $vsdir/tdm.pix1.pdl: $!");
  undef $ptr1;
  undef $pix1;

  my $vnorm0 = $tdm->vnorm(0);
  $vnorm0    = $vnorm0->convert($vtype) if ($vnorm0->type != $vtype);
  $vnorm0->writefraw("$vsdir/tdm.vnorm0.pdl")
    or $vs->confess("create(): failed to write $vsdir/tdm.vnorm0.pdl: $!");
  undef $vnorm0;
  undef $tdm;

  ##-- create: aux: tsorti
  $vs->vlog($logCreate, "create(): creating term-attribute sort-indices (NA=$NA x NT=$NT)");
  my $tsorti = mmzeroes("$vsdir/tsorti.pdl", $itype, $NT,$NA); ##-- [,($apos)] => $tvals->slice("($apos),")->qsorti
  foreach (0..($NA-1)) {
    $tvals->slice("($_),")->qsorti($tsorti->slice(",($_)"));
  }
  undef $tsorti;
  ##
  $vs->{attrs} = $coldb->{attrs}; ##-- save local copy of attributes

  ##-- create: aux: d2c: [$di] => $ci
  $vs->vlog($logCreate, "create(): creating doc<->category translation piddles (ND=$ND, NC=$NC)");
  defined(my $c2d = readPdlFile("$vsdir/c2d.pdl"))
    or $vs->logconfess("create(): failed to mmap $vsdir/c2d.pdl");
  $c2d->slice("(1),")->rld(sequence($itype,$NC), my $d2c=mmzeroes("$vsdir/d2c.pdl",$itype,$ND));
  undef $c2d;
  undef $d2c;

  ##-- create: aux: metadata attributes
  @{$vs->{meta}} = sort keys %meta;
  my %efopts     = (flags=>$vs->{flags}, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my $NM         = scalar @{$vs->{meta}};
  $mvals         = $vs->{mvals} = mmzeroes("$vsdir/mvals.pdl", $itype,$NM,$NC); ##-- [$mpos,$ci] => $mvali_at_ci : keep
  my ($menum);
  foreach (0..($NM-1)) {
    $vs->vlog($logCreate, "create(): creating metadata enum for attribute '$vs->{meta}[$_]'");
    $mattr = $vs->{meta}[$_];
    $mdata = $meta{$mattr};
    $menum = $vs->{"meta_e_$mattr"} = $DiaColloDB::ECLASS->new(%efopts);
    tied(@{$mdata->{vals}})->flush if ($mdata->{vals});
    defined(my $mmvals = readPdlFile("$vsdir/mvals_$mattr.pf", ReadOnly=>1,Dims=>[$NC],Datatype=>$itype))
      or $vs->logconfess("create(): failed to mmap $vsdir/mvals_$mattr.pf");
    ($tmp=$mvals->slice("($_),")) .= $mmvals;
    undef $mmvals;
    delete $mdata->{vals};
    $menum->fromHash($mdata->{s2i})
      or $vs->logconfess("create(): failed to create metadata enum for attribute '$mattr': $!");
    $menum->save("$vsdir/meta_e_$mattr")
      or $vs->logconfess("create(): failed to save metadata enum $vsdir/meta_e_$mattr: $!");
  }
  ##
  $vs->vlog($logCreate, "create(): creating metadata sort-indices (NM=$NM x NC=$NC)");
  my $msorti = $vs->{msorti} = mmzeroes("$vsdir/msorti.pdl", $itype, $NC,$NM); ##-- [,($mi)] => $mvals->slice("($mi),")->qsorti
  foreach (0..($NM-1)) {
    $mvals->slice("($_),")->qsorti($msorti->slice(",($_)"));
  }

  ##-- save: header
  $vs->vlog($logCreate, "create(): saving to $base*");
  $vs->saveHeader()
    or $vs->logconfess("create(): failed to save header data: $!");

  ##-- cleanup: temps
  if (!$coldb->{keeptmp}) {
    CORE::unlink($tdm0file)
	or $vs->logconfess("create(): failed to unlink tempfile $tdm0file: $!");
  }

  ##-- return
  return $vs;
}

##----------------------------------------------------------------------
## create: utils

##==============================================================================
## Relation API: union : TODO

## $vs = CLASS_OR_OBJECT->union($coldb, \@dbargs, %opts)
##  + merge multiple co-frequency indices into new object
##  + @dbargs : array of sub-objects ($coldb,...) containing {vsem} keys
##  + %opts: clobber %$vs
##  + implicitly flushes the new index
sub union {
  my ($vs,$coldb,$dbargs,%opts) = @_;
  $vs->logconfess("union(): native index mode not yet implemented");

  ##-- union: create/clobber
  $vs = $vs->new() if (!ref($vs));
  @$vs{keys %opts} = values %opts;

  ##-- union: sanity checks
  my $base = $vs->{base};
  $vs->logconfess("union(): no 'base' key defined") if (!$base);

  ##-- union: output directory
  my $vsdir = "$base.d";
  $vsdir =~ s{/$}{};
  !-d $vsdir
    or remove_tree($vsdir)
      or $vs->logconfess("union(): could not remove stale $vsdir: $!");
  make_path($vsdir)
    or $vs->logconfess("union(): could not create Vsem directory $vsdir: $!");

  ##-- union: logging
  my $logCreate = 'trace';

  ##-- union: create dcmap
  $vs->{dcopts} //= {};
  @{$vs->{dcopts}}{keys %{$coldb->{vsopts}//{}}} = values %{$coldb->{vsopts}//{}};
  my $map = $vs->{dcmap} = DocClassify::Mapper->new( %{$vs->{dcopts}} )
    or $vs->logconfess("union(): failed to create DocClassify::Mapper object");
  my $mverbose = $map->{verbose};
  $map->{verbose} = min2($mverbose,1);

  ##-- union: save local copy of attributes
  $vs->{attrs} = $coldb->{attrs};

  ##-- union: common variables
  my $itype = $map->itype;
  my $vtype = $map->vtype;
  my %ioopts = %{$vs->{dcio}//{}};
  my ($tmp,$tmp1);

  ##-- union: term-attributes: extract tuples
  my $NA  = scalar @{$coldb->{attrs}};
  my $NT0 = pdl($itype, [map {$_->{vsem}->nTerms} @$dbargs])->sum;
  $vs->vlog($logCreate, "union(): term-attribute tuples: extract (NA=$NA x NT<=$NT0)");
  my ($db,$dbvs,$tslice,$utvals, $a,$apos,$uapos, $a2u);
  my $tvals0 = zeroes($itype,$NA,$NT0);
  my $toff = 0;
  foreach $db (@$dbargs) {
    $dbvs   = $db->{vsem};
    $tslice = $db->{_vsunion_tslice0} = "$toff:".($toff+$dbvs->nTerms-1);
    foreach $a (@{$dbvs->{attrs}}) {
      next if (!defined($apos  = $dbvs->tpos($a)));
      next if (!defined($uapos = $vs->tpos($a)));
      $a2u = pdl($itype, $db->{"_union_${a}i2u"}->toArray);
      ($tmp=$tvals0->slice("($uapos),$tslice")) .= $a2u->index( $dbvs->{tvals}->slice("($apos),") );
      undef $a2u;
    }
    $toff += $dbvs->nTerms;
  }

  ##-- union: term-attributes: map
  $vs->vlog($logCreate, "union(): term-attribute tuples: map & sort");
  my $tvals = $vs->{tvals} = $tvals0->vv_uniqvec;
  my $NT    = $tvals->dim(1);
  foreach $db (@$dbargs) {
    $tslice = $db->{"_vsunion_tslice0"};
    $db->{"_vsunion_t2u"} = $tvals0->slice(",$tslice")->vsearchvec( $tvals );
  }
  my $tsorti = $vs->{tsorti} = zeroes($map->itype, $NT,$NA); ##-- [,($apos)] => $tvals->slice("($apos),")->qsorti
  foreach (0..($NA-1)) {
    $tvals->slice("($_),")->qsorti($tsorti->slice(",($_)"));
  }
  undef $tvals0;

  ##-- union: metadata
  $vs->vlog($logCreate, "union(): metadata: extract");
  my $mgood = $vs->{mgood} ? qr{$vs->{mgood}} : undef;
  my $mbad  = $vs->{mbad}  ? qr{$vs->{mbad}}  : undef;
  my %meta = (map {($_=>{n=>0, s2i=>{}, vals=>undef})}
	      grep { !(defined($mgood) && $_ !~ $mgood) || !(defined($mbad) && $_ =~ $mbad) }
	      map {@{$_->{vsem}{meta}}}
	      @$dbargs);
  my $meta = $vs->{meta} = [sort keys %meta];
  my $NM   = scalar @$meta;
  my $NC   = pdl($itype, [map {$_->{vsem}->nCats} @$dbargs])->sum;
  my $mvals = $vs->{mvals} = zeroes($itype, $NM,$NC);
  my $moff = 0;
  my ($mslice,$m,$mdata,$mi,$mstrs,$mpos,$umpos,$m2u);
  foreach $db (@$dbargs) {
    $dbvs = $db->{vsem};
    $mslice = "$moff:".($moff+$dbvs->nCats-1);
    foreach $m (@{$dbvs->{meta}}) {
      next if (!defined($mpos = $dbvs->mpos($m)));
      next if (!defined($umpos = $vs->mpos($m)));
      next if (!defined($mdata = $meta{$m}));
      foreach (@{$mstrs=$dbvs->{"meta_e_$m"}->toArray}) {
	$mi = $mdata->{s2i}{$_} = $mdata->{n}++ if (!defined($mi=$mdata->{s2i}{$_}));
      }
      $m2u = pdl($itype, [@{$mdata->{s2i}}{@$mstrs}]);
      ($tmp=$mvals->slice("($umpos),$mslice")) .= $m2u->index( $dbvs->{mvals}->slice("($mpos),") );
      undef $m2u;
    }
    $moff += $dbvs->nCats;
  }

  ##-- union: meta-attributes: sort
  $vs->vlog($logCreate, "union(): metadata: sort-indices (NM=$NM x NC=$NC)");
  my $msorti = $vs->{msorti} = zeroes($map->itype, $NC,$NM); ##-- [,($mi)] => $mvals->slice("($mi),")->qsorti
  foreach (0..($NM-1)) {
    $mvals->slice("($_),")->qsorti($msorti->slice(",($_)"));
  }

  ##-- union: meta-attributes: enums
  $vs->vlog($logCreate, "union(): metadata: enums");
  my %efopts = (flags=>$vs->{flags}, pack_i=>$coldb->{pack_id}, pack_o=>$coldb->{pack_off}, pack_l=>$coldb->{pack_len});
  my ($menum,$mattr);
  foreach (0..($NM-1)) {
    $vs->vlog($logCreate, "union(): metadata: enum: $vs->{meta}[$_]");
    $mattr = $vs->{meta}[$_];
    $mdata = $meta{$mattr};
    $menum = $vs->{"meta_e_$mattr"} = $DiaColloDB::ECLASS->new(%efopts);
    $menum->fromHash($mdata->{s2i})
      or $vs->logconfess("union(): failed to create metadata enum for attribute '$mattr': $!");
    $menum->save("$vsdir/meta_e_$mattr")
      or $vs->logconfess("create(): failed to save metadata enum $vsdir/meta_e_$mattr: $!");
  }
  undef %meta;

  ##-- union: aux: info
  $vs->{N} = pdl($itype, [map {$_->{vsem}{N}} @$dbargs])->sum;

  ##-- union: mapper: enums
  my $ND = pdl($itype, [map {$_->{vsem}->nDocs} @$dbargs])->sum;
  $vs->vlog($logCreate, "union(): mapper: identity-enums");
  $map->{tenum}  = $vs->idEnum($NT);
  $map->{gcenum} = $vs->idEnum($NC);
  $map->{lcenum} = $vs->idEnum($NC);
  $map->{denum}  = $vs->idEnum($ND);

  ##-- union:    d2c: ($ND)  : [$di]   => $ci
  ##-- union:    c2d: (2,$NC): [0,$ci] => $di_off, [1,$ci] => $di_len
  ##-- union: c2date: ($NC)  : [$ci]   => $date
  $vs->vlog($logCreate, "union(): doc<->category translation piddles (ND=$ND, NC=$NC)");
  my $d2c = $vs->{d2c} = zeroes($itype, $ND);
  my $c2d = $vs->{c2d} = zeroes($itype, 2,$NC);
  my $c2date = $vs->{c2date} = zeroes(ushort, $NC);
  my ($doff,$coff) = (0,0);
  foreach $db (@$dbargs) {
    $dbvs   = $db->{vsem};

    ($tmp=$d2c->slice("$doff:".($doff+$dbvs->nDocs-1))) .= $dbvs->{d2c};
    $tmp  += $coff;

    ($tmp=$c2d->slice(",$coff:".($coff+$dbvs->nCats-1))) .= $dbvs->{c2d};
    ($tmp1=$tmp->slice("(0),")) += $doff;

    ($tmp=$c2date->slice("$coff:".($coff+$dbvs->nCats-1))) .= $dbvs->{c2date};

    $doff += $dbvs->nDocs;
    $coff += $dbvs->nCats;
  }

  ##-- union: mapper: dcm
  $vs->vlog($logCreate, "union(): mapper: dcm (ND=$ND x NC=$NC)");
  my $dcm_w = zeroes($itype, 2,$ND);
  ($tmp=$dcm_w->slice("(0),")) .= sequence($itype,$ND);
  ($tmp=$dcm_w->slice("(1),")) .= $d2c;
  my $dcm = $map->{dcm} = PDL::CCS::Nd->newFromWhich($dcm_w, ones(byte,$ND)->append(0), steal=>1);

  ##-- union: mapper: tdm0
  $vs->vlog($logCreate, "union(): mapper: tdm0 (NT=$NT x ND=$ND)");
  my $tdm0_nnz = pdl($itype, [map {$_->{vsem}{dcmap}{tdm}->_nnz} @$dbargs])->sum;
  my $tdm0_w   = zeroes($itype, 2,$tdm0_nnz);
  my $tdm0_v   = zeroes($vtype,   $tdm0_nnz);
  $doff = 0;
  my $nzoff = 0;
  my ($dbmap,$dbtdm0,$nzslice);
  foreach $db (@$dbargs) {
    $dbvs   = $db->{vsem};
    $dbmap  = $dbvs->{dcmap};
    $dbtdm0 = $dbmap->get_tdm0();
    $nzslice = "$nzoff:".($nzoff+$dbmap->{tdm}->_nnz-1);
    $vs->logconfess("union(): mapper: tdm0: size mismatch: nnz(tdm0)=".$dbtdm0->_nnz." != ".$dbmap->{tdm}->_nnz."=nnz(tdm)")
      if ($dbtdm0->_nnz != $dbmap->{tdm}->_nnz);

    ($tmp=$tdm0_w->slice(",$nzslice")) .= $dbtdm0->_whichND;
    ($tmp1=$tmp->slice("(0),")) .= $db->{_vsunion_t2u}->index( $dbtdm0->_whichND->slice("(0),") );
    ($tmp1=$tmp->slice("(1),")) += $doff;

    ($tmp=$tdm0_v->slice("$nzslice")) .= $dbtdm0->_nzvals;

    delete $dbmap->{tdm0};
    $doff  += $dbvs->nDocs;
    $nzoff += $dbtdm0->_nnz;
  }
  my $tdm0 = $map->{tdm0} = PDL::CCS::Nd->newFromWhich($tdm0_w,$tdm0_v, missing=>0,dims=>[$NT,$ND]);
  undef $tdm0_w;
  undef $tdm0_v;

  ##-- union: mapper: tdm, tw, etc.
  $map->compile_tdm();

  ##-- union: mapper: ByLemma stuff
  $map->compile_disto();
  $map->lemmatizer();

  ##-- union: mapper: svd
  $vs->vlog($logCreate, "union(): mapper: svd");
  $map->get_tdm0(); ##-- needed for compile_xcm() with catProfile='average'
  $map->compileLocal(label=>"UNION", svdShrink=>1)
    or $vs->logconfess("union(): failed to compile mapper sub-object");

  ##-- mapper: aux data: ccsDocMissing ccsSvdNil (xcm|xdm|xtm)_sigma:implicit in DocClassify::Mapper::LSI::saveDirData()

  ##-- save
  $vs->vlog($logCreate, "union(): saving to $base*");

  ##-- save: header
  $vs->saveHeader()
    or $vs->logconfess("union(): failed to save header data: $!");

  ##-- save: mapper
  $map->saveDir("$vsdir/map.d", %ioopts)
    or $vs->logconfess("union(): failed to save mapper data to ${vsdir}/map.d: $!");

  ##-- save: aux data: piddles
  foreach (qw(tvals tsorti mvals msorti d2c c2d c2date)) {
    $map->writePdlFile($vs->{$_}, "$vsdir/$_.pdl", %ioopts)
      or $vs->logconfess("create(): failed to save auxilliary piddle $vsdir/$_.pdl: $!");
  }

  ##-- union: cleanup temporaries
  if (!$vs->{keeptmp}) {
    $vs->vlog($logCreate, "union(): cleaning up local temporaries");
    foreach $db (@$dbargs) {
      delete @$db{qw(_vsunion_tslice0 _vsunion_t2u)};
    }
  }

  ##-- union: all done
  return $vs;
}

##==============================================================================
## Relation API: dbinfo

## \%info = $rel->dbinfo($coldb)
##  + embedded info-hash for $coldb->dbinfo()
sub dbinfo {
  my $vs = shift;
  my $info = $vs->SUPER::dbinfo();
  @$info{qw(dcopts attrs meta mgood mbad N)} = @$vs{qw(dcopts attrs meta mgood mbad N)};
  $info->{nTerms} = $vs->nTerms;
  $info->{nDocs}  = $vs->nDocs;
  $info->{nCats}  = $vs->nCats;
  return $info;
}


##==============================================================================
## Relation API: profiling & comparison: top-level

##--------------------------------------------------------------
## Relation API: profile

## $mprf = $rel->profile($coldb, %opts)
## + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
## + %opts: as for DiaColloDB::Relation::profile()
## + really just wraps $rel->pprofile(), DiaColloDB::Profile::Pdl::toProfile(), and DiaColloDB::Profile::Multi::stringify()
sub profile {
  my ($vs,$coldb,%opts) = @_;

  ##-- vector-based profile
  my $pprfs     = $vs->vprofile($coldb,\%opts);
  my $groupby = $opts{groupby};

  ##-- construct multi-profile
  $vs->vlog($vs->{logvprofile}, "profile(): constructing output profile [strings=".($opts{strings} ? 1 : 0)."]");
  my $mp = DiaColloDB::Profile::Multi->new(profiles=>[map {$_->toProfile} @$pprfs],
					   titles=>$groupby->{titles},
					   qinfo=>$vs->qinfo($coldb, %opts),
					  );
  $mp->stringify($groupby->{g2s}) if ($opts{strings});

  return $mp;
}

##--------------------------------------------------------------
## Relation API: comparison (diff)

## $mpdiff = $rel->compare($coldb, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + %opts as for DiaColloDB::Relation::compare(), of which this method is a modified version
sub compare {
  my ($vs,$coldb,%opts) = @_;

  ##-- sanity checks / fixes
  $vs->{attrs} = $coldb->{attrs} if (!@{$vs->{attrs}//[]});

  ##-- common variables
  my $logLocal   = $coldb->{logProfile};
  my $groupby    = $opts{groupby} = $vs->groupby($coldb, $opts{groupby}, relax=>0);
  my %aopts      = map {exists($opts{"a$_"}) ? ($_=>$opts{"a$_"}) : qw()} (qw(query date slice), @{$opts{_abkeys}//[]});
  my %bopts      = map {exists($opts{"b$_"}) ? ($_=>$opts{"b$_"}) : qw()} (qw(query date slice), @{$opts{_abkeys}//[]});
  my %popts      = (kbest=>-1,cutoff=>'',global=>0,strings=>0,fill=>1, groupby=>$groupby);

  ##-- get profiles to compare
  my $aprfs = $vs->vprofile($coldb,{%opts, %aopts,%popts}) or return undef;
  my $bprfs = $vs->vprofile($coldb,{%opts, %bopts,%popts}) or return undef;

  ##-- alignment and trimming
  my $diffop = DiaColloDB::Profile::PdlDiff->diffop($opts{diff});
  $vs->vlog($logLocal, "compare(): align and trim (".($opts{global} ? 'global' : 'local')."; diff=$diffop)");
  my $ppairs = DiaColloDB::Profile::MultiDiff->align($aprfs,$bprfs);
  DiaColloDB::Profile::PdlDiff->trimPairs($ppairs, %opts); ##-- vsem version
  my $pdiffs = [map {DiaColloDB::Profile::PdlDiff->new(@$_, diff=>$opts{diff})} @$ppairs];
  if (!$opts{global}) {
    $_->gtrim( DiaColloDB::Profile::Diff->diffkbest($opts{diff})=>$opts{kbest} ) foreach (@$pdiffs);
  }

  ##-- convert to multi-diff and stringify
  my $diffs = [map {$_->toProfile} @$pdiffs];
  my $diff  = DiaColloDB::Profile::MultiDiff->new(profiles=>$diffs, titles=>$opts{groupby}{titles}, diff=>$opts{diff}, populate=>0);
  if ($opts{strings}//1) {
    $vs->vlog($logLocal, "compare(): stringify");
    $diff->stringify($groupby->{g2s});
  }

  return $diff;
}


##==============================================================================
## Profile: Utils: PDL-based profiling

## \@pprfs = $vs->vprofile($coldb, \%opts)
## + get a relation profile for selected items as an ARRAY of DiaColloDB::Profile objects
## + %opts: as for DiaColloDB::Relation::profile()
## + new/altered %opts:
##   (
##    vq      => $vq,        ##-- parsed query, DiaColloDB::Relation::Vsem::Query object
##    groubpy => \%groupby,  ##-- as returned by $vs->groupby($coldb, \%opts)
##    dlo     => $dlo,       ##-- as returned by $coldb->parseDateRequest(@opts{qw(date slice fill)},1);
##    dhi     => $dhi,       ##-- as returned by $coldb->parseDateRequest(@opts{qw(date slice fill)},1);
##    dslo    => $dslo,      ##-- as returned by $coldb->parseDateRequest(@opts{qw(date slice fill)},1);
##    dshi    => $dshi,      ##-- as returned by $coldb->parseDateRequest(@opts{qw(date slice fill)},1);
##   )
sub vprofile {
  my ($vs,$coldb,$opts) = @_;

  ##-- common variables
  my $logLocal = $vs->{logvprofile};

  ##-- sanity checks / fixes
  $vs->{attrs} = $coldb->{attrs} if (!@{$vs->{attrs}//[]});

  ##-- parse query
  my $groupby = $opts->{groupby} = $vs->groupby($coldb, $opts->{groupby}, relax=>0); ##-- TODO: allow metadata restrictions (but not group-keys)
  ##
  my $q = $opts->{qobj} // $coldb->parseQuery($opts->{query}, logas=>'query', default=>'', ddcmode=>1);
  my ($qo);
  $q->setOptions($qo=DDC::XS::CQueryOptions->new) if (!defined($qo=$q->getOptions));
  #$qo->setFilters([@{$qo->getFilters}, @$gbfilters]) if (@$gbfilters);
  $opts->{qobj} //= $q;

  ##-- parse date-request
  my ($dfilter,$dslo,$dshi,$dlo,$dhi) = $coldb->parseDateRequest(@$opts{qw(date slice fill)},1);
  @$opts{qw(dslo dshi dlo dhi)} = ($dslo,$dshi,$dlo,$dhi);

  ##-- parse & compile query
  my %vqopts = (%$opts,coldb=>$coldb,vsem=>$vs);
  my $vq     = $opts->{vq} = DiaColloDB::Relation::Vsem::Query->new($q)->compile(%vqopts);

  ##-- sanity checks: null-query
  my ($ti,$ci) = @$vq{qw(ti ci)};
  if (defined($ti) && !$ti->nelem) {
    $vs->logconfess($coldb->{error}="no index term(s) matched user query \`$opts->{query}'");
  } elsif (defined($ci) && !$ci->nelem) {
    $vs->logconfess($coldb->{error}="no index document(s) matched user query \`$opts->{query}'");
  }

  ##-- get query-vector
  my $tdm     = $vs->{tdm};
  my $sliceby = $opts->{slice} || 0;
  my ($qwhich,$qvals);
  if (defined($ti) && defined($ci)) {
    ##-- both term- and document-conditions
    $vs->vlog($logLocal, "vprofile($sliceby): query vector: xsubset");
    my $q_c2d     = $vs->{c2d}->dice_axis(1,$ci);
    my $di        = $q_c2d->slice("(1),")->rldseq($q_c2d->slice("(0),"))->qsort;
    my $subsize   = $ti->nelem * $di->nelem;
    $vs->vlog($logLocal, "vprofile($sliceby): requested subset size = $subsize (NT=".$ti->nelem." x Nd=".$ci->nelem.")");
    if (defined($vs->{submax}) &&  $subsize > $vs->{submax}) {
      $vs->logconfess($coldb->{error}="requested subset size $subsize (NT=".$ti->nelem." x Nd=".$ci->nelem.") too large; max=$vs->{submax}");
    }

    my $q_tdm  = $tdm->xsubset2d($ti,$di);

    $vs->vlog($logLocal, "vprofile($sliceby): query vector: decode");
    ##<<QPZD
    #$q_tdm->_vals->indadd($q_tdm->_whichND->slice("(1),"), $qvec=zeroes($q_tdm->type, $vs->nDocs));
    $q_tdm  = $q_tdm->sumover;
    $qwhich = $q_tdm->_whichND->flat;
    $qvals  = $q_tdm->_nzvals;
    ##>>QPZD
  }
  elsif (defined($ti)) {
    ##-- term-conditions only: extract from $vs->{tdm}
    $vs->vlog($logLocal, "vprofile($sliceby): query vector: terms");
    ##-- find matching nz-indices WITH ptr(0) [direct ccs construction is still too slow]
    my $ptr0    = $vs->{ptr0};
    my $nzi_off = $ptr0->index($ti);
    my $nzi_len = $ptr0->index($ti+1)-$nzi_off;
    my $nzi     = $nzi_len->rldseq($nzi_off);

    if (!$nzi->isempty) {
      $tdm->_vals->index($nzi)->indadd($tdm->_whichND->index2d(1,$nzi),
				       my $qvec=zeroes($tdm->type, $vs->nDocs));
      $qwhich = $qvec->which;
      $qvals  = $qvec->index($qwhich);
    }
  }
  elsif (defined($ci)) {
    ##-- doc-conditions only: slice from $map->{xcm}
    $vs->vlog($logLocal, "vpslice($sliceby): query vector: cats");
    my $q_c2d   = $vs->{c2d}->dice_axis(1,$ci);
    my $di      = $q_c2d->slice("(1),")->rldseq($q_c2d->slice("(0),")); #->qsort;

    ##-- sanity check
    $vs->logconfess("vpslice($sliceby): unsorted doc-list when decoding cat conditions") ##-- sanity check
      if ($di->nelem > 1 && !all($di->slice("0:-2") <= $di->slice("1:-1")));

    ##-- find matching nz-indices WITH ptr(1)
    my ($ptr1,$pix1) = @$vs{qw(ptr1 pix1)};
    my $nzi_off = $ptr1->index($di);
    my $nzi_len = $ptr1->index($di+1)-$nzi_off;
    my $nzi     = $nzi_len->rldseq($nzi_off)->qsort;

    if (!$nzi->isempty) {
      $tdm->_nzvals->index($nzi)->indadd($tdm->_whichND->slice("(1),")->index($nzi),
					 my $qvec=zeroes($tdm->type, $vs->nDocs));
      $qwhich = $qvec->which;
      $qvals  = $qvec->index($qwhich);
    }
  }

  ##-- evaluate query: get co-occurrence frequencies (groupby terms only)
  $vs->vlog($logLocal, "vprofile(): evaluating query: f12p");
  my $cofsub = PDL->can('diacollo_tdm_cof_t_'.$vs->itype) || \&PDL::diacollo_tdm_cof_t_long;
  my $gbpdl  = pdl($vs->itype, [map {$vs->tpos($_)} @{$groupby->{attrs}}]);
  $cofsub->($tdm->_whichND, @$vs{qw(ptr1 pix1)}, $tdm->_vals,
	    @$vs{qw(tvals d2c c2date)},
	    $sliceby,
	    $qwhich, $qvals,
	    $gbpdl,
	    my $f12p={});
  $vs->debug("found ", scalar(keys %$f12p), " item2 tuple(s)");

  ##-- get item2 keys (groupby terms only)
  $vs->vlog($logLocal, "vprofile(): evaluating query: f2p");
  my $pack_ix = $PDL::Types::pack[ $vs->itype->enum ];
  (my $pack_gkey = $pack_ix) =~ s/\*$/"[".scalar(@{$groupby->{attrs}})."]"/e;
  my $gkeys2  = pdl($vs->itype, map {unpack($pack_gkey,$_)} keys %$f12p);
  $gkeys2->reshape(scalar(@{$groupby->{attrs}}), $gkeys2->nelem/scalar(@{$groupby->{attrs}}));
  my $gti2    = undef;
  foreach (0..($gkeys2->dim(0)-1)) {
    my $gtia = $vs->termIds($groupby->{attrs}[$_], $gkeys2->slice("($_),"))->uniq;
    $gti2    = defined($gti2) ? $gti2->v_intersect($gtia) : $gtia;
  }
  my $gfsub = PDL->can('diacollo_tdm_gf_t_'.$vs->itype) || \&PDL::diacollo_tdm_gf_t_long;
  $gfsub->($tdm->_whichND, $vs->{ptr0}, $tdm->_vals,
	   @$vs{qw(tvals d2c c2date)},
	   $sliceby,
	   ($gti2//null->convert($vs->itype)),
	   $gbpdl,
	   $f12p,
	   my $f2p={});
  $vs->debug("got ", scalar(keys %$f2p), " independent item2 tuple-frequencies");
  $vs->logdie("what now?");


  ##-- DEBUG: dump f12p
  if (0) {
    my @tenums = map {$coldb->{"${_}enum"}} @{$vs->{attrs}};
    my (@avals,@astrs,$slice);
    binmode(STDOUT,':utf8');
    foreach my $key (sort {$f12p->{$b}<=>$f12p->{$a}} keys %$f12p) {
      @avals = unpack($pack_ix, $key);
      $slice = pop(@avals);
      @astrs = map {$tenums[$_]->i2s($avals[$_])} (0..$#avals);
      print "DEBUG: ", join("\t", $f12p->{$key}, $slice, @astrs), "\n";
    }
    $vs->logdie("what now?"); ##-- DEBUG
  }

  ##-- evaluate query: get independent item2 frequencies
  $vs->vlog($logLocal, "vprofile(): evalating query: f2p");
    $vs->logdie("what now?"); ##-- DEBUG

    ##-- OLD
  my (@pprfs,$pprf);
  foreach my $sliceVal ($opts->{slice} ? ($vq->{slices}->list) : 0) {
    $pprf = $vs->vpslice($coldb,$sliceVal,$opts);
    push(@pprfs,$pprf);
  }

  ##-- trim results
  if ($opts->{global} && @pprfs > 1) {
    ##-- trim: global
    my $pprfg  = DiaColloDB::Profile::Pdl->averageOver(\@pprfs);
    my $keep = $pprfg->gwhich(%$opts);
    $_->gtrim(keep=>$keep) foreach (@pprfs);
  }
  else {
    ##-- trim: local
    $_->gtrim(%$opts) foreach (@pprfs);
  }

  ##-- return
  return \@pprfs;
}

##----------------------------------------------------------------------
## $pprf_or_undef = $vs->vpslice($coldb, $sliceVal, \%opts)
## + get a slice-local profile as a DiaColloDB::Profile::Pdl object
## + %opts: as for vprofile()
sub vpslice {
  my ($vs,$coldb,$sliceVal,$opts) = @_;

  ##-- common variables
  my $logLocal = $vs->{logvprofile};
  my ($groupby,$vq) = @$opts{qw(groupby vq)};
  my %vqopts = (%$opts,vsem=>$vs,coldb=>$coldb);

  ##-- construct query: common
  my ($qvec); ##-- query-vector: ($svdR,1) : [$ri] => $x
  my $pprf  = DiaColloDB::Profile::Pdl->new(label=>$sliceVal,
					    N    =>$vs->{N});
  my $ti  = $vq->{ti};
  my $ci  = $vq->sliceCats($sliceVal,%vqopts);
  my $tdm = $vs->{tdm};

  ##-- construct query: sanity checks: null vectors --> null profile
  return $pprf if ((defined($ti) && !$ti->nelem) || (defined($ci) && !$ci->nelem));

  ##-- construct query: dispatch
  my ($f1); ##-- for profile construction
  if (defined($ti) && defined($ci)) {
    ##-- both term- and document-conditions
    $vs->vlog($logLocal, "vpslice($sliceVal): query vector: xsubset");
    my $q_c2d     = $vs->{c2d}->dice_axis(1,$ci);
    my $di        = $q_c2d->slice("(1),")->rldseq($q_c2d->slice("(0),"))->qsort;
    my $subsize   = $ti->nelem * $di->nelem;
    $vs->vlog($logLocal, "vpslice($sliceVal): requested subset size = $subsize (NT=".$ti->nelem." x Nd=".$ci->nelem.")");
    if (defined($vs->{submax}) &&  $subsize > $vs->{submax}) {
      $vs->logconfess($coldb->{error}="requested subset size $subsize (NT=".$ti->nelem." x Nd=".$ci->nelem.") too large; max=$vs->{submax}");
    }

    my $q_tdm     = $tdm->xsubset2d($ti,$di);
    return $pprf if ($q_tdm->allmissing); ##-- empty subset --> null profile

    $vs->vlog($logLocal, "vpslice($sliceVal): query vector: decode");
    ##<<QPZD
    #$q_tdm->_vals->indadd($q_tdm->_whichND->slice("(1),"), $qvec=zeroes($q_tdm->type, $vs->nDocs));
    $qvec = $q_tdm->sumover;
    ##>>QPZD

    ##-- get f1
    $f1 = $q_tdm->_nzvals / $vs->{tw}->index($q_tdm->_whichND->slice("(0),"));
    $f1 *= log(2);
    $f1->inplace->exp;
    $f1 -= $vs->{smoothf};
    $f1  = $f1->sumover->rint;

    ##-- old
    #$q_tdm = $q_tdm->sumover->dummy(0,1)->make_physically_indexed;
    #$vs->vlog($logLocal, "profile(): query vector: xsubset: svdapply");
    #$qvec = $map->{svd}->apply1($q_tdm)->xchg(0,1);
  }
  elsif (defined($ti)) {
    ##-- term-conditions only: extract from $vs->{tdm}
    $vs->vlog($logLocal, "vpslice($sliceVal): query vector: terms");
    ##<<
    ##-- find matching nz-indices WITHOUT ptr(0)
    #my $tdm_t   = $tdm->_whichND->slice("(0),");
    #my $nzi0  = vsearch($ti,   $tdm_t);
    #my $nzi1  = vsearch($ti+1, $tdm_t);
    #$nzi1->where(($nzi1==$tdm->_nnz-1) & ($tdm_t->index($nzi1)==$ti))++;       ##-- special case for final nz-value
    #$nzi1    -= $nzi0;							 ##-- convert to lengths
    #my $nzi   = $nzi1->rldseq($nzi0);					         ##-- decode to nz-indices
    ##
    ##-- find matching nz-indices WITH ptr(0) [direct ccs construction is still too slow]
    my $ptr0    = $vs->{ptr0};
    my $nzi_off = $ptr0->index($ti);
    my $nzi_len = $ptr0->index($ti+1)-$nzi_off;
    my $nzi     = $nzi_len->rldseq($nzi_off);
    ##>>

    if (!$nzi->isempty) {
      $tdm->_vals->index($nzi)->indadd(#$tdm->_whichND->slice("(1),")->index($nzi),
				       $tdm->_whichND->index2d(1,$nzi),
				       $qvec=zeroes($tdm->type, $vs->nDocs));
    }

    ##-- get f1
    $f1 = $tdm->_vals->index($nzi) / $vs->{tw}->index($tdm->_whichND->index2d(0,$nzi));
    $f1 *= log(2);
    $f1->inplace->exp;
    $f1 -= $vs->{smoothf};
    $f1  = $f1->sumover->rint;
  }
  elsif (defined($ci)) {
    ##-- doc-conditions only: slice from $map->{xcm}
    $vs->vlog($logLocal, "vpslice($sliceVal): query vector: cats");
    my $q_c2d   = $vs->{c2d}->dice_axis(1,$ci);
    my $di      = $q_c2d->slice("(1),")->rldseq($q_c2d->slice("(0),")); #->qsort;

    ##-- sanity check
    $vs->logconfess("vpslice($sliceVal): unsorted doc-list when decoding cat conditions") ##-- sanity check
      if ($di->nelem > 1 && !all($di->slice("0:-2") <= $di->slice("1:-1")));

    ##<<
    ##-- find matching nz-indices WITHOUT ptr(1) [direct ccs construction still too slow
    #my $nz_ci  = $vs->{d2c}->index($tdm->_whichND->slice("(1),"));
    #my $nz_qsi = $nz_ci->qsorti;
    #my $nz_cix = $nz_ci->index($nz_qsi);
    #my $nzi0   = $ci->vsearch($nz_cix);
    #my $nzi1   = ($ci+1)->vsearch($nz_cix);
    #$nzi1->where(($nzi1==$tdm->_nnz-1) & ($nz_cix->index($nzi1)==$ci))++; ##-- special case for final nz-value
    ##
    #$nzi1 -= $nzi0;							  ##-- convert to lengths
    #my $nzi = $nz_qsi->index($nzi1->rldseq($nzi0));			  ##-- decode & translate to original nz-indices
    ##
    ##-- find matching nz-indices WITH ptr(1)
    my ($ptr1,$pix1) = @$vs{qw(ptr1 pix1)};
    my $nzi_off = $ptr1->index($di);
    my $nzi_len = $ptr1->index($di+1)-$nzi_off;
    my $nzi     = $nzi_len->rldseq($nzi_off)->qsort;
    ##>>

    if (!$nzi->isempty) {
      $tdm->_nzvals->index($nzi)->indadd($tdm->_whichND->slice("(1),")->index($nzi),
					 $qvec=zeroes($tdm->type, $vs->nDocs));

      ##-- get f1
      $f1 = $tdm->_nzvals->index($nzi) / $vs->{tw}->index($tdm->_whichND->slice("(0),")->index($nzi));
      $f1 *= log(2);
      $f1->inplace->exp;
      $f1 -= $vs->{smoothf};
      $f1  = $f1->sumover->rint;
    }
  }
  ##>>

  ##-- sanity check: ensure we've got a query vector
  $vs->logconfess($coldb->{error}="empty query vector for user query \`$opts->{query}'") if (!defined($qvec) || !$qvec->nelem);

  ##-- map to & extract k-best terms
  $vs->vlog($logLocal, "vpslice($sliceVal): extracting k-nearest neighbors (terms)");
  my $sim = $vs->qsim($qvec);                                                   ##-- cosine sim : [-1:1]
  $sim->inplace->setnantobad->inplace->setbadtoval(-1);				##-- ... map NaN=>$sim_min=-1
  #(my $tmp=$sim->index($ti)) .= -1 if (defined($ti));				##-- ... eliminate target term(s)?  -->goofy for diff
  $sim = $sim->index($groupby->{ghaving}) if (defined($groupby->{ghaving}));	##-- ... apply group-restriction

  ##-- groupby: aggregate
  $vs->vlog($logLocal, "vpslice($sliceVal): groupby");
  my ($g_keys,$g_sim) = $groupby->{gaggr}->($sim);

  ##-- convert distance to similarity: gaussian (distribution *does* look quite normal modulo long tail of "good" (latent) matches)
  #my $g_sim = ($g_sim)->gausscdf(0,0.25);
  ##$g_sim->inplace->minus(1,$g_sim,1);
  ##
  (my $noval = zeroes(float,$g_sim->nelem)) .= 'nan'; ##-- jQuery json parser chokes on unquoted "nan" values: argh
  @$pprf{qw(gkeys gvals f1 f2 f12)} = ($g_keys,$g_sim,$f1,$noval,$noval);
  return $pprf;
}

##----------------------------------------------------------------------
## $sim = $vs->qsim($qvec)
##   + return $sim(NT) in range [-1:1], NAN indicates bad norm
sub qsim {
  my ($vs,$qvec) = @_;
  ##<<PZD
  #return $vs->{tdm}->vcos_pzd($qvec, $vs->{vnorm0});
  $qvec = $qvec->toccs if (!UNIVERSAL::isa($qvec,'PDL::CCS::Nd'));
  $vs->{rows1} //= $vs->{tdm}->_whichND->index2d(0,$vs->{pix1});
  $vs->{vals1} //= $vs->{tdm}->_vals->index($vs->{pix1});
  return $vs->{ptr1}->ccs_vcos_pzd(@$vs{qw(rows1 vals1)}, $qvec->_whichND->flat,$qvec->_nzvals, $vs->{vnorm0});
  ##--!PZD
  #return $vs->{tdm}->vcos_zdd($qvec);						##-- cosine sim : [-1:1]
  ##>>PZD
}

##==============================================================================
## Profile: Utils: domain sizes

## $NT = $vs->nTerms()
##  + gets number of terms
sub nTerms {
  return $_[0]{tdm}->dim(0);
}

## $ND = $vs->nDocs()
##  + returns number of documents (breaks)
BEGIN { *nBreaks = \&nDocs; }
sub nDocs {
  return $_[0]{tdm}->dim(1);
}

## $NC = $vs->nFiles()
##  + returns number of categories (original source files)
BEGIN { *nCategories = *nCats = \&nFiles; }
sub nFiles {
  return $_[0]{c2date}->nelem;
}

## $NA = $vs->nAttrs()
##  + returns number of term-attributes
sub nAttrs {
  return $_[0]{tvals}->dim(0);
}

## $NM = $vs->nMeta()
##  + returns number of meta-attributes
sub nMeta {
  return $_[0]{mvals}->dim(0);
}

##==============================================================================
## Profile: Utils: attribute positioning

## \%tpos = $vs->tpos()
##  $tpos = $vs->tpos($tattr)
##  + get or build term-attribute position lookup hash
sub tpos {
  $_[0]{tpos} //= { (map {($_[0]{attrs}[$_]=>$_)} (0..$#{$_[0]{attrs}})) };
  return @_>1 ? $_[0]{tpos}{$_[1]} : $_[0]{tpos};
}

## \%mpos = $vs->mpos()
## $mpos  = $vs->mpos($mattr)
##  + get or build meta-attribute position lookup hash
sub mpos {
  $_[0]{mpos} //= { (map {($_[0]{meta}[$_]=>$_)} (0..$#{$_[0]{meta}})) };
  return @_>1 ? $_[0]{mpos}{$_[1]} : $_[0]{mpos};
}

##==============================================================================
## Profile: Utils: query parsing & evaluation

## $idPdl = $vs->idpdl($idPdl)
## $idPdl = $vs->idpdl(\@ids)
## $idPdl = $vs->idpdl($ids)
sub idpdl {
  shift if (UNIVERSAL::isa($_[0],__PACKAGE__));
  my $ids = shift;
  return null->long   if (!defined($ids));
  $ids = [$ids] if (!ref($ids));
  $ids = pdl(long,$ids) if (!UNIVERSAL::isa($ids,'PDL'));
  return $ids;
}

## $tupleIds = $vs->tupleIds($attrType, $attrName, $valIdsPdl)
## $tupleIds = $vs->tupleIds($attrType, $attrName, \@valIds)
## $tupleIds = $vs->tupleIds($attrType, $attrName, $valId)
sub tupleIds {
  my ($vs,$typ,$attr,$valids) = @_;
  $valids = $valids=$vs->idpdl($valids);

  ##-- check for empty value-set
  if ($valids->nelem == 0) {
    return null->convert($vs->itype);
  }

  ##-- non-empty: get base data
  my $apos = $vs->can("${typ}pos")->($vs,$attr);
  my $vals = $vs->{"${typ}vals"}->slice("($apos),");

  ##-- check for singleton value-set & maybe do simple linear search
  if ($valids->nelem == 1) {
    return ($vals==$valids)->which;
  }

  ##-- nontrivial value-set: do vsearch lookup
  my $sorti   = $vs->{"${typ}sorti"}->slice(",($apos)");
  my $vals_qs = $vals->index($sorti);
  my $i0      = $valids->vsearch($vals_qs);
  my $i0_mask = ($vals_qs->index($i0) == $valids);
  $i0         = $i0->where($i0_mask);
  my $ilen    = ($valids->where($i0_mask)+1)->vsearch($vals_qs);
  $ilen      -= $i0;
  $ilen->slice("-1")->lclip(1) if ($ilen->nelem); ##-- hack for bogus 0-length at final element
  my $iseq    = $ilen->rldseq($i0);
  return $sorti->index($iseq)->qsort;
}

## $ti = $vs->termIds($tattrName, $valIdsPDL)
## $ti = $vs->termIds($tattrName, \@valIds)
## $ti = $vs->termIds($tattrName, $valId)
sub termIds {
  return $_[0]->tupleIds('t',@_[1..$#_]);
}

## $ci = $vs->catIds($mattrName, $valIdsPDL)
## $ci = $vs->catIds($mattrName, \@valIds)
## $ci = $vs->catIds($mattrName, $valId)
sub catIds {
  return $_[0]->tupleIds('m',@_[1..$#_]);
}

## $bool = $vs->hasMeta($attr)
##  + returns true iff $vs supports metadata attribute $attr
sub hasMeta {
  return defined($_[0]->mpos($_[1]));
}

## $enum_or_undef = $vs->metaEnum($mattr)
##  + returns metadata attribute enum for $attr
sub metaEnum {
  my ($vs,$attr) = @_;
  return undef if (!$vs->hasMeta($attr));
  return $vs->{"meta_e_$attr"};
}

## $cats = $vs->catSubset($terms)
## $cats = $vs->catSubset($terms,$cats)
##  + gets (sorted) cat-subset for (sorted) term-set $terms
##  + too expensive, since it uses $tdm->dice_axis(0,$terms)
sub catSubset {
  my ($vs,$terms,$cats) = @_;
  return $cats if (!defined($terms));
  return DiaColloDB::Utils::_intersect_p($cats, $vs->{d2c}->index($vs->{tdm}->dice_axis(0,$terms)->_whichND->slice("(1),"))->uniq);
}


##----------------------------------------------------------------------

## \%groupby = $vs->groupby($coldb, $groupby_request, %opts)
## \%groupby = $vs->groupby($coldb, \%groupby,        %opts)
##  + modified version of DiaColloDB::groupby() suitable for pdl-ized Vsem relation
##  + $grouby_request : see DiaColloDB::parseRequest()
##  + returns a HASH-ref:
##    (
##     ##-- OLD: equivalent to DiaColloDB::groupby() return values
##     req => $request,    ##-- save request
##     areqs => \@areqs,   ##-- parsed attribute requests ([$attr,$ahaving],...)
##     attrs => \@attrs,   ##-- like $coldb->attrs($groupby_request), modulo "having" parts
##     titles => \@titles, ##-- like map {$coldb->attrTitle($_)} @attrs
##     ##
##     ##-- REMOVED: not constructed for Vsem::groupby()
##     #x2g => \&x2g,       ##-- group-id extraction code suitable for e.g. DiaColloDB::Relation::Cofreqs::profile(groupby=>\&x2g)
##     #g2s => \&g2s,       ##-- stringification object suitable for DiaColloDB::Profile::stringify() [CODE,enum, or undef]
##     ##
##     ##-- NEW: equivalent to DiaColloDB::groupby() return values
##     ghaving => $ghaving, ##-- pdl ($NHavingOk) : term indices $ti s.t. $ti matches groupby "having" requests
##     gaggr   => \&gaggr,  ##-- code: ($gkeys,$gdist) = gaggr($dist) : where $dist is diced to $ghaving on dim(1) and $gkeys is sorted
##     g2s     => \&g2s,    ##-- stringification object suitable for DiaColloDB::Profile::stringify() [CODE,enum, or undef]
##     ##
##     ##-- NEW: pdl utilties
##     #gv    => $gv,       ##-- pdl ($NG): [$gvi] => $gi : group-id enumeration
##     #gn    => $gn,       ##-- pdl ($NG): [$gvi] => $n  : number of terms in group
##    )
##  + %opts:
##     warn  => $level,    ##-- log-level for unknown attributes (default: 'warn')
##     relax => $bool,     ##-- allow unsupported attributes (default=0)
sub groupby {
  my ($vs,$coldb,$gbreq,%opts) = @_;
  return $gbreq if (UNIVERSAL::isa($gbreq,'HASH'));

  ##-- get data
  my $wlevel = $opts{warn} // 'warn';
  my $gb = { req=>$gbreq };

  ##-- get attribute requests
  my $gbareqs = $gb->{areqs} = $coldb->parseRequest($gb->{req}, %opts,logas=>'groupby');

  ##-- get attribute names (compat)
  my $gbattrs = $gb->{attrs} = [map {$_->[0]} @$gbareqs];

  ##-- get attribute titles
  $gb->{titles} = [map {$coldb->attrTitle($_)} @$gbattrs];

  ##-- get "having"-clause matches
  my $ghaving = undef;
  foreach (grep {$_->[1] && !UNIVERSAL::isa($_->[1],'DDC::XS::CQTokAny')} @$gbareqs) {
    my $avalids = $coldb->enumIds($coldb->{"$_->[0]enum"}, $_->[1], logLevel=>$coldb->{logProfile}, logPrefix=>"groupby(): fetch filter ids: $_->[0]");
    my $ahaving = $vs->termIds($_->[0], $avalids);
    $ghaving    = DiaColloDB::Utils::_intersect_p($ghaving,$ahaving);
  }
  $gb->{ghaving} = $ghaving;

  ##-- get pdl-ized group-aggregation and -stringification objects
  my ($g_keys); ##-- pdl ($NHavingOk): [$hvi] => $gi : term-ids for generic aggregation by $ghaving or raw term-id
  if (@{luniq($gbattrs)} == @{luniq($coldb->{attrs})}) {
    ##-- project all attributes: t2g: use native term-ids
    #$gb->{t2g} = sub { return $_[0]; };

    ##-- project all attribute: aggregate by term-identity; i.e. don't (+sorted)
    $gb->{gaggr} = sub {
      return (defined($ghaving)
	      ? ($ghaving,$_[0])
	      : (sequence(long,$_[0]->nelem),$_[0]));
    };

    ##-- project all attributes: g2s: stringification
    my $tvals  = $vs->{tvals};
    my @tenums = map {$coldb->{"${_}enum"}} @{$vs->{attrs}};
    my @gbpos  = map {$vs->tpos($_)} @$gbattrs;
    $gb->{g2s} = sub {
      return join("\t", map {$tenums[$_]->i2s($tvals->at($_,$_[0]))//''} @gbpos);
    };
  }
  elsif (@$gbattrs == 1) {
    ##-- project single attribute: t2g: use native attribute-ids
    my $gpos   = $vs->tpos($gbattrs->[0]);
    #$gb->{t2g} = sub { return $vs->{tvals}->slice("($gpos),")->index($_[0]); };

    ##-- project single attribute: gaggr: aggregate by native attribute-ids (-sorted)
    $g_keys = $vs->{tvals}->slice("($gpos),");

    ##-- project single attribute: g2s: stringification
    my $gbenum = $coldb->{$gbattrs->[0]."enum"};
    $gb->{g2s} = sub { return $gbenum->i2s($_[0]); };
  }
  else {
    ##-- project multiple attributes: t2g: create local vector-enum
    my $gpos   = [map {$vs->tpos($_)} @$gbattrs];
    my $gvecs  = $vs->{tvals}->dice_axis(0,$gpos);
    my $gsorti = $gvecs->vv_qsortveci;
    my $gvids  = zeroes(long, $gvecs->dim(1));
    $gvecs->dice_axis(1,$gsorti)->enumvecg($gvids->index($gsorti));
    #$gb->{t2g} = sub { return $gvids->index($_[0]); };

    ##-- project multiple attributes: gaggr: aggregate by local vector-enum (-sorted)
    $g_keys = $gvids;

    ##-- project multiple attributes: g2s: stringification
    my @tenums = map {$coldb->{"${_}enum"}} @{$vs->{attrs}};
    $gb->{g2s} = sub {
      return join("\t", map {$tenums[$gpos->[$_]]->i2s($gvecs->at($_,$_[0]))//''} (0..$#$gpos));
    };
  }

  ##-- aggregation: generic
  if (!defined($gb->{gaggr})) {
    $g_keys      = $g_keys->index($ghaving) if (defined($ghaving));
    my ($gv,$gn) = $g_keys->valcounts;
    my $g_vids   = $g_keys->vsearch($gv);
    $gb->{gaggr} = sub {
      my $dist = shift;
      my $g_vdist = zeroes($dist->type,$gv->nelem);
      $dist->indadd($g_vids, $g_vdist);
      $g_vdist /= $gn;
      return ($gv,$g_vdist);
    };
  }

  return $gb;
}


##==============================================================================
## Relation API: default: query info

## \%qinfo = $rel->qinfo($coldb, %opts)
##  + get query-info hash for profile administrivia (ddc hit links)
##  + %opts: as for profile(), additionally:
##    (
##     #qreqs => \@qreqs,      ##-- as returned by $coldb->parseRequest($opts{query})
##     #gbreq => \%groupby,    ##-- as returned by $coldb->groupby($opts{groupby})
##    )
##  + returned hash \%qinfo should have keys:
##    (
##     fcoef => $fcoef,         ##-- frequency coefficient (2*$coldb->{dmax} for CoFreqs)
##     qtemplate => $qtemplate, ##-- query template with __W1.I1__ rsp __W2.I2__ replacing groupby fields
##    )
sub qinfo {
  my ($vs,$coldb,%opts) = @_;

  ##-- parse item1 query & options
  my $q1 = $opts{qobj} ? $opts{qobj}->clone : $coldb->parseQuery($opts{query}, logas=>'qinfo', default=>'', ddcmode=>1);
  my ($qo);
  $q1->setOptions($qo=DDC::XS::CQueryOptions->new) if (!defined($qo=$q1->getOptions));
  my $q1str = $q1->toString.' =1';

  ##-- item2 query (via groupby, lifted from Relation::qinfoData())
  my $xi = 1;
  my $qf = $qo->getFilters // [];
  my $q2str = '';
  foreach (@{$opts{groupby}{areqs}}) {
    if ($_->[0] =~ /^doc\.(.*)/) {
      push(@$qf, DDC::XS::CQFHasField->new($1,"__W2.${xi}__"));
    }
    else {
      $q2str .= ' WITH ' if ($q2str);
      $q2str .= DDC::XS::CQTokExact->new($_->[0],"__W2.${xi}__")->toString;
    }
    ++$xi;
  }
  $q2str ||= '*';
  $q2str .= ' =2';

  ##-- options: set filters, WITHIN
  $qo->setFilters($qf);
  (my $inbreak = $coldb->{vbreak}) =~ s/^#//;

  ##-- construct query
  my $qtemplate = ("$q1str && $q2str "
		   .$qo->toString
		   ." #IN $inbreak"
		  );
  return {
	  fcoef => 1,
	  qtemplate => $qtemplate,
	 };
}

##==============================================================================
## Footer
1;

__END__
