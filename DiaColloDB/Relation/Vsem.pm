## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::Vsem.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: vector-space semantic model (native)

package DiaColloDB::Relation::Vsem;
use DiaColloDB::Relation;
use DiaColloDB::Relation::Vsem::Query;
use DiaColloDB::Profile::Pdl;
use DiaColloDB::Profile::PdlDiff;
use DiaColloDB::Utils qw(:fcntl :file :math :json :list :pdl);
use DiaColloDB::PDL::MM;
use File::Path qw(make_path remove_tree);
use PDL;
use PDL::IO::FastRaw;
use PDL::CCS;
use PDL::CCS::IO::FastRaw;
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
##   minFreq    => $fmin,   ##-- minimum total term-frequency for model inclusion (default=10)
##   minDocFreq => $dfmin,  ##-- minimim "doc-frequency" (#/docs per term) for model inclusion (default=
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
			       minFreq => 10,
			       minDocFreq => 5,
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
##  + implementation used (temporary, tied) doc-array $coldb->{doctmpa}
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
  my $doctmpa = $coldb->{doctmpa};
  my $base    = $vs->{base};
  my $logCreate = $vs->{logCreate} // $coldb->{logCreate} // 'trace';
  $vs->logconfess("create(): no source document array in parent DB") if (!UNIVERSAL::isa($coldb->{doctmpa},'ARRAY'));
  $vs->logconfess("create(): no 'base' key defined") if (!$base);

  ##-- initialize: output directory
  my $vsdir = "$base.d";
  $vsdir =~ s{/$}{};
  !-d $vsdir
    or remove_tree($vsdir)
      or $vs->logconfess("create(): could not remove stale $vsdir: $!");
  make_path($vsdir)
    or $vs->logconfess("create(): could not create Vsem directory $vsdir: $!");

  ##-- initialize: logging
  my $nfiles    = scalar(@$doctmpa);
  my $logFileN  = $coldb->{logCorpusFileN} // max2(1,int($nfiles/10));

  ##-- initialize: metadata
  my %meta = qw(); ##-- ( $meta_attr => {n=>$nkeys, s2i=>\%s2i, vals=>$pdl}, ... )
  my $mgood = $vs->{mgood} ? qr{$vs->{mgood}} : undef;
  my $mbad  = $vs->{mbad}  ? qr{$vs->{mbad}}  : undef;

  ##-- create temp files tdm0.ix.tmp, tdm0.nz.tmp
  my $itype = $vs->itype;
  my $vtype = $vs->vtype;
  my $pack_ix = $PDL::Types::pack[ $itype->enum ];
  my $pack_nz = $PDL::Types::pack[ $vtype->enum ];
  my $ix0file = "$vsdir/tdm0.ix.tmp"; # pdl: ($nnz0,2):  [$nzi,*] => [$ti0,$di]
  my $nz0file = "$vsdir/tdm0.nz.tmp"; # pdl: ($nnz0+1):  [$nzi]   => $f_td
  my $t0file  = "$vsdir/t0.tmp";      # pdl: ($NA,$NT0): [*,$ti0] => [$ai0,$ai1,...,$aiN] # not sorted
  CORE::open(my $ix0fh, ">$ix0file")
      or $vs->logconfess("create(): failed to create tempfile $ix0file: $!");
  CORE::open(my $nz0fh, ">$nz0file")
      or $vs->logconfess("create(): failed to create tempfile $nz0file: $!");
  CORE::open(my $t0fh, ">$t0file")
      or $vs->logconfess("create(): failed to create tempfile $t0file: $!");

  ##-- create: simulate DocClassify::Mapper::trainCorpus(): populate tdm0.*
  $vs->vlog($logCreate, "create(): processing input documents [NC=$nfiles]");
  my $NA     = scalar(@{$coldb->{attrs}});
  my $NC     = $nfiles;
  my $c2date = $vs->{c2date} = mmzeroes("$vsdir/c2date.pdl", ushort, $NC);  ##-- c2date ($NC): [$ci]   -> $date
  my $c2d    = $vs->{c2d}    = mmzeroes("$vsdir/c2d.pdl", $itype, 2,$NC);   ##-- c2d  (2,$NC): [0,$ci] => $di_off, [1,$ci] => $di_len
  my $json   = DiaColloDB::Utils->jsonxs();
  my ($doc,$filei,$doclabel,$docid);
  my ($mattr,$mval,$mdata,$mvali,$mvals);
  my ($sig,$ts,$ti,$f);
  my ($tmp);
  my $tnull = join(' ',map {0} (1..$NA)); ##-- "null" term
  my $ts2i = {''=>0, $tnull=>0};
  $t0fh->print(pack($pack_ix, split(' ',$tnull)));
  my $NT0  = 1;
  my $sigi = 0;
  foreach $doc (@$doctmpa) {
    $doclabel = $doc->{meta}{basename} // $doc->{meta}{file_} // $doc->{label};
    $vs->vlog($coldb->{logCorpusFile}, sprintf("create(): processing signatures [%3.0f%%]: %s", 100*($filei-1)/$nfiles, $doclabel))
      if ($logFileN && ($filei++ % $logFileN)==0);

    $docid = $doc->{id} // ++$docid;

    #$vs->debug("c2date: id=$docid/$NC ; doc=$doclabel");
    $c2date->set($docid,$doc->{date});
    $c2d->set(0,$docid => $sigi);

    ##-- parse metadata
    #$vs->debug("meta: id=$docid/$NC ; doc=$doclabel");
    while (($mattr,$mval) = each %{$doc->{meta}//{}}) {
      next if ((defined($mgood) && $mattr !~ $mgood) || (defined($mbad) && $mattr =~ $mbad));
      $mdata = $meta{$mattr} = {n=>1, s2i=>{''=>0}, vals=>mmtemp("$vsdir/mvals_$mattr.tmp",$itype,$NC)} if (!defined($mdata=$meta{$mattr}));
      $mvali = ($mdata->{s2i}{$mval} //= $mdata->{n}++);
      $mdata->{vals}->set($docid,$mvali);
    }

    ##-- create temporary DocClassify::Document objects for each embedded signature
    #$vs->debug("sigs: id=$docid/$NC ; doc=$doclabel");
    foreach $sig (@{$doc->{sigs}}) {
      while (($ts,$f) = each %$sig) {
	if (!defined($ti=$ts2i->{$ts})) {
	  $ti = $ts2i->{$ts} = $NT0++;
	  $t0fh->print(pack($pack_ix, split(' ',$ts)));
	}
	$ix0fh->print(pack($pack_ix, $ti, $sigi));
	$nz0fh->print(pack($pack_nz, $f));
      }
      ++$sigi;
    }

    ##-- update c2d (length)
    $c2d->set(1,$docid => $sigi - $c2d->at(0,$docid));
  }
  ##-- create: tdm0: add missing value
  $nz0fh->print(pack($pack_nz, 0));
  $ix0fh->close() or $vs->logconfess("create(): close failed for tempfile $ix0file: $!");
  $nz0fh->close() or $vs->logconfess("create(): close failed for tempfile $nz0file: $!");
  $t0fh->close() or $vs->logconfess("create(): close failed for tempfile $t0file: $!");

  ##-- create: tdm0: map to piddles
  my $ND  = $sigi;
  my $nnz0 = (-s $nz0file) / length(pack($pack_nz,0)) - 1;
  my $density = $nnz0 / ($ND*$NT0);
  $vs->vlog($logCreate, "create(): mapping tdm0 data from tempfiles (ND=$ND, NT0=$NT0; density=", sprintf("%.2g%%", 100*$density), ")");
  defined(my $ix0 = mapfraw($ix0file,{ReadOnly=>1,Creat=>0,Trunc=>0,Dims=>[2,$nnz0],Datatype=>$itype}))
    or $vs->logconfess("create(): mapfraw() failed for $ix0file: $!");
  defined(my $nz0 = mapfraw($nz0file,{ReadOnly=>1,Creat=>0,Trunc=>0,Dims=>[$nnz0+1],Datatype=>$vtype}))
    or $vs->logconfess("create(): mapfraw() failed for $nz0file: $!");
  defined(my $t0 = mapfraw($t0file,{ReadOnly=>1,Creat=>0,Trunc=>0,Dims=>[$NA,$NT0],Datatype=>$itype}))
    or $vs->logconfess("create(): mapfraw() failed for $t0file: $!");

  ##-- create: tdm0: filter: by term-frequency
  my $t_mask = mmtemp("$vsdir/t_mask.tmp", byte, $NT0);
  $vs->{minFreq} //= 0;
  $vs->vlog($logCreate, "create(): filter: by term-frequency (minFreq=$vs->{minFreq})");
  if ($vs->{minFreq} > 0) {
    $nz0->slice("0:-2")->indadd($ix0->slice("(0),"), my $tf0=mmtemp("$vsdir/tf0.tmp", $vtype, $NT0));
    $tf0->ge($vs->{minFreq}, $t_mask, 0);
  } else {
    $t_mask .= 1;
  }

  ##-- create: tdm0: filter: by doc-frequency
  $vs->{minDocFreq} //= 0;
  $vs->vlog($logCreate, "create(): filter: by doc-frequency (minDocFreq=$vs->{minDocFreq})");
  if ($vs->{minDocFreq} > 0) {
    pdl($itype,1)->indadd($ix0->slice("(0),"), my $df=mmtemp("$vsdir/df.tmp", $itype, $NT0));
    $df->ge($vs->{minDocFreq}, (my $df_mask=mmtemp("$vsdir/df_mask.tmp", byte, $NT0)), 0);
    $t_mask &= $df_mask;
  }

  ##-- create: tdm0: filter: prune
  $t_mask->set(0=>1); ##-- always keep "null" term
  my $NT     = $t_mask->dsumover;
  my $pkeept = $NT/$NT0;
  $vs->vlog($logCreate,
	    sprintf("create(): filter: keeping %.2f%% original term types (%.2f%% pruned)", 100*$pkeept, 100*(1-$pkeept)));
  $t_mask->which(my $t1x=mmtemp("$vsdir/t1x.tmp", $itype, $NT));  	    ##-- $t1x  : pdl ($NT): [$ti] => $t0i
  $vs->debug("t1x->dims = ", join(' ', $t1x->dims));
  $vs->debug("tvals:create");
  my $tvals = $vs->{tvals} = mmzeroes("$vsdir/tvals.pdl", $itype, $NA,$NT); ##-- $tvals: pdl ($NA,$NT): [$ai,$ti] => $avali_for_term_ti : keep
  $vs->debug("tvals:dice_axis");
  $tvals   .= $t0->dice_axis(1, $t1x); ##-- output param -> error
  $vs->debug("tvals:undef t0");
  undef $t0;
  CORE::unlink $t0file;

  ##-- create: tdm0: convert to nz-mask : in-memory
  # ERROR: Usage:  PDL::index(a,ind,c) (you may leave temporaries or output variables out of list) at ...[/usr/share/perl/5.20/perl5db.pl:732] line 2.
  # ... even though that's how we're calling it; maybe this has something to do with the [a] qualifier on the index() output piddle?
  #$t_mask->index($ix0->slice("(0),"), my $nzi_mask=mmzeroes("$vsdir/nzi_mask.tmp", byte, $nnz0, {temp=>1})); ##-- output param -> error
  $vs->debug("nzi_mask");
  my $nzi_mask = $t_mask->index($ix0->slice("(0),"));
  my $nnz     = $nzi_mask->dsumover;
  my $pkeepv  = $nnz/$nnz0;
  $vs->vlog($logCreate, sprintf("create(): filter: keeping %.2f%% of original nz-values (%.2f%% pruned)", 100*$pkeepv, 100*(1-$pkeepv)));
  $nzi_mask->which( (my $nzi_which=mmtemp("$vsdir/nzi_which.tmp", PDL::CCS::ccs_indx(), $nnz+1))->slice("0:-2") );
  $nzi_which->set($nnz=>$nnz0);
  undef $nzi_mask;
  undef $t_mask;

  ##-- create: filter: create filtered $ix, $nz (still unsorted)
  (my $nz = mmtemp("$vsdir/tdm.nz.tmp", $vtype, $nnz+1)) .= $nz0->index($nzi_which);
  undef $nz0;
  CORE::unlink $nz0file;
  ##
  (my $ix = mmtemp("$vsdir/tdm.ix.tmp", $itype, 2,$nnz)) .= $ix0->dice_axis(1,$nzi_which->slice("0:-2"));
  undef $nzi_which;
  undef $ix0;
  CORE::unlink $ix0file;
  ##
  ##-- create: filter: map unfiltered to filtered term-ids
  $ix->slice("(0),")->vsearch($t1x, $ix->slice("(0),"));
  undef $t1x;

  ##-- create: tw (term-weights): max-entropy-quotient: see e.g. Berry(1995); Nakov, Popova, & Mateev (2001)
  $vs->vlog($logCreate, "create(): computing term weights");
  $nz->slice("0:-2")->indadd($ix->slice("(0),"),			##-- tf  :  pdl: [$ti] -> f($ti) : keep
			     my $tf=$vs->{tf}=mmzeroes("$vsdir/tf.pdl", $vtype, $NT));
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
  $vs->{tw} = $tw;

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
  $ix->qsortveci( (my $qsi = mmtemp("$vsdir/tdm_qsi.tmp", $itype, $nnz+1))->slice("0:-2") );	##-- get sort-indices
  $qsi->set($nnz=>$nnz);

  ##-- create: tfidf: ccs matrix
  defined(my $tdm = $vs->{tdm} = PDL::CCS::Nd->newFromWhich($ix->dice_axis(1,$qsi->slice("0:-2")),
							    $nz->index($qsi),
							    sorted=>1,
							    steal=>1,
							    pdims=>[$NT,$ND]))
    or $vs->logconfess("create(): failed to create PDL::CCS::Nd tfidf matrix: $!");

  ##-- create: tfidf: save
  writePdlFile($tdm, "$vsdir/tdm", %ioopts)
    or $vs->logconfess("creeate(): failed to store tfidf matrix data to $vsdir/tdm.*: $!");

  ##-- create: tfidf: cleanup
  undef $qsi;
  undef $ix;
  undef $nz;

  ##-- create: aux: tsorti
  $vs->vlog($logCreate, "create(): creating term-attribute sort-indices (NA=$NA x NT=$NT)");
  my $tsorti = $vs->{tsorti} = mmzeroes("$vsdir/tsorti.pdl", $itype, $NT,$NA); ##-- [,($apos)] => $tvals->slice("($apos),")->qsorti
  foreach (0..($NA-1)) {
    $tvals->slice("($_),")->qsorti($tsorti->slice(",($_)"));
  }
  ##
  $vs->{attrs} = $coldb->{attrs}; ##-- save local copy of attributes

  ##-- create: aux: d2c: [$di] => $ci
  $vs->vlog($logCreate, "create(): creating doc<->category translation piddles (ND=$ND, NC=$NC)");
  $c2d->slice("(1),")->rld(sequence($itype,$NC), my $d2c=$vs->{d2c}=mmzeroes("$vsdir/d2c.pdl",$itype,$ND));
  # $c2d: already created in main doc-processing loop, above

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
    ($tmp=$mvals->slice("($_),")) .= $mdata->{vals};
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

  ##-- create: aux: info
  $vs->{N} = $tf->sum;

  ##-- save: header
  $vs->vlog($logCreate, "create(): saving to $base*");
  $vs->saveHeader()
    or $vs->logconfess("create(): failed to save header data: $!");

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
## + really just wraps $rel->vprofile(), DiaColloDB::Profile::Pdl::toProfile(), and DiaColloDB::Profile::Multi::stringify()
sub profile {
  my ($vs,$coldb,%opts) = @_;

  ##-- vector-based profile
  my $pprfs     = $vs->vprofile($coldb,\%opts);
  my $groupby = $opts{groupby};

  ##-- construct multi-profile
  $vs->vlog($vs->{logvprofile}, "profile(): constructing output profile [strings=".($opts{strings} ? 1 : 0)."]");
  my $mp = DiaColloDB::Profile::Multi->new(profiles=>[map {$_->toProfile} @$pprfs], titles=>$groupby->{titles}, qinfo=>'TODO');
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
## Profile: Utils: Vector-based profiling

## \@pprfs = $vs->vprofile($coldb, \%opts)
## + get a relation profile for selected items as an ARRAY of DiaColloDB::Profile::Pdl objects
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
  my $q = $coldb->parseQuery($opts->{query}, logas=>'query', default=>'', ddcmode=>1);
  my ($qo);
  $q->setOptions($qo=DDC::XS::CQueryOptions->new) if (!defined($qo=$q->getOptions));
  #$qo->setFilters([@{$qo->getFilters}, @$gbfilters]) if (@$gbfilters);

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

  ##-- evaluate query by slice
  $vs->vlog($logLocal, "vprofile(): evaluating query by target slice");
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
    $vs->vlog($logLocal, "requested subset size = $subsize (NT=".$ti->nelem." x Nd=".$ci->nelem.")");
    if (defined($vs->{submax}) &&  $subsize > $vs->{submax}) {
      $vs->logconfess($coldb->{error}="requested subset size $subsize (NT=".$ti->nelem." x Nd=".$ci->nelem.") too large; max=$vs->{submax}");
    }

    my $q_tdm     = $tdm->xsubset2d($ti,$di);
    return $pprf if ($q_tdm->allmissing); ##-- empty subset --> null profile

    $vs->vlog($logLocal, "vpslice($sliceVal): query vector: decode");
    $q_tdm->_nzvals->indadd($q_tdm->_whichND->slice("(1),"),
			    $qvec=zeroes($q_tdm->type, $vs->nDocs));

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
    my $tdm_t = $tdm->_whichND->slice("(0),");
    my $nzi0  = vsearch($ti,   $tdm_t);
    my $nzi1  = vsearch($ti+1, $tdm_t);
    $nzi1->where(($nzi1==$tdm->_nnz-1) & ($tdm_t->index($nzi1)==$ti))++;       ##-- special case for final nz-value
    $nzi1    -= $nzi0;							       ##-- convert to lengths
    my $nzi = $nzi1->rldseq($nzi0);					       ##-- decode to nz-indices
    if (!$nzi->isempty) {
      $tdm->_nzvals->index($nzi)->indadd($tdm->_whichND->slice("(1),")->index($nzi),
					 $qvec=zeroes($tdm->type, $vs->nDocs));

      ##-- get f1
      $f1 = $tdm->_nzvals->index($nzi) / $vs->{tw}->index($tdm_t->index($nzi));
      $f1 *= log(2);
      $f1->inplace->exp;
      $f1 -= $vs->{smoothf};
      $f1  = $f1->sumover->rint;
    }
  }
  elsif (defined($ci)) {
    ##-- doc-conditions only: slice from $map->{xcm}
    $vs->vlog($logLocal, "vpslice($sliceVal): query vector: cats");
    my $q_c2d   = $vs->{c2d}->dice_axis(1,$ci);
    my $di      = $q_c2d->slice("(1),")->rldseq($q_c2d->slice("(0),"))->qsort;
    #my $nz_qsi  = $tdm->_whichND->slice("(1),")->qsorti();
    ##
    my $nz_ci  = $vs->{d2c}->index($tdm->_whichND->slice("(1),"));
    my $nz_qsi = $nz_ci->qsorti;
    my $nz_cix = $nz_ci->index($nz_qsi);
    my $nzi0   = $ci->vsearch($nz_cix);
    my $nzi1   = ($ci+1)->vsearch($nz_cix);
    $nzi1->where(($nzi1==$tdm->_nnz-1) & ($nz_cix->index($nzi1)==$ci))++; ##-- special case for final nz-value
    ##
    $nzi1 -= $nzi0;							  ##-- convert to lengths
    my $nzi = $nz_qsi->index($nzi1->rldseq($nzi0));			  ##-- decode & translate to original nz-indices
    ##
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

  ##-- sanity check: ensure we've got a query vector
  $vs->logconfess($coldb->{error}="empty query vector for user query \`$opts->{query}'") if (!defined($qvec) || !$qvec->nelem);

  ##-- map to & extract k-best terms
  $vs->vlog($logLocal, "vpslice($sliceVal): extracting k-nearest neighbors (terms)");
  my $sim = $tdm->vcos_zdd($qvec);						##-- cosine sim : [-1:1]
  $sim->inplace->setnantobad->inplace->setbadtoval(-1);				##-- ... map NaN=>$sim_min=2
  #(my $tmp=$sim->index($ti)) .= -1 if (defined($ti));				##-- ... eliminate target term(s)?  -->goofy for diff
  $sim = $sim->index($groupby->{ghaving}) if (defined($groupby->{ghaving}));	##-- ... apply group-restriction

  ##-- groupby: aggregate
  my ($g_keys,$g_sim) = $groupby->{gaggr}->($sim);

  ##-- convert distance to similarity: gaussian (distribution *does* look quite normal modulo long tail of "good" (latent) matches)
  #my $g_sim = ($g_sim)->gausscdf(0,0.25);
  ##$g_sim->inplace->minus(1,$g_sim,1);
  ##
  (my $noval = zeroes(float,$g_sim->nelem)) .= 'nan';
  @$pprf{qw(gkeys gvals f1 f2 f12)} = ($g_keys,$g_sim,$f1,$noval,$noval);
  return $pprf;
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
    return null->long;
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
##     qreqs => \@qreqs,      ##-- as returned by $coldb->parseRequest($opts{query})
##     gbreq => \%groupby,    ##-- as returned by $coldb->groupby($opts{groupby})
##    )
##  + TODO

##==============================================================================
## Footer
1;

__END__
