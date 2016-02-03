#!/usr/bin/perl -w

use lib qw(. dclib);
use DiaColloDB;
use DiaColloDB::Utils qw(:sort :regex);
use DiaColloDB::Relation::Vsem;
use PDL;
use PDL::CCS;
use Data::Dumper;
use Benchmark qw(timethese cmpthese);
use Getopt::Long qw(:config no_ignore_case);
use utf8;
use strict;

use Inline 'Pdlpp';  ##-- see __Pdlpp__ block
use Inline Pdlpp => Config => (
			       #directory => './inline',
			       clean_after_build => 0,
			      );

BEGIN {
  select(STDERR); $|=1; select(STDOUT); $|=1;
  binmode(STDOUT,':utf8');

  no warnings 'once';
  $PDL::BIGPDL=1;
}

##======================================================================
## command-line
my ($help);
my $iters = 1;
my $dotest = 1;
my $qfile = undef;
my %qopts = (
	     slice => 10,
	    );

GetOptions(
	   'help|h' => \$help,
	   'date-slice|ds|sl=i' => \$qopts{slice},
	   'iters|i|count|c=i' => \$iters,
	   'test|t!' => \$dotest,
	   'query-file|qfile|qf|file|f=s' => \$qfile,
	  );
if ($help || @ARGV < 1) {
  print STDERR <<EOF;

Usage: $0 [OPTIONS] DBDIR QSTR...

Options:
  -slice SLICE
  -iters COUNT

EOF
  exit $help ? 0 : 1;
}

##======================================================================
## debug messages
use File::Basename;
our $prog = basename($0);

sub vmsg0 {
  print STDERR @_;
}
sub vmsg {
  vmsg0("$prog: ", @_, "\n");
}

##======================================================================
## test pointer-extraction

sub test_ptr {
  my $dbdir = shift;

  DiaColloDB->ensureLog();
  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  my $vs    = $coldb->{vsem};
  my $tdm   = $vs->{tdm};
  my $ix0   = $tdm->_whichND->slice("(0),");
  my $N0    = $tdm->dim(0);

  ##-- test
  my ($ptr0,$ixix0) = ccs_encode_pointers($ix0, $N0);
  my $ptr_r  = ptr0_rle($ix0,$N0);
  my $ptr_pp = $ix0->_ccs_encode_pointer0($N0+1);
  print STDERR "test:ptr_r: ", (all($ptr_r==$ptr0) ? "ok" : "NOT ok"), "\n";
  print STDERR "test:ptr_pp: ", (all($ptr_pp==$ptr0) ? "ok" : "NOT ok"), "\n";

  ##-- bench
  my ($ptr,$ixix);
  cmpthese($iters,
	   {
	    'ccs_encode_pointers' => sub { ($ptr,$ixix) = ccs_encode_pointers($ix0, $N0); },
	    'ptr0_rle' => sub { $ptr = ptr0_rle($ix0,$N0); },
	    'ptr0_pp' => sub { $ptr = $ix0->_ccs_encode_pointer0($N0+1); },
	   });
  ##--
  #                       Rate         ptr0_rle ccs_encode_pointers          ptr0_pp
  # ptr0_rle            4.10/s               --                -19%             -70%
  # ccs_encode_pointers 5.08/s              24%                  --             -63%
  # ptr0_pp             13.6/s             233%                168%               --
}
#test_ptr(@ARGV);

sub ptr0_rle {
  my ($ix,$N) = @_;
  my ($lens,$vals) = $ix->rle();
  $lens->gt(0, my $mask=zeroes(byte,$lens->nelem), 0);
  my $ngood = $mask->dsumover;
  $lens = $lens->slice("0:".($ngood-1));
  $vals = $vals->slice("0:".($ngood-1));
  my $ptr = zeroes($ix->type, $N+1);
  (my $tmp=$ptr->slice("1:-1")->index($vals)) .= $lens;
  return $ptr->cumusumover;
}

##======================================================================
## test xsubset: guts

## ($vq,$ti,$di) = xsubset_indices($coldb,$qstr)
sub xsubset_indices {
  my ($coldb,$qstr) = @_;

  my $logLocal = 'debug';
  my $vs = $coldb->{vsem};

  ##-- parse query string: hacks
  my $opts = {%qopts};
  if ($qstr =~ s/\#(?:slice|sl|ds)\s*[\[\s]([0-9]+)\]?//) {
    $opts->{slice} = $1;
  }

  ##-- parse query string
  my $q  = $coldb->parseQuery($qstr, logas=>'query', default=>'', ddcmode=>1);
  $q->setOptions(DDC::XS::CQueryOptions->new) if (!defined($q->getOptions));

  ##-- parse & compile query
  my %vqopts = (%$opts,coldb=>$coldb,vsem=>$vs);
  my $vq     = $opts->{vq} = DiaColloDB::Relation::Vsem::Query->new($q)->compile(%vqopts);

  ##-- evaluate query by slice (1st only)
  my $sliceVal = ($opts->{slice} ? ($vq->{slices}->at(0)) : 0);

  ##-- vpslice()
  my $ti  = $vq->{ti} // sequence($vs->itype, $vs->nTerms);
  my $ci  = $vq->sliceCats($sliceVal,%vqopts) // sequence($vs->itype, $vs->nCats);
  my $tdm = $vs->{tdm};

  ##-- xsubset: both term- and document-conditions
  my $q_c2d     = $vs->{c2d}->dice_axis(1,$ci);
  my $di        = $q_c2d->slice("(1),")->rldseq($q_c2d->slice("(0),"))->qsort;
  my $subsize   = $ti->nelem * $di->nelem;
  $vs->vlog($logLocal, "requested subset size = $subsize (Nt=".$ti->nelem." x Nd=".$di->nelem.")");

  $vq->{slice} = $opts->{slice} // 0;
  return ($vq,$ti,$di);
  #my $q_tdm     = $tdm->xsubset2d($ti,$di);
  #return $q_tdm;
}

##======================================================================
## test xsubset: MAIN

sub test_xsubset {
  my $dbdir = shift;

  DiaColloDB->ensureLog();
  my $coldb = DiaColloDB->new(dbdir=>$dbdir) or die("$0: failed to open $dbdir/: $!");
  my $vs    = $coldb->{vsem};
  my $tdm   = $vs->{tdm};
  my $ptr0  = $tdm->_whichND->slice("(0),")->_ccs_encode_pointer0($tdm->dim(0)+1);
  my $rows0 = $tdm->_whichND->slice("(1),");
  my ($ptr1,$ixix1) = $tdm->ptr(1);
  my $rows1 = $tdm->_whichND->slice("(0),")->index($ixix1);
  #$rows1->sever;

  foreach my $qstr (@_) {
    my ($vq,$ti,$di) = xsubset_indices($coldb,$qstr);
    $coldb->debug("BENCH[slice=$vq->{slice},iters=$iters]: $qstr");
    my $nab = pdl(double, [($ti->nelem*$di->nelem), $tdm->_nnz])->min;

    ##-- info
    $coldb->info("NT=".$tdm->dim(0)
		 ." ND=".$tdm->dim(1)
		 ." Nnz=".$tdm->_nnz
		 ." ptr=".$ptr0->nelem."~[".join(':',$ptr0->minmax)."]"
		 ." row=".$rows0->nelem."~[".join(':',$rows0->minmax)."]"
		 ." Nti=".$ti->nelem
		 ." Ndi=".$di->nelem
		);

    ##-- tests
    if ($dotest) {
      vmsg("test: xindex2d");
      $tdm->_whichND->_ccs_xindex2d_int($ti,$di, my $xi0=zeroes($vs->itype, $nab), my $nxi0=zeroes($vs->itype,1));
      $xi0->reshape($nxi0->sclr);
      ##
      vmsg("test: ptr_xindex2d");
      $ptr0->_ptr_xindex2d($rows0, $ti,$di, my $xip=zeroes($vs->itype, $nab), my $nxip=zeroes($vs->itype,1));
      $xip->reshape($nxip->sclr);
      print STDERR "test:_ptr_xindex2d: ", (all($nxi0==$nxip) && all($xi0==$xip) ? "ok" : "NOT ok"), "\n";
      ##
      vmsg("test: iptr_xindex2d");
      $ptr1->_ptr_xindex2d($rows1, $di,$ti, my $xiq=zeroes($vs->itype, $nab), my $nxiq=zeroes($vs->itype,1));
      $xiq->reshape($nxiq->sclr);
      print STDERR "test:_iptr_xindex2d: ", (all($nxi0==$nxiq) && all($xi0==$ixix1->index($xiq)->qsort) ? "ok" : "NOT ok"), "\n";
      ##
      vmsg("test: flat_xindex2d");
      $tdm->_whichND->_flat_xindex2d($ti,$di, my $xif=zeroes($vs->itype, $nab), my $nxif=zeroes($vs->itype,1));
      $xif->reshape($nxif->sclr);
      print STDERR "test:_flat_xindex2d: ", (all($nxi0==$nxif) && all($xi0==$xif) ? "ok" : "NOT ok"), "\n";
    }

    ##-- bench
    my ($xi,$nxi);
    cmpthese($iters,
	     {
	      ccs => sub { $tdm->_whichND->_ccs_xindex2d_int($ti,$di, $xi=zeroes($vs->itype, $nab), $nxi=zeroes($vs->itype,1)); },
	      ptr => sub { $ptr0->_ptr_xindex2d($rows0, $ti,$di, $xi=zeroes($vs->itype, $nab), $nxi=zeroes($vs->itype,1)); },
	      iptr => sub { $ptr1->_ptr_xindex2d($rows1, $di,$ti, $xi=zeroes($vs->itype, $nab), $nxi=zeroes($vs->itype,1)); },
	      flat => sub { $tdm->_whichND->_flat_xindex2d($ti,$di, my $xif=zeroes($vs->itype, $nab), my $nxif=zeroes($vs->itype,1)); },
	     });
    print "--\n";
  }
}

##-- read query file?
if ($qfile) {
  open(my $fh,"<$qfile") or die("$0: open failed for query-file '$qfile': $!");
  push(@ARGV, grep {($_//'') ne ''} map {chomp; s/^\s*$//; $_} <$fh>);
  close($fh);
}

push(@ARGV, 'V* !#has[textClass,/politik/i]') if (@ARGV < 2);
push(@ARGV, 'Mann') if (@ARGV < 2);
test_xsubset(@ARGV);



##======================================================================
## inline pdlpp code

__DATA__

__Pdlpp__

use PDL;
use PDL::VectorValued::Dev;
no warnings 'uninitialized';
my $INDX  = PDL->can('indx') ? "indx" : "int";
my $CINDX = PDL->can('indx') ? "PDL_Indx" : "PDL_Long";
if (!PDL->can('indx')) {
  pp_addhdr("\n#define PDL_Indx PDL_Long\n");
}
pp_addhdr(<<EOH);

typedef PDL_Indx CCS_Indx;

/*#define XINDEX_DEBUG 1*/

#ifdef XINDEX_DEBUG
# define XIDEBUG(x) x
#else
# define XIDEBUG(x)
#endif

EOH

##----------------------------------------------------------------------
## _ptr0 : optimized ccs_encode_pointers() for pre-sorted input
pp_def('_ccs_encode_pointer0',
       Pars => "$INDX\ ix(Nnz); $INDX\ [o]ptr(Nplus1);",
       OtherPars => 'int N1=>Nplus1;',
       Code =>
q{
   CCS_Indx nzi=0;
   loop (Nplus1) %{
     $ptr() = nzi;
     while (nzi < $SIZE(Nnz) && $ix(Nnz=>nzi)==Nplus1) ++nzi;
   %}
},
       );


##----------------------------------------------------------------------
## _ptr_xindex2d : xsubset2d via ptr(0)
vvpp_def('_ptr_xindex2d',
	 Pars => ("\n    "
		  .join("\n    ",
			"ptr(Mplus1); rowids(Nnz);", ##-- logical (M,N)~(T,D) with ptr(0)
			"a(Na); b(Nb);",             ##-- logical mask (Na~M * Nb~N)
			"[o]ab(Nab);",               ##-- nz-indices for non-missing values in logical mask (a() x b())
			"[o]nab();",                 ##-- number of non-missing values in ab()
		       )),
	 Code =>
q{
   CCS_Indx am,am1, nzi_min,nzi_max, nzi_lb;
   CCS_Indx abi = 0;

   XIDEBUG(fprintf(stderr, "_ptr_xindex2d(Mplus1=%d, Nnz=%d, Na=%d, Nb=%d, Nab_max=%d)\n", $SIZE(Mplus1), $SIZE(Nnz), $SIZE(Na), $SIZE(Nb), $SIZE(Nab)); fflush(stderr);)
   loop (Na) %{
     am      = $a();
     am1     = am+1;
     nzi_min = $ptr(Mplus1=>am);
     nzi_max = $ptr(Mplus1=>am1);

     loop (Nb) %{
       $LB('$b()', '$rowids(Nnz=>$_)', 'nzi_min','nzi_max', 'nzi_lb');
       if (nzi_lb < nzi_max && $rowids(Nnz=>nzi_lb) == $b()) {
         $ab(Nab=>abi) = nzi_lb;
#ifdef XINDEX_DEBUG
	 if (nzi_lb==17990925) {
           fprintf(stderr, "BUG: nzi_lb=%d : a=%d ; am=%d ; am1=%d ; nzi_min=%d ; nzi_max=%d\n", nzi_lb, $a(), am, am1, nzi_min, nzi_max); fflush(stderr);
         }
#endif
         ++abi;
         nzi_min = nzi_lb + 1;
         if (abi >= $SIZE(Nab)) break;
       }
     %}
     if (abi >= $SIZE(Nab)) break;
   %}
   $nab() = abi;
   for ( ; abi < $SIZE(Nab); ++abi) {
     $ab(Nab=>abi) = -1;
   }
},
	);
##--/_ptr_xindex2d()

##----------------------------------------------------------------------
## _flat_xindex2d : xsubset2d, flat linear traversal
vvpp_def('_flat_xindex2d',
	 Pars => ("\n    "
		  .join("\n    ",
			"which(Two,Nnz);",           ##-- logical (M,N)~(T,D)
			"a(Na); b(Nb);",             ##-- logical mask (Na~M * Nb~N)
			"[o]ab(Nab);",               ##-- nz-indices for non-missing values in logical mask (a() x b())
			"[o]nab();",                 ##-- number of non-missing values in ab()
		       )),
	 Code =>
q{
   CCS_Indx ai=0,bi=0,nzi=0,abi=0;
   $GENERIC(which) av,bv;

   XIDEBUG(fprintf(stderr, "_flat_xindex2d(Two=%d, Nnz=%d, Na=%d, Nb=%d, Nab_max=%d)\n", $SIZE(Two), $SIZE(Nnz), $SIZE(Na), $SIZE(Nb), $SIZE(Nab)); fflush(stderr);)
   if ($SIZE(Two) != 2) {
     croak("_flat_index2d(): bogus input dimension Two=%ld for index-piddle which(Two,Nnz) must be 2", $SIZE(Two));
   }

   while (nzi<$SIZE(Nnz) && ai<$SIZE(Na) && bi<$SIZE(Nb) && abi<$SIZE(Nab)) {
     XIDEBUG(fprintf(stderr, "LOOP nzi=%d~[%d,%d], ai=%d~[%d], bi=%d~[%d], abi=%d\n", nzi, $which(Two=>0,Nnz=>nzi),$which(Two=>1,Nnz=>nzi), ai,$a(Na=>ai), bi,$b(Nb=>bi), abi); fflush(stderr);)

     //-- increment which
     av = $a(Na=>ai);
     bv = $b(Nb=>bi);
     while (nzi<$SIZE(Nnz) && $which(Two=>0,Nnz=>nzi) < av) ++nzi;
     while (nzi<$SIZE(Nnz) && $which(Two=>0,Nnz=>nzi)==av && $which(Two=>1,Nnz=>nzi) < bv) ++nzi;

     //-- check for match
     if ($which(Two=>0,Nnz=>nzi)==av && $which(Two=>1,Nnz=>nzi)==bv) {
       XIDEBUG(fprintf(stderr, "LOOP/MATCH nzi=%d~[%d,%d], ai=%d~[%d], bi=%d~[%d], abi=%d\n", nzi, $which(Two=>0,Nnz=>nzi),$which(Two=>1,Nnz=>nzi), ai,$a(Na=>ai), bi,$b(Nb=>bi), abi); fflush(stderr);)
       $ab(Nab=>abi) = nzi;
       ++abi;
       ++nzi;
     }

     //-- increment (a x b)
     XIDEBUG(fprintf(stderr, "LOOP/AB nzi=%d~[%d,%d], ai=%d~[%d], bi=%d~[%d], abi=%d\n", nzi, $which(Two=>0,Nnz=>nzi),$which(Two=>1,Nnz=>nzi), ai,$a(Na=>ai), bi,$b(Nb=>bi), abi); fflush(stderr);)
     av = $which(Two=>0,Nnz=>nzi);
     bv = $which(Two=>1,Nnz=>nzi);
     while (ai<$SIZE(Na) &&  $a(Na=>ai) < av) { ++ai; bi=0; }
     while (ai<$SIZE(Na) && ($a(Na=>ai) < av || ($a(Na=>ai)==av && $b(Nb=>bi) < bv))) {
       if (++bi >= $SIZE(Nb)) {
         ++ai;
         bi = 0;
       } else {
         while (bi<$SIZE(Nb) && $b(Nb=>bi) < bv) ++bi;
         if (bi >= $SIZE(Nb)) {
           ++ai;
           bi = 0;
         }
       }
     }

     XIDEBUG(fprintf(stderr, "LOOP/END nzi=%d~[%d,%d], ai=%d~[%d], bi=%d~[%d], abi=%d\n", nzi, $which(Two=>0,Nnz=>nzi),$which(Two=>1,Nnz=>nzi), ai,$a(Na=>ai), bi,$b(Nb=>bi), abi); fflush(stderr);)
   }

   //-- fill unused elements with -1
   $nab() = abi;
   for ( ; abi < $SIZE(Nab); ++abi) {
     $ab(Nab=>abi) = -1;
   }
},
	);
##--/_flat_xindex2d()

