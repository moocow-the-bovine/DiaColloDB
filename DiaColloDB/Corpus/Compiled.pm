## -*- Mode: CPerl -*-
## File: DiaColloDB::Corpus::Compiled.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, source corpus (pre-compiled)

package DiaColloDB::Corpus::Compiled;

use threads;
use threads::shared;

use DiaColloDB::Corpus;
use DiaColloDB::Logger;
use DiaColloDB::Utils qw(:fcntl);
use File::Basename qw(basename dirname);
use File::Path qw(make_path remove_tree);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Persistent DiaColloDB::Corpus);

##==============================================================================
## Constructors etc.

## $corpus = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- NEW in DiaColloDB::Corpus::Compiled
##    base    => $basename,  ##-- basename of compiled corpus
##    flags   => $flags,     ##-- open mode flags (fcntl flags or perl-style; default='r')
##    filters => \%filters,  ##-- corpus filters ( keys match /^(p|w|l)(good|bad)(_file)?$/ )
##    njobs   => $njobs,     ##-- number of parallel worker jobs for create()
##    temp    => $bool,      ##-- implicitly unlink() on exit?
##    logThreads => $level   ##-- log-level for thread stuff (default='debug')
##    ##
##    ##-- INHERITED from DiaColloDB::Corpus
##    files => \@files,      ##-- source files
##    #dclass => $dclass,     ##-- DiaColloDB::Document subclass for loading (override default='DiaColloDB::Document::JSON')
##    dopts  => \%opts,      ##-- options for $dclass->fromFile() (override default={})
##    cur    => $i,          ##-- index of current file
##    logOpen => $level,     ##-- log-level for open(); default='info'
##   )
sub new {
  my $that = shift;
  my $corpus  = $that->SUPER::new(
                                  ##-- new
                                  base   => undef,
                                  flags  => 'r',
                                  filters => {},
                                  #temp    => 0,
                                  opened => 0,
                                  logThreads => 'debug',

                                  @_, ##-- user arguments

                                  ##-- strong overrides
                                  dclass => 'DiaColloDB::Document::JSON',
                                 );
  return $corpus->open() if (defined($corpus->{base}));
  return $corpus;
}

sub DESTROY {
  my $obj = $_[0];
  $obj->unlink() if ($obj->{temp});
  $obj->close() if ($obj->opened);
}

##==============================================================================
## Persistent API

## @keys = $obj->headerKeys()
##  + keys to save as header; default implementation returns all keys of all non-references
sub headerKeys {
  return (grep {$_ !~ m{^(?:log|cur$|base$|njobs$|opened$)}} keys %{$_[0]});
}

## $bool = $obj->unlink()
##  + unlinks disk files
##  + implcitly calls $obj->close() if available
sub unlink {
  my $obj = shift;
  my $base = $obj->{base};
  $obj->close();
  CORE::unlink("$base.hdr") && File::Path::remove_path("$base.d");
}

##==============================================================================
## Corpus API: open/close

## $bool = $corpus->open([$base], %opts);  ##-- compat
## $bool = $corpus->open($base,   %opts);  ##-- new
##  + opens corpus "$base.*"
##  + \@ARGV should be a single-element $base, or (base=>$base) must exist or be specified in %opts
##  + DiaColloDB::Corpus %opts:
##     glob => $bool,     ##-- (ignored) whether to glob arguments
##     list => $bool,     ##-- (ignored) whether arguments are file-lists
sub open {
  my ($corpus,$argv,%opts) = @_;
  $corpus  = $corpus->new() if (!ref($corpus));
  $corpus->close() if ($corpus->opened);
  @$corpus{keys %opts} = values(%opts);

  ##-- sanity check(s): base
  if (UNIVERSAL::isa($argv,'ARRAY') && @$argv==1) {
    $corpus->{base} = $argv->[0]; ##-- single-element list
  } elsif (defined($argv) && !ref($argv)) {
    $corpus->{base} = $argv;      ##-- simple scalar
  }
  $corpus->logconfess("open() method requires a single source basename")
    if (!$corpus->{base});
  my $base = $corpus->{base};
  $corpus->vlog($corpus->{logOpen}, "open($base.*)");

  my $flags = $corpus->{flags} = fcflags($corpus->{flags});
  return undef if (fcread($flags) && !$corpus->loadHeaderFile);

  ##-- force document-class
  $corpus->{dclass} = 'DiaColloDB::Document::JSON';

  return $corpus;
}

## $bool = $corpus->close()
sub close {
  my $corpus = shift;
  my $rc = ($corpus->opened && fcwrite($corpus->{flags}) ? $corpus->flush : 1);
  $rc &&= $corpus->SUPER::close();
  $corpus->{opened} = 0 if ($rc);
  return $corpus;
}

##----------------------------------------------------------------------
## Compiled API: open/close

## $bool = $corpus->opened()
sub opened {
  my $corpus = shift;
  return $corpus->{base} && $corpus->{opened};
}

## $bool = $corpus->flush()
sub flush {
  my $corpus = shift;
  return undef if (!$corpus->opened || !fcwrite($corpus->{flags}));
  $corpus->saveHeader()
    or $corpus->logconfess("flush(): failed to store header file ", $corpus->headerFile, ": $!");
}

## $corpus = $corpus->reopen(%opts)
sub reopen {
  my $corpus = shift;
  my $base   = $corpus->{base};
  return $corpus if (!$corpus->opened);
  return $corpus->close() && $corpus->open([$base], @_);
}

##==============================================================================
## Corpus API: iteration
##  + inherited from DiaColloDB::Corpus

##==============================================================================
## Corpus::Compiled API: corpus compilation

## $ccorpus = CLASS_OR_OBJECT->create($src_corpus, %opts)
##  + compile $src_corpus to $opts{base}, returns new object
sub create {
  my ($that,$icorpus,%opts) = @_;
  my $ocorpus = ref($that) ? $that : $that->new();
  my $logas = 'create()';

  ##-- open output corpus
  my $obase = $opts{base}
    or $ocorpus->logconfess("$logas: no output corpus {base} specified");
  $ocorpus->close()
    or $ocorpus->logconfess("$logas: failed to close stale corpus");
  @$ocorpus{keys %opts} = values %opts;

  ##-- remove output directory
  my $outdir = "$obase.d";
  !-d $outdir
    or remove_tree($outdir)
    or $ocorpus->logconfess("$logas: could not remove old data directory '$outdir': $!");

  ##-- create output directory
  -d $outdir
    or make_path($outdir)
    or $ocorpus->logconfess("$logas: could not create data directory '$outdir': $!");

  ##-- do any filtering at all?
  my $filters  = $ocorpus->{filters};
  my $dofilter = grep {defined($_)} values %$filters;
  if ($dofilter) {
    $ocorpus->vlog('info', "$logas: corpus content filters enabled");
    foreach (grep {defined($filters->{$_})} sort keys %$filters) {
      $ocorpus->vlog('info', "  + filter $_ => $filters->{$_}");
    }
  } else {
    $ocorpus->vlog('info', "$logas: corpus content filters disabled");
  }

  ##-- common data
  my $nfiles   = $icorpus->size();
  my $logFileN = $ocorpus->{logFileN} || int($nfiles / 20) || 1;
  my $filei_shared = 0;
  my @outkeys  = keys %{DiaColloDB::Document->new};
  my (@outfiles);
  share( $filei_shared );
  share( @outfiles );

  ##--------------------------------------------
  my $cb_worker = sub {
    my $thrid = shift || threads->tid();
    $logas .= "#$thrid";
    (*STDERR)->autoflush(1);
    $ocorpus->vlog($ocorpus->{logThreads}, "$logas: starting worker thread #$thrid");

    ##-- initialize filters (formerly in DiaColloDB.pm)
    ##
    ##-- initialize: filter regexes
    my $pgood = $filters->{pgood} ? qr{$filters->{pgood}} : undef;
    my $pbad  = $filters->{pbad}  ? qr{$filters->{pbad}}  : undef;
    my $wgood = $filters->{wgood} ? qr{$filters->{wgood}} : undef;
    my $wbad  = $filters->{wbad}  ? qr{$filters->{wbad}}  : undef;
    my $lgood = $filters->{lgood} ? qr{$filters->{lgood}} : undef;
    my $lbad  = $filters->{lbad}  ? qr{$filters->{lbad}}  : undef;
    ##
    ##-- initialize: filter lists
    my $pgoodh = DiaColloDB->loadFilterFile($filters->{pgoodfile});
    my $pbadh  = DiaColloDB->loadFilterFile($filters->{pbadfile});
    my $wgoodh = DiaColloDB->loadFilterFile($filters->{wgoodfile});
    my $wbadh  = DiaColloDB->loadFilterFile($filters->{wbadfile});
    my $lgoodh = DiaColloDB->loadFilterFile($filters->{lgoodfile});
    my $lbadh  = DiaColloDB->loadFilterFile($filters->{lbadfile});
    ##
    ##-- initialize: filter loop variables
    my ($tok,$w,$p,$l);

    my ($filei);
    while (1) {
      {
        lock($filei_shared);
        $filei = $filei_shared;
        ++$filei_shared;
      }
      last if ($filei >= $nfiles);

      my $idoc    = $icorpus->idocument($filei);
      my $infile  = $icorpus->{files}[$filei];
      my $outfile = "$outdir/$filei.json";

      #$ocorpus->vlog('info', sprintf("processing files [%3.0f%%]: %s -> %s", 100*($filei-1)/$nfiles, $infile, $outfile))
      $ocorpus->vlog('info', sprintf("%s: processing files [%3.0f%%]: %s", $logas, 100*($filei-1)/$nfiles, $infile))
        if ($logFileN && ($filei % $logFileN)==0);

      ##-- buffer list output, sanity check(s)
      {
        lock(@outfiles);
        $outfiles[$filei] = $outfile;
      }

      ##-- apply filters
      if ($dofilter) {
        my $ftokens = [];
        foreach $tok (@{$idoc->{tokens}}) {
          if (ref($tok)) {
            ##-- normal token
            ($w,$p,$l) = @$tok{qw(w p l)};

            ##-- apply regex filters
            next if ((defined($pgood)    && $p !~ $pgood) || ($pgoodh && !exists($pgoodh->{$p}))
                     || (defined($pbad)  && $p =~ $pbad)  || ($pbadh  &&  exists($pbadh->{$p}))
                     || (defined($wgood) && $w !~ $wgood) || ($wgoodh && !exists($wgoodh->{$w}))
                     || (defined($wbad)  && $w =~ $wbad)  || ($wbadh  &&  exists($wbadh->{$w}))
                     || (defined($lgood) && $l !~ $lgood) || ($lgoodh && !exists($lgoodh->{$l}))
                     || (defined($lbad)  && $l =~ $lbad)  || ($lbadh  &&  exists($lbadh->{$l}))
                    );
          }
          push(@$ftokens,$tok) if (defined($tok) || (@$ftokens && defined($ftokens->[$#$ftokens])));
        }
        $idoc->{tokens} = $ftokens;
      }

      ##-- create output document
      my $odoc = {};
      @$odoc{@outkeys} = @$idoc{@outkeys};

      ##-- dump output document (json)
      DiaColloDB::Utils::saveJsonFile($odoc,$outfile, pretty=>0,canonical=>0)
          or $ocorpus->logconfess("$logas: failed to save JSON data for '$infile' to '$outfile': $!");
    }

    $ocorpus->vlog($ocorpus->{logThreads}, "$logas: worker thread #$thrid exiting normally");
  };
  ##--/cb_worker

  ##-- spawn workers
  my $njobs = $ocorpus->{njobs} // 0;
  if ($njobs==0) {
    $ocorpus->info("$logas: running in serial mode");
    $cb_worker->(0);
  } else {
    $ocorpus->info("$logas: running in parallel mode with $njobs job(s)");
    my @workers = (map {threads->new($cb_worker,$_)} (1..$njobs));
    foreach my $thrid (1..$njobs) {
      my $worker = $workers[$thrid-1];
      $worker->join();
      if (defined(my $err=$worker->error)) {
        $ocorpus->logconfess("$logas: error for worker thread #$thrid: $err");
      }
    }
  }

  ##-- adopt list of compiled files
  $ocorpus->{files} = \@outfiles;

  ##-- save header
  $ocorpus->saveHeader()
    or $ocorpus->logconfess("$logas: failed to save header file ", $ocorpus->headerFile, ": $!");

  return $ocorpus;
}



##==============================================================================
## Footer
1;

__END__




