## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::DDC.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: ddc client (using DDC::Client::Distributed)

package DiaColloDB::Relation::DDC;
use DiaColloDB::Relation;
use DiaColloDB::Utils qw(:sort :env :run :pack :file);
use DDC::Client::Distributed;
use Fcntl qw(:seek);
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Relation);

##==============================================================================
## Constructors etc.

## $ddc = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- persistent options
##    base => $basename,               ##-- configuration header basename (default=undef)
##    ##
##    ##-- ddc client options
##    ddcServer => "$server:$port",    ##-- ddc server; default=undef (required)
##    ddcTimeout => $timeout,          ##-- ddc timeout; default=60
##    ddcLimit   => $limit,            ##-- default limit for ddc queries (default=-1)
##    ddcSample  => $sample,           ##-- default sample size for ddc queries (default=-1:all)
##    dmax    => $maxDistance,         ##-- default distance for near() queries (default=5; 1=immediate adjacency; ~ ddc CQNear.Dist+1)
##    cfmin   => $minFreq,             ##-- default minimum frequency for count() queries (default=2)
##    ##
##    ##-- low-level data
##    dclient   => $ddcClient,         ##-- a DDC::Client::Distributed object
##   )
sub new {
  my $that = shift;
  my $rel  = $that->SUPER::new(
			       #base       => undef,
			       ddcServer  => undef,
			       ddcTimeout => 60,
			       ddcLimit   => -1,
			       ddcSample  => -1,
			       dmax       => 5,
			       cfmin      => 2,
			       @_
			      );
  $rel->{class} = ref($rel);
  return $rel->open() if (defined($rel->{base}));
  return $rel;
}

## $rel_or_undef = $CLASS_OR_OBJECT->fromDB($coldb,%opts)
##  + default implementation clobbers $rel->headerKeys() from %$coldb, %opts
sub fromDB {
  my ($that,$coldb,%opts) = @_;
  my $rel = ref($that) ? $that : $that->new();
  $rel->{$_} = $coldb->{$_} foreach (grep {exists $coldb->{$_}} $rel->headerKeys);
  @$rel{keys %opts} = values %opts;
  return undef if (!$rel->{ddcServer});
  return $rel;
}


##==============================================================================
## Relation API: create

## $rel = $CLASS_OR_OBJECT->create($coldb,$tokdat_file,%opts)
##  + default just calls fromDB() and saveHeaderFile()
sub create {
  my ($rel,$coldb,$datfile,%opts) = @_;
  $rel = $rel->fromDB($coldb,%opts) or return undef;
  $rel->saveHeader() if ($rel->{base});
  return $rel;
}

##==============================================================================
## Relation API: union (SKETCHY)

## $rel = $CLASS_OR_OBJECT->union($coldb, \@pairs, %opts)
##  + merge multiple co-frequency indices into new object
##  + @pairs : array of pairs ([$ug,\@xi2u],...)
##    of unigram-objects $ug and tuple-id maps \@xi2u for $ug
##  + %opts: clobber %$rel
##  + default just calls create(), but should probably create a list of ddc servers to query
sub union {
  my ($rel,$coldb,$pairs,%opts) = @_;
  $rel->logwarn("union() of ddc may not work as expected without global 'ddcServer' option")
    if (!$coldb->{ddcServer} && !$opts{ddcServer});
  return $rel->create($coldb,undef,%opts);
}

##==============================================================================
## Relation API: profiling & comparison: top-level

##--------------------------------------------------------------
## Relation API: profile

## $mprf = $rel->profile($coldb, %opts)
## + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
## + %opts: as for DiaColloDB::Relation::profile(), also:
##   (
##    ##-- sampling options
##    limit => $limit,       ##-- maximum number of items to return from ddc; sets $qconfig{limit} (default: query "#limit[N]" or $rel->{ddcLimit})
##    sample => $sample,     ##-- ddc sample size; sets $qconfig{qcount} Sample property (default: query "#sample[N]" or $rel->{ddcSample})
##    cfmin => $cfmin,       ##-- minimum subcorpus frequency for returned items (default: query "#fmin[N]" or $rel->{cfmin})
##    dmax  => $dmax,        ##-- maxmimum distance for implicit near() queries (default: query "#dmax[N]" or $rel->{dmax})
##   )
sub profile {
  my ($rel,$coldb,%opts) = @_;

  ##-- get query
  my $qcount = $rel->countQuery($coldb,\%opts);
  my $qstr   = $qcount->toString;
  $rel->vlog($coldb->{logProfile}, "f12-query: $qstr");

  ##-- query ddc server & check for errors
  my $client = $rel->ddcClient(limit=>$opts{limit});
  my $result = $client->queryJson($qstr);
  $rel->logconfess($coldb->{error}="profile(): DDC query failed: ".($result->{error_}//'(undefined error)'))
    if ($result->{error_} || $result->{istatus_} || $result->{nstatus_} || !$result->{counts_});
  $rel->vlog($coldb->{logProfile}, "fetched ", ($result->{end_}//'?'), " of ~", ($result->{nhits_}//'?'), " count-query result row(s)");

  ##-- get count-by expressions, titles
  my $cbexprs = $qcount->getKeys->getExprs;
  my @titles  = map {
    $coldb->attrTitle($_->can('getIndexName')
		      ? $_->getIndexName
		      : do { (my $label=$_->toString) =~ s{\'((?:\\.|[^\'])*)\'}{$1}; $label })
  } @$cbexprs[1..$#$cbexprs];


  ##-- parse raw counts into slice-wise profiles
  my %y2prf = qw();
  my ($y,$prf,$key);
  foreach (@{$result->{counts_}}) {
    $y   = $_->[1]//'0';
    $key = join("\t", @$_[2..$#$_]);
    $prf = ($y2prf{$y} //= DiaColloDB::Profile->new(label=>$y));
    $prf->{f12}{$key}   += $_->[0];
  }
  undef $result; ##-- save some memory

  ##-- query independent f2 and update slice-wise profiles
  my $fcoef = $opts{fcoef};
  $client->{limit} = -1;
  my $qkeys2  = DDC::XS::CQKeys->new($qcount);
  $qkeys2->setOptions(DDC::XS::CQueryOptions->new()) if (!$qkeys2->getOptions);
  $qkeys2->getOptions->setSeparateHits(1);
  $qkeys2->SetMatchId(2);
  my $qcount2 = DDC::XS::CQCount->new($qkeys2, $qcount->getKeys, -1, $qcount->getSort, $qcount->getLo, $qcount->getHi);
  my $qstr2  = $qcount2->toString;
  $rel->vlog($coldb->{logProfile}, "f2-query: $qstr2");
  my $result2 = $client->queryJson($qstr2);
  $rel->logconfess($coldb->{error}="profile(): DDC f2-query failed: ".($result2->{error_}//'(undefined error)'))
    if ($result2->{error_} || $result2->{istatus_} || $result2->{nstatus_} || !$result2->{counts_});
  $rel->vlog($coldb->{logProfile}, "fetched ", ($result2->{end_}//'?'), " of ~", ($result2->{nhits_}//'?'), " f2-query result row(s)");
  foreach (@{$result2->{counts_}}) {
    next if (!defined($prf=$y2prf{$y=$_->[1]}));
    $key = join("\t", @$_[2..$#$_]);
    $prf->{f2}{$key} += $_->[0]*$fcoef if (exists $prf->{f12}{$key});
  }
  undef $result2; ##-- save some memory

  ##-- query independent f1 and update slice-wise profiles
  my $qcount1 = $qcount2->clone();
  $_->setMatchId(1) foreach (grep {UNIVERSAL::isa($_,'DDC::XS::CQCountKeyExprToken') && $_->getMatchId==2}
			     @{$qcount1->getDtr->getQCount->getKeys->getExprs},
			     @{$qcount1->getKeys->getExprs},
			    );
  $qcount1->getDtr->setMatchId(1);
  my $qstr1  = $qcount1->toString;
  $rel->vlog($coldb->{logProfile}, "f1-query: $qstr1");
  my $result1 = $client->queryJson($qstr1);
  $rel->logconfess($coldb->{error}="profile(): DDC f1-query failed: ".($result1->{error_}//'(undefined error)'))
    if ($result1->{error_} || $result1->{istatus_} || $result1->{nstatus_} || !$result1->{counts_});
  $rel->vlog($coldb->{logProfile}, "fetched ", ($result1->{end_}//'?'), " of ~", ($result1->{nhits_}//'?'), " f1-query result row(s)");
  foreach (@{$result1->{counts_}}) {
    next if (!defined($prf=$y2prf{$y=$_->[1]}));
    $prf->{f1} += $_->[0]*$fcoef;
  }

  ##-- finalize sub-profiles: label, titles, N, compile & trim
  my $N1 = 0; $N1 += $_->{f1} foreach (values %y2prf);
  my $N  = $coldb->{xf}{N} * $fcoef;
  $N     = $N1 if ($N1 > $N);
  foreach $prf (values %y2prf) {
    $prf->{titles} = \@titles;

    $prf->{N} = $N;
    $prf->compile($opts{score}, eps=>$opts{eps});
    $prf->trim(kbest=>$opts{kbest}, cutoff=>$opts{cutoff});
  }


  ##-- honor "fill" option
  if ($opts{fill}) {
    for ($y=$opts{dlo}; $y <= $opts{dhi}; $y += ($opts{slice}||1)) {
      next if (exists($y2prf{$y}));
      $prf = $y2prf{$y} = DiaColloDB::Profile->new(N=>$N,f1=>0,label=>$y,titles=>\@titles);
    }
  }

  ##-- finalize: return meta-profile
  return DiaColloDB::Profile::Multi->new(
					 profiles => [@y2prf{sort {$a<=>$b} keys %y2prf}],
					 titles   => \@titles,
					);
}

##==============================================================================
## Utils: profiling

##--------------------------------------------------------------
## $dclient = $rel->ddcClient(%opts)
##  + returns cacned $rel->{dclient} if defined, otherwise creates and caches a new client
##  + chokes if ddcServer is not defined
##  + %opts clobber %{$rel->{dclient}}
sub ddcClient {
  my ($rel,%opts) = @_;
  if (defined($rel->{dclient})) {
    ##-- cached client: just set options
    @{$rel->{dclient}}{keys %opts} = values %opts;
    return $rel->{dclient};
  }

  ##-- sanity check(s)
  $rel->logconfess("ddcClient(): no 'ddcServer' key defined") if (!$rel->{ddcServer});

  ##-- get server
  my ($server,$port) = @$rel{qw(ddcServer ddcPort)};
  $server ||= 'localhost';
  $port     = $1 if ($server =~ s/\:([0-9]+)$//);
  $port   ||= 50000;

  return $rel->{dclient} = DDC::Client::Distributed->new(
							 timeout=>($rel->{ddcTimeout}//60),
							 connect=>{PeerAddr=>$server,PeerPort=>$port},
							 mode=>'json',
							 %opts
							);
}

##--------------------------------------------------------------
## $qcount = $rel->countQuery($coldb,\%opts)
## + creates a DDC::XS::CQCount object for profile() options %opts
## + %opts: as for DiaColloDB::Relation::DDC::profile()
## + sets following keys in %opts:
##   (
##    limit  => $limit,		##-- hit return limit for ddc query
##    dlo    => $dlo,           ##-- minimum date-slice, from @opts{qw(date slice fill)}
##    dhi    => $dhi,           ##-- maximum date-slice, from @opts{qw(date slice fill)}
##    fcoef  => $fcoef,		##-- frequency coefficient, parsed from "#coef[N]", auto-generated for near() queries
##   )
sub countQuery {
  my ($rel,$coldb,$opts) = @_;

  ##-- groupby clause: date
  my $gbdate = ($opts->{slice}<=0
		? DDC::XS::CQCountKeyExprConstant->new($opts->{slice}||'0')
		: DDC::XS::CQCountKeyExprDateSlice->new('date',$opts->{slice}));

  ##-- groupby clause: user request
  my $gbreq  = $coldb->parseRequest($opts->{groupby}, logas=>'groupby', default=>undef);
  my $gbkeys = [$gbdate];
  my $qrestr = undef; ##-- groupby restriction clause (WITH)
  foreach (@$gbreq) {
    push(@$gbkeys, DDC::XS::CQCountKeyExprToken->new($_->[0], 2, 0));
    if (defined($_->[1]) && !UNIVERSAL::isa($_->[1], 'DDC::XS::CQTokAny')) {
      $qrestr = (defined($qrestr) ? DDC::XS::CQWith->new($qrestr,$_->[1]) : $_->[1]);
    }
  }
  my $qkeys  = DDC::XS::CQCountKeyExprList->new;
  $qkeys->setExprs($gbkeys);

  ##-- query hacks: override options
  my $limit  = ($opts->{query} =~ s/\s*\#limit\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{limit}//$rel->{ddcLimit})) || -1;
  my $sample = ($opts->{query} =~ s/\s*\#samp(?:le)?\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{sample}//$rel->{ddcSample})) || -1;
  my $dmax   = ($opts->{query} =~ s/\s*\#d(?:ist)?max\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{dmax}//$rel->{dmax})) || 1;
  my $cfmin  = ($opts->{query} =~ s/\s*\#c?fmin\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{cfmin}//$rel->{cfmin})) // '';
  my $fcoef  = ($opts->{query} =~ s/\s*\#f?coef\s*[\s\[]s*([\+\-]?\d*\.?\d+(?:[eE][\+-]?\d+)?)\s*\]?//i ? $1 : $opts->{fcoef});

  ##-- parse daughter query & setup match-ids
  my $qdtr  = $coldb->parseQuery($opts->{query}, logas=>'query', default=>'', ddcmode=>1);
  my $qopts = $qdtr->getOptions || DDC::XS::CQueryOptions->new;
  $qdtr->setOptions(undef);

  ##-- get target query nodes (item1 ~ matchid =1, item2 ~ matchid =2)
  my (@qnodes1,@qnodes2);
  foreach (grep {UNIVERSAL::can($_,'getMatchId')} @{$qdtr->Descendants}) {
    push(@qnodes1,$_) if ($_->getMatchId<=1);
    push(@qnodes2,$_) if ($_->getMatchId==2);
  }
  $rel->logconfess("no primary target-nodes found in daughter query '", $qdtr->toString, "': use match-id =1 to specify primary target(s)")
    if (!@qnodes1);
  $_->setMatchId(1) foreach (@qnodes1);

  ##-- check for target match-id =2 and maybe implicitly construct near() query
  if (!@qnodes2) {
    my $qany = DDC::XS::CQTokAny->new();
    $qany->SetMatchId(2);
    $qany = DDC::XS::CQWith->new($qany,$qrestr) if (defined($qrestr));
    $dmax = 1 if ($dmax < 1);
    my $qnear = DDC::XS::CQNear->new($dmax-1,$qdtr,$qany);
    @qnodes2  = ($qany);
    $qdtr     = $qnear;
    $fcoef    = 2 * $dmax;
  }
  elsif ($qrestr) {
    ##-- append groupby restriction to all targets (=2)
    my ($nod,$newnod);
    $qdtr = $qdtr->mapTraverse(sub {
				 $nod = shift;
				 if (UNIVERSAL::isa($nod, 'DDC::XS::CQToken') && $nod->getMatchId == 2) {
				   $newnod = DDC::XS::CQWith->new($nod,$qrestr);
				   $newnod->setOptions($nod->getOptions);
				   $nod->setOptions(undef);
				   return $newnod;
				 }
				 return $nod;
			       });
  }

  ##-- guess fcoef
  if (!defined($fcoef)) {
    if (UNIVERSAL::isa($qdtr,'DDC::XS::CQNear')) {
      $fcoef = 2 * ($qdtr->getDist + 1);
    }
    else {
      $fcoef = 1;
    }
  }

  ##-- qdtr options
  $qopts->setSeparateHits(1) if ($opts->{query} !~ /\#(?:sep(?:arate)?|nojoin)(?:_hits)?\b/i);
  $qdtr->setOptions($qopts);

  ##-- date clause
  my ($dfilter,$dlo,$dhi) = $coldb->parseDateRequest(@$opts{qw(date slice fill)});
  my $filters = $qopts->getFilters;
  if ($dfilter && !grep {UNIVERSAL::isa($_,'DDC::XS::CQFDateSort')} @$filters) {
    unshift(@$filters, DDC::XS::CQFDateSort->new(DDC::XS::LessByDate(),
						 ($dlo ? "${dlo}-00-00" : ''),
						 ($dhi ? "${dhi}-12-31" : '')
						));
    $qopts->setFilters($filters);
  }

  ##-- set random seed if we're using a limited sample
  if ($sample > 0) {
    my $gotseed = 0;
    foreach (@$filters) {
      if (UNIVERSAL::isa($_,'DDC::XS::CQFRandomSort')) {
	$_->setArg1(int(rand(100))) if (!$_->getArg1); ##-- use 100 random seeds
	$gotseed = 1;
	last;
      }
    }
    if (!$gotseed) {
      push(@$filters, DDC::XS::CQFRandomSort->new(int(rand(100))));
      $qopts->setFilters($filters);
    }
  }

  ##-- cleanup coldb parser (so we're using "real" refcounts)
  $coldb->qcompiler->CleanParser();

  ##-- finalize: construct count query & set options
  $cfmin = '' if (($cfmin//1) <= 1);
  my $qcount = DDC::XS::CQCount->new($qdtr, $qkeys, $sample, DDC::XS::GreaterByCountValue(), $cfmin);
  @$opts{qw(limit sample dlo dhi fcoef cfmin)} = ($limit,$sample,$dlo,$dhi,$fcoef,$cfmin);
  return $qcount;
}

##--------------------------------------------------------------
## $f2 = $rel->estimateFrequency()
##  + TODO

##==============================================================================
## Pacakge Alias(es)
package DiaColloDB::DDC;
use strict;
our @ISA = qw(DiaColloDB::Relation::DDC);


##==============================================================================
## Footer
1;

__END__
