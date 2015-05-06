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
##    dmax    => $maxDistance,         ##-- default distance for near() queries (default=5)
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
##    limit => $limit,       ##-- maximum number of items to return from ddc; sets $qconfig{limit} (overridden by query "#limit[N]"
##    sample => $sample,     ##-- ddc sample size; sets $qconfig{qcount} Sample property (overriden by query "#sample[N]"
##   )
sub profile {
  my ($rel,$coldb,%opts) = @_;

  ##-- get query
  my $qcount = $rel->countQuery($coldb,\%opts);
  my $qstr   = $qcount->toString;
  $rel->vlog($coldb->{logProfile}, "DDC query: $qstr");

  ##-- query ddc server & check for errors
  my $client = $rel->ddcClient(limit=>$opts{limit});
  my $result = $client->queryJson($qstr);
  $rel->logconfess($coldb->{error}="profile(): DDC query failed: ".($result->{error_}//'(undefined error)'))
    if ($result->{error_} || $result->{istatus_} || $result->{nstatus_} || !$result->{counts_});

  ##-- parse counts into slice-wise profiles
  my %y2prf = qw();
  my ($y,$prf,$key,$N);
  foreach (@{$result->{counts_}}) {
    $y   = $_->[1]//'0';
    $key = join("\t", @$_[2..$#$_]);
    $prf = ($y2prf{$y} //= DiaColloDB::Profile->new(label=>$y));
    $prf->{f12}{$key}   += $_->[0];
    $N  += $_->[0];
  }

  ##-- finalize sub-profiles: label, titles, N, f1, f2(hacked), compile & trim
  my @titles = map {$coldb->attrTitle($_->getIndexName)} grep {UNIVERSAL::isa($_,'DDC::XS::CQCountKeyExprToken')} @{$qcount->getKeys->getExprs};
  my ($f1);
  foreach $prf (values %y2prf) {
    $prf->{titles} = \@titles;
    $prf->{N}      = $N;
    $prf->{f2}     = $prf->{f12}; ##-- hack

    $f1  = 0;
    $f1 += $_ foreach (values %{$prf->{f12}});
    $prf->{f1} = $f1;

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

  ##-- query hacks: limit/sample
  my $limit  = ($opts->{query} =~ s/\s*\#limit?\s*[\s\[]\s*([\+\-]?\d+)\s*\]?// ? $1 : ($opts->{limit}//$rel->{ddcLimit}));
  my $sample = ($opts->{query} =~ s/\s*\#sample?\s*[\s\[]\s*([\+\-]?\d+)\s*\]?// ? $1 : ($opts->{sample}//$rel->{ddcSample}));

  ##-- check for match-id and maybe implicitly construct near() query
  my $qdtr  = $coldb->parseQuery($opts->{query}, logas=>'query', default=>'', ddcmode=>1);
  my $qopts = $qdtr->getOptions || DDC::XS::CQueryOptions->new;
  $qdtr->setOptions(undef);
  my @qtargets = $qdtr->HasMatchId ? (grep {UNIVERSAL::can($_,'getMatchId') && $_->getMatchId == 2} @{$qdtr->Descendants}) : qw();
  if (!@qtargets) {
    my $qany = DDC::XS::CQTokAny->new();
    $qany->SetMatchId(2);
    $qany = DDC::XS::CQWith->new($qany,$qrestr) if (defined($qrestr));
    my $qnear = DDC::XS::CQNear->new(($rel->{dmax}||1),$qdtr,$qany);
    @qtargets = ($qany);
    $qdtr = $qnear;
  }
  elsif ($qrestr) {
    ##-- append groupby restriction to all targets
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

  ##-- cleanup coldb parser (so we're using "real" refcounts)
  $coldb->qcompiler->CleanParser();

  ##-- finalize: construct count query & set options
  my $qcount = DDC::XS::CQCount->new($qdtr, $qkeys, $sample, DDC::XS::GreaterByCountValue());
  @$opts{qw(limit dlo dhi)} = ($limit,$dlo,$dhi);
  return $qcount;
}

##==============================================================================
## Pacakge Alias(es)
package DiaColloDB::DDC;
use strict;
our @ISA = qw(DiaColloDB::Relation::DDC);


##==============================================================================
## Footer
1;

__END__
