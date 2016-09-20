## -*- Mode: CPerl -*-
## File: DiaColloDB::Relation::DDC.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, profiling relation: ddc client (using DDC::Client::Distributed)

package DiaColloDB::Relation::DDC;
use DiaColloDB::Relation;
use DiaColloDB::Utils qw(:math :list);
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
##    ddcServer => "$server:$port",    ##-- ddc server (required; default=$coldb->{ddcServer} via fromDB() method)
##    ddcTimeout => $timeout,          ##-- ddc timeout; default=120
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
			       ddcTimeout => 120,
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
## Relation API: dbinfo

## \%info = $rel->dbinfo($coldb)
##  + embedded info-hash for $coldb->dbinfo()
sub dbinfo {
  my ($rel,$coldb) = @_;
  $rel = $rel->fromDB($coldb);
  my $info = $rel->SUPER::dbinfo();
  my @keys = qw(ddcServer ddcTimeout ddcLimit ddcSample dmax cfmin);
  @$info{@keys} = @$rel{@keys};
  return $info;
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
  my ($that,$coldb,%opts) = @_;
  my $rel = $that->fromDB($coldb,%opts)
    or $that->logconfess($coldb->{error}="profile(): initialization failed (did you forget to set the 'ddcServer' option?)");

  ##-- get count-query, count-by expressions, titles
  my $qcount = $rel->countQuery($coldb,\%opts);

  ##-- get count-by expressions, titles
  my $cbexprs = $qcount->getKeys->getExprs;
  my @titles  = map {
    $coldb->attrTitle($_->can('getIndexName')
		      ? $_->getIndexName
		      : do { (my $label=$_->toString) =~ s{\'((?:\\.|[^\'])*)\'}{$1}; $label =~ s{ ~ s/}{~s/}g; $label })
  } @$cbexprs[1..$#$cbexprs];

  ##-- get raw f12 results and parse into slice-wise profiles
  my $result12 = $rel->ddcQuery($coldb, $qcount, limit=>$opts{limit}, logas=>'f12');
  my (%y2prf,$y,$prf,$key);
  foreach (@{$result12->{counts_}}) {
    $y   = $_->[1]//'0';
    $key = join("\t", @$_[2..$#$_]);
    $prf = ($y2prf{$y} //= DiaColloDB::Profile->new(label=>$y));
    $prf->{f12}{$key}   += $_->[0];
  }
  undef $result12; ##-- save some memory

  ##-- check whether count-query uses any token-attributes
  my $needKeys = grep {UNIVERSAL::can($_,'getMatchId') && $_->getMatchId==2} @{$qcount->getKeys->getExprs};

  ##-- get raw g2 results and update slice-wise profiles
  my $fcoef  = $opts{fcoef};
  my ($qcount2);
  if ($needKeys) {
    my $qkeys2 = DDC::Any::CQKeys->new($qcount);
    $qkeys2->setOptions($qcount->getDtr->getOptions);
    $qkeys2->SetMatchId(2);
    $qcount2 = DDC::Any::CQCount->new($qkeys2, $qcount->getKeys, $qcount->getSample, $qcount->getSort, $qcount->getLo, $qcount->getHi);
  }
  else {
    my $qdtr2 = DDC::Any::CQTokAny->new;
    $qdtr2->setOptions($qcount->getDtr->getOptions);
    ($qcount2 = $qcount->clone())->setDtr($qdtr2);
  }
  my $result2 = $rel->ddcQuery($coldb, $qcount2, limit=>-1, logas=>'f2');
  foreach (@{$result2->{counts_}}) {
    next if (!defined($prf=$y2prf{$y=$_->[1]}));
    $key = join("\t", @$_[2..$#$_]);
    $prf->{f2}{$key} += $_->[0]*$fcoef if (exists $prf->{f12}{$key});
  }
  undef $result2; ##-- save some memory

  ##-- query independent f1 and update slice-wise profiles
  if ($needKeys) {
    my $qcount1 = $qcount2->clone();
    $_->setMatchId(1) foreach (grep {UNIVERSAL::isa($_,'DDC::Any::CQCountKeyExprToken') && $_->getMatchId==2}
			       @{$qcount1->getDtr->getQCount->getKeys->getExprs},
			       @{$qcount1->getKeys->getExprs},
			      );
    $qcount1->getDtr->setMatchId(1);
    my $result1 = $rel->ddcQuery($coldb, $qcount1, limit=>-1, logas=>'f1');
    foreach (@{$result1->{counts_}}) {
      next if (!defined($prf=$y2prf{$y=$_->[1]}));
      $prf->{f1} += $_->[0]*$fcoef;
    }
    undef $result1; ##-- save some memory (but not much)
  } else {
    foreach $prf (values %y2prf) {
      my $f1=0;
      $f1 += $_ foreach (values %{$prf->{f12}});
      $prf->{f1} = $f1*$fcoef;
    }
  }

  ##-- query independent N
  my $qstrN   = "count(* #sep)";
  my $resultN = $rel->ddcQuery($coldb, $qstrN, limit=>1, logas=>'fN');
  my $N       = $resultN->{counts_}[0][0] * $fcoef;

  ##-- finalize sub-profiles: label, titles, N, compile
  my $N1 = 0; $N1 += $_->{f1} foreach (values %y2prf);
  $N     = $N1 if ($N1 > $N);
  my ($f1);
  foreach $prf (values %y2prf) {
    $prf->{titles} = \@titles;

    if (!$prf->{f1}) {
      $f1  = 0;
      $f1 += $_ foreach (values %{$prf->{f12}});
      $prf->{f1} = $f1;
    }

    $prf->{N} = $N;
    $prf->compile($opts{score}, eps=>$opts{eps});
  }

  ##-- honor "fill" option
  if ($opts{fill}) {
    for ($y=$opts{dslo}; $y <= $opts{dshi}; $y += ($opts{slice}||1)) {
      next if (exists($y2prf{$y}));
      $prf = $y2prf{$y} = DiaColloDB::Profile->new(N=>$N,f1=>0,label=>$y,titles=>\@titles)->compile($opts{score},eps=>$opts{eps});
    }
  }

  ##-- setup meta-profile query info
  my $qtemplate = $qcount->getDtr->clone();
  my $qinfo = {
	       q12=>$qcount->toStringFull,
	       #q1=>$qcount1->toStringFull,
	       #q2=>$qcount2->toStringFull,
	       #qN=>$qstrN,
	       fcoef=>$fcoef,
	       qtemplate=>$opts{qtemplate},
	      };
  foreach (values %$qinfo) { utf8::decode($_) if (!utf8::is_utf8($_)); }

  ##-- finalize: collect multi-profile & trim
  $rel->vlog($coldb->{logProfile}, "profile(): collect and trim");
  my $mp = DiaColloDB::Profile::Multi->new(
					   profiles => [@y2prf{sort {$a<=>$b} keys %y2prf}],
					   titles   => \@titles,
					   qinfo    => $qinfo,
					  );
  $mp->trim(%opts, empty=>!$opts{fill});

  ##-- return
  return $mp;
}

##--------------------------------------------------------------
## Relation API: extend

## $mprf = $rel->extend($coldb, %opts)
## + get f2 frequencies profile for selected items as a DiaColloDB::Profile::Multi object
## + requires 'query' option for correct estimation of 'fcoef'
## + %opts: as for profile()
sub extend {
  my ($that,$coldb,%opts) = @_;
  my $rel = $that->fromDB($coldb,%opts)
    or $that->logconfess($coldb->{error}="extend(): initialization failed (did you forget to set the 'ddcServer' option?)");

  ##-- common variables
  $opts{coldb}   = $coldb;
  my $opts       = \%opts;
  my $logProfile = $coldb->{logProfile};

  ##-- sanity check(s)
  my ($slice2keys);
  if (!($slice2keys=$opts{slice2keys})) {
    $rel->warn($coldb->{error}="extend(): no 'slice2keys' parameter specified!");
    return undef;
  }
  elsif (!UNIVERSAL::isa($slice2keys,'HASH')) {
    $rel->warn($coldb->{error}="extend(): failed to parse 'slice2keys' parameter");
    return undef;
  }

  ##-- get "real" count-query, count-by expressions, titles, fcoef, ...
  my $qcount = $rel->countQuery($coldb,\%opts);

  ##-- parse slice2keys into HASH-refs: ($slice => {$key=>[split(/\t/,$key)], ... }, ...)
  $slice2keys->{$_} = {map {($_=>[split(/\t/,$_)])} @{$slice2keys->{$_}}} foreach (keys %$slice2keys);
  my $items         = {map { %$_ } values %$slice2keys};

  ##-- create "extend" query (batch)
  my $gbexprs   = $opts{gbexprs}->getExprs;
  my $gbfilters = $opts{gbfilters};
  my $qxdtr     = undef;
  my @qxfilt    = qw();
  my ($gbxi,$gbai,$expr,$qa,$fa);
  foreach $gbxi (1..$#$gbexprs) {
    $expr = $gbexprs->[$gbxi];
    $gbai = $gbxi-1;
    if (UNIVERSAL::isa($expr,'DDC::Any::CountKeyExprConstant') || UNIVERSAL::isa($_,'DDC::Any::CQCountKeyExprDate')) {
      ; ##-- skip these
    }
    if (UNIVERSAL::isa($expr,'DDC::Any::CQCountKeyExprToken')) {
      ##-- token expression -> literal set
      $qa = DDC::Any::CQTokSet->new;
      $qa->setIndexName($expr->getIndexName);
      $qa->setValues([map {$_->[$gbai]//''} values %$items]);
      $qxdtr = defined($qxdtr) ? DDC::Any::CQWith->new($qxdtr,$qa) : $qa;
    }
    elsif (UNIVERSAL::isa($expr,'DDC::Any::CQCountKeyExprBibl')) {
      ##-- bibl expression -> literal set
      $fa = DDC::Any::CQFHasFieldSet->new($expr->getLabel,[map {$_->[$gbai]//''} values %$items]);
      push(@qxfilt, $fa);
    }
    elsif (UNIVERSAL::isa($_, 'DDC::Any::CQCountKeyExprRegex') && UNIVERSAL::isa($_->getSrc, 'DDC::Any::CQCountKeyExprBibl')) {
      ##-- regex transformation: ignore
      #$fa = DDC::Any::CQFHasFieldRegex->new($expr->getLabel);
      #$fa->setRegex(join('|', map {"\\Q$_->[$gbai]\\E"} values %$items));
      #push(@qxfilt, $fa);
      ;
    }
    else {
      $coldb->warn("can't generate extend query-template for groupby expression of type ", ref($expr)//'(undefined)', " \`", $expr->toString, "'");
    }
  }
  $coldb->logconfess($coldb->{error}="no query restrictions found for 'extend' query") if (!$qxdtr);
  $qxdtr->setMatchId(2);
  $qxdtr->setOptions($qcount->getDtr->getOptions->clone);

  ##-- cleanup coldb parser (so we're using "real" refcounts)
  $coldb->qcompiler->CleanParser();

  my $qextend = $qcount->clone;
  $qextend->setDtr($qxdtr);

  ##-- get slice-wise f2
  my $xresult = $rel->ddcQuery($coldb, $qextend, limit=>$opts{limit}, logas=>'f2_extend', logTrunc=>-1);
  my $fcoef   = $opts{fcoef} || 1;
  my (%y2prf,$y,$prf,$key);
  foreach (@{$xresult->{counts_}}) {
    $y   = ($_->[1]//'0');
    $key = join("\t", @$_[2..$#$_]);
    next if (!exists $slice2keys->{$y}{$key});
    $prf = ($y2prf{$y} //= DiaColloDB::Profile->new(label=>$y,N=>0,f1=>0));
    $prf->{f2}{$key} += $_->[0]*$fcoef;
    $prf->{f12}{$key} = 0;
  }

  ##-- honor "fill" option
  if ($opts{fill}) {
    for ($y=$opts{dslo}; $y <= $opts{dshi}; $y += ($opts{slice}||1)) {
      next if (exists($y2prf{$y}));
      $prf = $y2prf{$y} = DiaColloDB::Profile->new(label=>$y,N=>0,f1=>0);
    }
  }

  ##-- finalize: collect multi-profile
  my $mp = DiaColloDB::Profile::Multi->new(profiles => [@y2prf{sort {$a<=>$b || $a cmp $b} keys %y2prf}]);
  return $mp;
}

##--------------------------------------------------------------
## Relation API: compare

## $mprf = $rel->compare($coldb, %opts)
## + get a comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
## + %opts: as for DiaColloDB::Relation::compare(), also:
##   (
##    ##-- sampling options
##    (a|b)?limit => $limit,       ##-- maximum number of items to return from ddc; sets $qconfig{limit} (default: query "#limit[N]" or $rel->{ddcLimit})
##    (a|b)?sample => $sample,     ##-- ddc sample size; sets $qconfig{qcount} Sample property (default: query "#sample[N]" or $rel->{ddcSample})
##    (a|b)?cfmin => $cfmin,       ##-- minimum subcorpus frequency for returned items (default: query "#fmin[N]" or $rel->{cfmin})
##    (a|b)?dmax  => $dmax,        ##-- maxmimum distance for implicit near() queries (default: query "#dmax[N]" or $rel->{dmax})
##   )
sub compare {
  my $rel = shift;
  return $rel->SUPER::compare(@_, _gbparse=>0, _abkeys=>[qw(limit sample cfmin dmax)], strings=>0);
}

##==============================================================================
## Utils: profiling

##--------------------------------------------------------------
## $dclient = $rel->ddcClient(%opts)
##  + returns cached $rel->{dclient} if defined, otherwise creates and caches a new client
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
  $rel->logconfess($opts{coldb}{error}="ddcClient(): no 'ddcServer' key defined") if (!$rel->{ddcServer});

  ##-- get server
  my ($server,$port) = @$rel{qw(ddcServer ddcPort)};
  $server ||= 'localhost';
  $port     = $1 if ($server =~ s/\:([0-9]+)$//);
  $port   ||= 50000;

  return $rel->{dclient} = DDC::Client::Distributed->new(
							 timeout=>($rel->{ddcTimeout}//120),
							 connect=>{PeerAddr=>$server,PeerPort=>$port},
							 mode=>'json',
							 %opts
							);
}

##--------------------------------------------------------------
## $results = $rel->ddcQuery($coldb, $query_or_str, %opts)
##  + %opts:
##     logas => $prefix,   ##-- log prefix (default: 'ddcQuery()')
##     loglevel => $level, ##-- log level (default=$coldb->{logProfile})
##     limit => $limit,    ##-- set result client limit (default: current client limit, or -1 for limit=>undef)
##     logTrunc => $len,   ##-- truncate long query strings to max $len characters (default=-1: full string)
sub ddcQuery {
  my ($rel,$coldb,$query,%opts) = @_;
  my $logas = $opts{logas} // 'ddcQuery';
  my $level = exists($opts{loglevel}) ? $opts{loglevel} : $coldb->{logProfile};
  my $trunc = $opts{logTrunc} // -1;

  my $qstr = ref($query) ? $query->toStringFull : $query;
  my $cli  = $rel->ddcClient();
  $cli->{limit} = $opts{limit}//-1 if (exists($opts{limit}));

  $rel->vlog($level, "$logas: query[server=$rel->{ddcServer},limit=$cli->{limit}]: ",
	     ($trunc < 0 || length($qstr) <= $trunc ? $qstr : (substr($qstr,0,$trunc)."...")));
  $cli->open()
    or $rel->logconfess($coldb->{error}="$logas: failed to connect to DDC server on $rel->{ddcServer}: $!")
    if (!defined($cli->{sock}));
  my $result = $cli->queryJson($qstr);

  $rel->logconfess($coldb->{error}="$logas ERROR: DDC query failed: ".($result->{error_}//'(undefined error)'))
    if ($result->{error_} || $result->{istatus_} || $result->{nstatus_} || !$result->{counts_});
  my $approx = ($result->{end_}//0) < ($result->{nhits_}//2**32) ? "~" : '';
  $rel->vlog($level, "$logas: fetched ", ($result->{end_}//'?'), " of $approx", ($result->{nhits_}//'?'), " result row(s)");

  return $result;
}

##--------------------------------------------------------------
## $fcoef = $rel->fcoef($cquery)
sub fcoef {
  my ($rel,$qnod) = @_;
  if (UNIVERSAL::isa($qnod,'DDC::Any::CQNear')) {
    return 2 * ($qnod->getDist + 1) * $rel->fcoef($qnod->getDtr1) * $rel->fcoef($qnod->getDtr2);
  }
  elsif (UNIVERSAL::isa($qnod,'DDC::Any::CQAnd') || UNIVERSAL::isa($qnod,'DDC::Any::CQOr')) {
    return max2($rel->fcoef($qnod->getDtr1),$rel->fcoef($qnod->getDtr2));
  }
  elsif (UNIVERSAL::isa($qnod,'DDC::Any::CQSeq')) {
    my $fcoef = 1;
    my $items = $qnod->getItems;
    my $dists = $qnod->getDists;
    my $dops  = $qnod->getDistOps;
    foreach (0..$#$dists) {
      $fcoef *= $rel->fcoef($items->[$_]);
      $dops->[$_] //= '<';
      if ($dops->[$_] eq '<') {
	$fcoef *= ($dists->[$_]+1);
      }
      elsif ($dops->[$_] eq '>') {
	$fcoef *= 32-($dists->[$_]+1); ##-- ddc global MaxDistanceForNear=32
      }
    }
    return $fcoef * $rel->fcoef($items->[$#$items]);
  }
  return 1;
}

##--------------------------------------------------------------
## $qcount = $rel->countQuery($coldb,\%opts)
## + creates a DDC::Any::CQCount object for profile() options %opts
## + %opts: as for DiaColloDB::Relation::DDC::profile()
## + sets following keys in %opts:
##   (
##    gbexprs => $gbexprs,      ##-- groupby expressions (DDC::Any::CQCountKeyExprList)
##    gbrestr => $gbrestr,      ##-- groupby item2 restrictions (DDC::Any::CQWith conjunction of token expressions)
##    gbfilters => \@gbfilters, ##-- groupby filter restrictions (ARRAY-ref of DDC::Any::CQFilter)
##    limit  => $limit,		##-- hit return limit for ddc query
##    dslo   => $dslo,          ##-- minimum date-slice, from @opts{qw(date slice fill)}
##    dshi   => $dshi,          ##-- maximum date-slice, from @opts{qw(date slice fill)}
##    dlo    => $dlo,           ##-- minimum date request (ddc)
##    dhi    => $dhi,           ##-- maximum date request (ddc)
##    fcoef  => $fcoef,		##-- frequency coefficient, parsed from "#coef[N]", auto-generated for near() queries
##    qtemplate => $qtemplate,  ##-- query template for ddc hit link-up
##   )
sub countQuery {
  my ($rel,$coldb,$opts) = @_;

  ##-- groupby clause: user request
  my ($gbexprs,$gbrestr,$gbfilters) = @$opts{qw(gbexprs gbrestr gbfilters)} = $coldb->parseGroupBy($opts->{groupby}, %$opts);

  ##-- query hacks: override options
  my $limit  = ($opts->{query} =~ s/\s*\#limit\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{limit}//$rel->{ddcLimit})) || -1;
  my $sample = ($opts->{query} =~ s/\s*\#samp(?:le)?\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{sample}//$rel->{ddcSample})) || -1;
  my $dmax   = ($opts->{query} =~ s/\s*\#d(?:ist)?max\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{dmax}//$rel->{dmax})) || 1;
  my $cfmin  = ($opts->{query} =~ s/\s*\#c?fmin\s*[\s\[]\s*([\+\-]?\d+)\s*\]?//i ? $1 : ($opts->{cfmin}//$rel->{cfmin})) // '';
  my $fcoef  = ($opts->{query} =~ s/\s*\#f?coef\s*[\s\[]s*([\+\-]?\d*\.?\d+(?:[eE][\+-]?\d+)?)\s*\]?//i ? $1 : $opts->{fcoef});

  ##-- parse daughter query & setup match-ids
  my $qdtr  = $coldb->parseQuery($opts->{query}, logas=>'query', default=>'', ddcmode=>-1);
  my $qopts = $qdtr->getOptions || DDC::Any::CQueryOptions->new;
  $qdtr->setOptions(undef);

  ##-- get target query nodes (item1 ~ matchid =1, item2 ~ matchid =2)
  my (@qnodes1,@qnodes2);
  foreach (grep {UNIVERSAL::can($_,'getMatchId')} @{$qdtr->Descendants}) {
    push(@qnodes1,$_) if ($_->getMatchId<=1);
    push(@qnodes2,$_) if ($_->getMatchId==2);
  }
  $rel->logconfess($coldb->{error}="no primary target-nodes found in daughter query '", $qdtr->toString, "': use match-id =1 to specify primary target(s)")
    if (!@qnodes1 && !@qnodes2);
  $_->setMatchId(1) foreach (@qnodes1);

  ##-- check for target match-id =2 and maybe implicitly construct near() query
  if (!@qnodes2) {
    $gbrestr //= DDC::Any::CQTokRegex->new('p',$coldb->{pgood},0) if ($coldb->{pgood}); ##-- simulate coldb 'pgood' restriction (content words only)
    my $qany = $gbrestr // DDC::Any::CQTokAny->new();
    $qany->setMatchId(2);
    $dmax = 1 if ($dmax < 1);
    my $qnear = DDC::Any::CQNear->new($dmax-1,$qdtr,$qany);
    @qnodes2  = ($qany);
    $qdtr     = $qnear;
  }
  elsif ($gbrestr) {
    ##-- append groupby restriction to all targets (=2)
    my ($nod,$newnod);
    $qdtr = $qdtr->mapTraverse(sub {
				 $nod = shift;
				 if (UNIVERSAL::isa($nod, 'DDC::Any::CQToken') && $nod->getMatchId == 2) {
				   $newnod = DDC::Any::CQWith->new($nod,$gbrestr);
				   $newnod->setOptions($nod->getOptions);
				   $nod->setMatchId(0);
				   $newnod->setMatchId(2);
				   $nod->setOptions(undef);
				   return $newnod;
				 }
				 return $nod;
			       });
  }

  ##-- maybe guess fcoef
  $fcoef //= $rel->fcoef($qdtr);
  $rel->vlog($coldb->{logProfile}, "guessed fcoef=$fcoef");

  ##-- qdtr options
  $qopts->setSeparateHits(1) if ($opts->{query} !~ /\#(?:sep(?:arate)?|nojoin)(?:_hits)?\b/i);
  $qdtr->setOptions($qopts);

  ##-- date clause
  my ($dfilter,$dslo,$dshi,$dlo,$dhi) = $coldb->parseDateRequest(@$opts{qw(date slice fill)},1);
  my $filters = [@$gbfilters, @{$qopts->getFilters}];
  if ($dfilter && !grep {UNIVERSAL::isa($_,'DDC::Any::CQFDateSort')} @$filters) {
    unshift(@$filters, DDC::Any::CQFDateSort->new(DDC::Any::LessByDate(),
						 ($dlo ? "${dlo}-00-00" : ''),
						 ($dhi ? "${dhi}-12-31" : '')
						));
  }
  $qopts->setFilters($filters);

  ##-- set random seed if we're using a limited sample
  if ($sample > 0) {
    my $gotseed = 0;
    foreach (@$filters) {
      if (UNIVERSAL::isa($_,'DDC::Any::CQFRandomSort')) {
	$_->setArg1(int(rand(100))) if (!$_->getArg1); ##-- use 100 random seeds
	$gotseed = 1;
	last;
      }
    }
    if (!$gotseed) {
      push(@$filters, DDC::Any::CQFRandomSort->new(int(rand(100))));
      $qopts->setFilters($filters);
    }
  }

  ##-- setup query template
  my ($qtconds,$qtcond,@qtfilters);
  my $xi=0;
  foreach (@{$gbexprs->getExprs}) {
    if (!defined($_) || UNIVERSAL::isa($_,'DDC::Any::CQCountKeyExprConstant') || UNIVERSAL::isa($_,'DDC::Any::CQCountKeyExprDate')) {
      ; ##-- skip these
    } elsif (UNIVERSAL::isa($_,'DDC::Any::CQCountKeyExprToken')) {
      ##-- token expression
      $qtcond  = DDC::Any::CQTokExact->new($_->getIndexName, "__W2.${xi}__");
      $qtconds = defined($qtconds) ? DDC::Any::CQWith->new($qtconds,$qtcond) : $qtcond;
    } elsif (UNIVERSAL::isa($_, 'DDC::Any::CQCountKeyExprBibl')) {
      ##-- bibl expression
      my $label = $_->getLabel;
      push(@qtfilters, DDC::Any::CQFHasField->new($label, "__W2.${xi}__"))
	if (!grep {((ref($_)//'') eq 'DDC::Any::CQFHasField') && $_->getArg0 eq $label} @$filters);
    }  elsif (UNIVERSAL::isa($_, 'DDC::Any::CQCountKeyExprRegex') && UNIVERSAL::isa($_->getSrc, 'DDC::Any::CQCountKeyExprBibl')) {
      ##-- regex transformation: hack
      if ($_->getReplacement eq '' && $_->getPattern =~ /^(.)\.\*\$/) {
	##-- hack for simple prefix regex transformations (textclass)
	push(@qtfilters, DDC::Any::CQFHasFieldPrefix->new($_->getSrc->getLabel, "__W2.${xi}__"));
      } elsif ($_->getReplacement eq '') {
	##-- hack for simple regex deletions (any substring; not always correct)
	push(@qtfilters, DDC::Any::CQFHasFieldInfix->new($_->getSrc->getLabel, "__W2.${xi}__"));
      } else {
	##-- non-trivial regex transformation
	$coldb->warn("can't generate template for non-trivial regex transformation \`", $_->toString, "'");
      }
    } else {
      $coldb->warn("can't generate template for groupby expression of type ", ref($_)//'(undefined)', " \`", $_->toString, "'");
    }
    ++$xi;
  }
  #$qtconds //= DDC::Any::CQTokAny->new();
  $qtconds->setMatchId(2) if ($qtconds);
  my $qtemplate = $qdtr->clone->mapTraverse(sub {
					      my $nod = shift;
					      if (UNIVERSAL::can($nod,'getMatchId') && $nod->getMatchId==2) {
						return $qtconds // $nod;
					      }
					      return $nod;
					    });
  $qtemplate->setOptions($qdtr->getOptions->clone) if (!$qtemplate->getOptions); ##-- traversal bug
  $qtemplate->getOptions->setFilters([@$filters,@qtfilters]);

  ##-- cleanup coldb parser (so we're using "real" refcounts)
  $coldb->qcompiler->CleanParser();

  ##-- finalize: construct count query & set options
  $cfmin = '' if (($cfmin//1) <= 1);
  my $qcount = DDC::Any::CQCount->new($qdtr, $gbexprs, $sample, DDC::Any::GreaterByCountValue(), $cfmin);
  @$opts{qw(limit sample dslo dshi dlo dhi fcoef cfmin qtemplate)} = ($limit,$sample,$dslo,$dshi,$dlo,$dhi,$fcoef,$cfmin,$qtemplate->toStringFull);
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
