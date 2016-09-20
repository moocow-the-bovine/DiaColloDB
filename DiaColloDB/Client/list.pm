## -*- Mode: CPerl -*-
## File: DiaColloDB::Client::list.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, client: list

package DiaColloDB::Client::list;
use DiaColloDB::Client;
use DiaColloDB::Utils qw(:list :math :si);
use strict;

##-- try to use threads
our ($HAVE_FORKS);
BEGIN {
  $HAVE_FORKS = eval <<EOF;
#use threads; ##-- segfaults on join()ing 2nd thread (possibly bogus destruction)
use forks;
1
EOF
  $@ = '';
}

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Client);

##==============================================================================
## Constructors etc.

## $cli = CLASS_OR_OBJECT->new(%args)
## $cli = CLASS_OR_OBJECT->new(\@urls, %args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Client: options
##    url  => $url,       ##-- list url (sub-urls separated by whitespace, "+SCHEME://" or "+://")
##    ##
##    ##-- DiaColloDB::Client::list
##    urls  => \@urls,     ##-- db urls
##    opts  => \%opts,     ##-- sub-client options
##    fudge => $coef,      ##-- get ($coef*$kbest) items from sub-clients (0:all, default=10)
##    fork  => $bool,      ##-- run each subclient query in its own fork? (default=if available)
##    lazy => $bool,       ##-- use temporary on-demand sub-clients (true,default) or persistent sub-clients (false)
##    extend => $bool,     ##-- use extend() queries to acquire correct f2 counts? (default=true)
##    logFudge => $level,  ##-- log-level for fudge-factor debugging (default='debug')
##    logFork => $level,   ##-- log-level for thread (fork) options (default='none')
##    ##
##    ##-- guts
##    #clis => \@clis,     ##-- per-url clients for mode, v0.11.000
##   )

## %defaults = $CLASS_OR_OBJ->defaults()
##  + called by new()
sub defaults {
  return (
	  #urls=>[],
	  #clis=>[],
	  opts=>{},
	  fudge=>10,
	  logFudge => 'debug',
	  logThread => 'none',
	  fork => $HAVE_FORKS,
	  lazy => 1,
	  extend => 1,
	 );
}

##==============================================================================
## I/O: open/close

## $cli_or_undef = $cli->open_list(  \@urls,  %opts)
## $cli_or_undef = $cli->open_list($list_url, %opts)
## $cli_or_undef = $cli->open_list()
##  + creates new client for each url, passing %opts to DiaColloDB::Client->new()
##  + component URLs beginning with '?' are treated as options to $cli itself
sub open_list {
  my ($cli,$url) = (shift,shift);

  ##-- parse URLs
  $url //= $cli->{url};
  my ($urls);
  if (UNIVERSAL::isa($url,'ARRAY')) {
    $urls = $url;
    $url  = "list://".join(' ', @$urls);
  } else {
    ($urls=$url) =~ s{^list://}{};
    $urls        = [map {s{^://}{}; $_} grep {($_//'') ne ''} split(m{\s+|\+(?=[a-zA-Z0-9\+\-\.]*://)},$urls)];
  }

  ##-- parse list-client options (query-only URLs)
  my $curls = [];
  foreach (@$urls) {
    if (UNIVERSAL::isa($_,'HASH')) {
      ##-- HASH-ref: clobber local options
      @$cli{keys %$_} = values %$_;
    }
    elsif (m{^(?:://)?\?}) {
      ##-- query-string only: clobber local options
      my %form = URI->new($_)->query_form;
      @$cli{keys %form} = values %form;
    }
    else {
      ##-- sub-URL
      push(@$curls,$_);
    }
  }
  @$cli{qw(url urls)} = ($url,$curls);

  ##-- sanity check(s)
  if ($cli->{fork} && !$HAVE_FORKS) {
    $cli->warn("fork-mode requested, but 'forks' module unavailable");
    $cli->{fork} = 0;
  }

  ##-- save sub-client options in $cli->{opts}
  if (@_) {
    my %opts = @_;
    $cli->{opts}{keys %opts} = values %opts;
  }

  ##-- open sub-clients (non-lazy mode)
  $cli->{clis} = [map {$cli->client($_)} (0..$#$curls)] if (!$cli->{lazy});

  return $cli;
}

## $cli_or_undef = $cli->close()
##  + default just returns $cli
sub close {
  my $cli = shift;
  $_->close() foreach (grep {defined($_)} @{$cli->{clis}//[]});
  delete $cli->{clis};
  return $cli;
}

## $bool = $cli->opened()
##  + override checks for non-empty $cli->{urls}
##  + ensures all sub-clients are opened in non-lazy mode
sub opened {
  return (ref($_[0])
	  && $_[0]{urls}
	  && @{$_[0]{urls}}
	  && ($_[0]{lazy} || (
			      $_[0]{clis}
			      && @{$_[0]{clis}}==@{$_[0]{urls}}
			      && !grep {!defined($_) || !$_->opened} @{$_[0]{clis}}
			     ))
	 );
}

## $cli = $cli->client($i, %opts)
##  + open (temporary) sub-client #$i
sub client {
  my ($cli,$i,%opts) = @_;
  return $cli->{clis}[$i] if (!$cli->{lazy} && $cli->{clis} && $cli->{clis}[$i]); ##-- non-lazy mode
  my $url = $cli->{urls}[$i]
    or $cli->logconfess("client(): no URL for client #$i");
  my $sub = DiaColloDB::Client->new($url,%{$cli->{opts}//{}},%opts)
    or $cli->logconfess("client(): failed to create client for URL '$url': $!");
  return $sub;
}

##==============================================================================
## I/O: Persistent API: header
##  + largely INHERITED from DiaColloDB::Persistent

## @keys = $coldb->headerKeys()
##  + keys to save as header
sub headerKeys {
  return (qw(url urls), grep {!ref($_[0]{$_}) && $_ !~ m{^log}} keys %{$_[0]});
}



##==============================================================================
## utils: threaded sub-client calls

##  @results = $cli->subcall(\&CODE, @args)
## \@results = $cli->subcall(\&CODE, @args)
##  + calls CODE($cli, $i, @args) in scalar context foreach $i (0..$#{$cli->{urls}})
##  + CODE is expected to return anything other than undef
sub subcall {
  my ($cli,$code,@args) = @_;
  my ($i,@results);
  if ($HAVE_FORKS && $cli->{fork}) {
    ##-- threaded call
    #PDL::no_clone_skip_warning() if (UNIVERSAL::can('PDL','no_clone_skip_warning')); ##-- ithreads warning
    my (@thrs);
    for ($i=0; $i <= $#{$cli->{urls}}; ++$i) {
      $cli->vlog($cli->{logThread}, "subcall(): spawning thread for subclient[$i]");
      push(@thrs, threads->create({context=>'scalar'}, $code, $cli, $i, @args));
    }
    for ($i=0; $i <= $#{$cli->{urls}}; ++$i) {
      $cli->vlog($cli->{logThread}, "subcall(): joining thread for subclient[$i]");
      my $rv = $thrs[$i]->join(); ##-- perl 'threads' module (ithreads) segfaults here at 2nd encounter (client #0:ok, client #1:segfault)
      $cli->logconfess("subcall(): error processing subclient[$i] ($cli->{urls}[$i])") if ($thrs[$i]->error);
      push(@results, $rv);
    }
  }
  else {
    ##-- non-threaded call
    for ($i=0; $i <= $#{$cli->{urls}}; ++$i) {
      push(@results, scalar($code->($cli,$i,@args)));
    }
  }
  return wantarray ? @results : \@results;
}

##==============================================================================
## dbinfo

## \%info = $cli->dbinfo()
##   + returned info is {dtrs=>\@dtr_info, fudge=>$coef},
sub dbinfo {
  my $cli  = shift;
  my @dtrs = $cli->subcall(sub {
			     my $sub = $_[0]->client($_[1]);
			     $sub->dbinfo()
			       or $_[0]->logconfess("dbinfo() failed for client URL $sub->{url}: $sub->{error}");
			   });

  ##-- collect & merge daughter info
  my $info  = {dtrs=>\@dtrs, (map {($_=>$cli->{$_})} qw(fudge fork lazy)), urls=>join(' ',@{$cli->{urls}})};
  my %attrs = qw();
  my %rels  = qw();
  my ($di,$d);
  foreach $di (0..$#dtrs) {
    $d = $dtrs[$di];
    $d->{url} = $cli->{urls}[$di];
    foreach (@{$d->{attrs}}) {
      $attrs{$_->{name}}[$di] = $_;
    }
    foreach (keys %{$d->{relations}}) {
      $rels{$_}[$di] = $d->{relations}{$_};
    }
  }
  $info->{timestamp} = (sort map {$_->{timestamp}||''} @dtrs)[$#dtrs];
  $info->{xdmax}     = lmax(map {$_->{xdmax}} @dtrs);
  $info->{xdmin}     = lmin(map {$_->{xdmax}} @dtrs);
  $info->{du_b}      = lsum(map {$_->{du_b}} @dtrs);
  $info->{du_h}      = si_str($info->{du_b});
  $info->{version}   = $DiaColloDB::VERSION;

  ##-- extract common attributes
  my ($aname,$avals,$a,$counts);
  foreach $aname (keys %attrs) {
    $avals = $attrs{$aname};
    next if ((grep {defined $_} @$avals) != @dtrs);
    $a = { name=>$aname, title=>$avals->[0]{title} };
    $a->{size} = join('+', map {$_->{size}} @$avals);
    $a->{alias} = [sort grep {$counts->{$_} >= @dtrs} keys %{$counts = lcounts([map {@{$_->{alias}//[]}} @$avals])}];
    push(@{$info->{attrs}}, $a);
  }

  ##-- extract common relations
  my ($rname,$rvals,$r);
  foreach $rname (keys %rels) {
    $rvals = $rels{$rname};
    next if ((grep {defined $_} @$rvals) != @dtrs);
    $r = { };
    $r->{class} = join(' ', @{luniq([map {$_->{class}} @$rvals])});
    $r->{du_b}  = lsum(map {$_->{du_b}} @$rvals);
    $r->{du_h}  = si_str($r->{du_b});


    $r->{attrs} = [sort grep {$counts->{$_} >= @dtrs} keys %{$counts = lcounts([map {@{$_->{attrs}//[]}} @$rvals])}]
      if (grep {$_->{attrs}} @$rvals);
    $r->{meta} = [sort grep {$counts->{$_} >= @dtrs} keys %{$counts = lcounts([map {@{$_->{meta}//[]}} @$rvals])}]
      if (grep {$_->{meta}} @$rvals);

    $info->{relations}{$rname} = $r;
  }

  return $info;
}


##==============================================================================
## Profiling

##--------------------------------------------------------------
## Profiling: Generic

## $mprf = $cli->profile($relation, %opts)
##  + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts: as for DiaColloDB::profile()
##  + sets $cli->{error} on error
sub profile {
  my ($cli,$rel,%opts) = @_;

  ##-- defaults
  DiaColloDB->profileOptions(\%opts);

  ##-- fudge coefficient
  my $kbest  = $opts{kbest} // 0;
  my $kfudge = ($cli->{fudge} // 1)*$kbest;
  $cli->vlog($cli->{logFudge}, "profile(): querying ", scalar(@{$cli->{urls}}), " client URL(s) with (fudge=", ($cli->{fudge}//1), ") * (kbest=$kbest) = $kfudge");

  ##-- query clients
  my @mps = $cli->subcall(sub {
			    my $sub = $_[0]->client($_[1]);
			    $sub->profile($rel,%opts,strings=>1,kbest=>$kfudge,cutoff=>0)
			      or $_[0]->logconfess("profile() failed for client URL $sub->{url}: $sub->{error}");
			  });

  if ($cli->{extend} && @mps > 1) {
    $cli->vlog($cli->{logFudge}, "profile(): extending sub-profiles");

    ##-- fill-out multi-profiles (ensure compatible slice-partitioning & find "missing" keys)
    DiaColloDB::Profile::Multi->xfill(\@mps);
    my $xkeys = DiaColloDB::Profile::Multi->xkeys(\@mps);
    #$cli->trace("extend(): xkeys=", DiaColloDB::Utils::saveJsonString($xkeys, utf8=>0));

    ##-- extend multi-profiles with "missing" keys
    my @mpx = $cli->subcall(sub {
			      #return undef if (!$xkeys->[$_[1]] || !grep {@$_} values(%{$xkeys->[$_[1]]})); ##-- don't need extend here
			      my $sub = $_[0]->client($_[1]);
			      $sub->extend($rel,%opts,strings=>1,score=>'f',cutoff=>0,slice2keys=>JSON::to_json($xkeys->[$_[1]]))
				or $_[0]->logconfess("extend() failed for client url $sub->{url}: $sub->{error}");
			    });
    foreach (0..$#mpx) {
      $mps[$_]->_add($mpx[$_]) if (defined($mpx[$_]));
    }
  }

  ##-- create final profile
  my $mp = shift(@mps) or return undef;
  $mp->_add($_) foreach (@mps);
  $cli->vlog($cli->{logFudge}, "profile(): collected fudged profile of size ", $mp->size)
    if (($cli->{logFudge}//'off') !~ /^(?:off|none)$/);

  ##-- re-compile and -trim
  $mp->compile($opts{score}, eps=>$opts{eps})->trim(kbest=>$kbest, cutoff=>$opts{cutoff}, empty=>!$opts{fill});

  $cli->vlog($cli->{logFudge}, "profile(): trimmed final profile to size ", $mp->size)
    if (($cli->{logFudge}//'off') !~ /^(?:off|none)$/);

  return $mp;
}

##--------------------------------------------------------------
## Profiling: extend (pass-2 for multi-clients)

## $mprf = $cli->extend($relation, %opts)
##  + get an extension-profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts: as for DiaColloDB::extend()
##  + sets $cli->{error} on error
sub extend {
  my ($cli,$rel,%opts) = @_;

  ##-- defaults
  DiaColloDB->profileOptions(\%opts);

  ##-- query clients
  my @mps = $cli->subcall(sub {
			    my $sub = $_[0]->client($_[1]);
			    $sub->extend($rel,%opts,strings=>1)
			      or $_[0]->logconfess("extend() failed for client URL $sub->{url}: $sub->{error}");
			  });

  ##-- create final profile
  my $mp = shift(@mps) or return undef;
  $mp->_add($_) foreach (@mps);

  return $mp;
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $cli->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + adpated from generic DiaColloDB::Relation::profile()
##  + %opts: as for DiaColloDB::compare()
##  + sets $cli->{error} on error
sub compare {
  my ($cli,$rel,%opts) = @_;

  ##-- defaults
  DiaColloDB->compareOptions(\%opts);

  ##-- common variables
  my %aopts = map {exists($opts{"a$_"}) ? ($_=>$opts{"a$_"}) : qw()} (qw(query date slice), @{$opts{_abkeys}//[]});
  my %bopts = map {exists($opts{"b$_"}) ? ($_=>$opts{"b$_"}) : qw()} (qw(query date slice), @{$opts{_abkeys}//[]});
  my %popts = (kbest=>-1,cutoff=>'',global=>0,strings=>0,fill=>1);

  ##-- get profiles to compare
  my $mpa = $cli->profile($rel,%opts, %aopts,%popts) or return undef;
  my $mpb = $cli->profile($rel,%opts, %bopts,%popts) or return undef;

  ##-- alignment and trimming
  my $ppairs = DiaColloDB::Profile::MultiDiff->align($mpa,$mpb);
  DiaColloDB::Profile::MultiDiff->trimPairs($ppairs, %opts);
  my $diff = DiaColloDB::Profile::MultiDiff->new($mpa,$mpb, titles=>$mpa->{titles}, diff=>$opts{diff});
  $diff->trim( DiaColloDB::Profile::Diff->diffkbest($opts{diff})=>$opts{kbest} ) if (!$opts{global});

  ##-- return
  return $diff;
}

##==============================================================================
## Footer
1;

__END__
