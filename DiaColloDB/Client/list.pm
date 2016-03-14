## -*- Mode: CPerl -*-
## File: DiaColloDB::Client::list.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, client: list

package DiaColloDB::Client::list;
use DiaColloDB::Client;
use strict;


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
##    url  => $url,       ##-- list url (space-separated sub-urls)
##    ##
##    ##-- DiaColloDB::Client::list
##    urls  => \@urls,     ##-- db urls
##    opts  => \%opts,     ##-- sub-client options
##    fudge => $coef,      ##-- get ($coef*$kbest) items from sub-clients (0:all, default=10)
##    logFudge => $level,  ##-- log-level for fudge-factor debugging (default='debug')
##    ##
##    ##-- guts
##    clis => \@clis,      ##-- per-url clients
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
    $url  = "list://".join(' ',@$urls);
  } else {
    ($urls=$url) =~ s{^list://}{};
    $urls        = [split(' ',$urls)];
  }

  ##-- parse list-client options (query-only URLs)
  my $curls = [];
  foreach (@$urls) {
    if (/^\?/) {
      my %form = URI->new($_)->query_form;
      @$cli{keys %form} = values %form;
    } else {
      push(@$curls,$_);
    }
  }
  @$cli{qw(url urls)} = ($url,$curls);

  ##-- open sub-clients
  my ($c);
  my $clis = $cli->{clis} = [];
  foreach (@$curls) {
    $c = DiaColloDB::Client->new($_,%{$cli->{opts}//{}},@_)
      or $cli->logconfess("open(): failed to create client for URL '$_': $!");
    push(@$clis, $c);
  }
  return $cli;
}

## $cli_or_undef = $cli->close()
##  + default just returns $cli
sub close {
  my $cli = shift;
  $_->close() foreach (grep {defined($_)} @{$cli->{clis}});
  delete $cli->{clis};
  return $cli;
}

## $bool = $cli->opened()
##  + default just checks for $cli->{url}
sub opened {
  return (ref($_[0])
	  && $_[0]{clis}
	  && $_[0]{urls}
	  && @{$_[0]{clis}}==@{$_[0]{urls}}
	  && !grep {!defined($_) || !$_->opened} @{$_[0]{clis}});
}

##==============================================================================
## dbinfo

## \%info = $cli->dbinfo()
##   + returned info is {dtrs=>\@dtr_info, fudge=>$coef},
sub dbinfo {
  my $cli  = shift;
  my $dtrs = [];
  my ($dinfo);
  foreach (@{$cli->{clis}}) {
    $dinfo = $_->dbinfo()
      or $cli->logconfess("dbinfo() failed for client URL $_->{url}: $_->{error}");
    $dinfo->{url} = $_->{url};
    push(@$dtrs,$dinfo);
  }
  return {dtrs=>$dtrs, fudge=>$cli->{fudge}};
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

  ##-- defualts
  DiaColloDB->profileOptions(\%opts);

  ##-- fudge coefficient
  my $kbest  = $opts{kbest} // 0;
  my $kfudge = ($cli->{fudge} // 1)*$kbest;
  $cli->vlog($cli->{logFudge}, "profile(): querying ", scalar(@{$cli->{clis}}), " client(s) with (fudge=", ($cli->{fudge}//1), ") * (kbest=$kbest) = $kfudge");

  ##-- query clients
  my ($mp,$mpi);
  foreach (@{$cli->{clis}}) {
    $mpi = $_->profile($rel,%opts,strings=>1,kbest=>$kfudge,cutoff=>0)
      or $cli->logconfess("profile() failed for client URL $_->{url}: $_->{error}");
    $mp  = defined($mp) ? $mp->_add($mpi) : $mpi;
  }
  $cli->vlog($cli->{logFudge}, "profile(): collected fudged profile of size ", $mp->size)
    if (($cli->{logFudge}//'off') !~ /^(?:off|none)$/);

  ##-- re-compile and -trim
  $mp->compile($opts{score}, eps=>$opts{eps})->trim(kbest=>$kbest, cutoff=>$opts{cutoff}, empty=>!$opts{fill});

  $cli->vlog($cli->{logFudge}, "profile(): trimmed final profile to size ", $mp->size)
    if (($cli->{logFudge}//'off') !~ /^(?:off|none)$/);

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

  ##-- defaults ::: CONTINUE HERE
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




