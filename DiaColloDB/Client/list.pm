## -*- Mode: CPerl -*-
## File: DiaColloDB::Client::Distributed.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, client

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
##    fudge => $coef,      ##-- get ($coef*$kbest) items from sub-clients (default=1)
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
	  fudge=>1,
	 );
}

##==============================================================================
## I/O: open/close

## $cli_or_undef = $cli->open_list(  \@urls,  %opts)
## $cli_or_undef = $cli->open_list($list_url, %opts)
## $cli_or_undef = $cli->open_list()
##  + creates new client for each url, passing %opts to DiaColloDB::Client->new()
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
  @$cli{qw(url urls)} = ($url,$urls);

  ##-- open sub-clients
  my ($c);
  my $clis = $cli->{clis} = [];
  foreach (@$urls) {
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
  my ($mp,$mpi);
  my $kbest  = $opts{kbest} // 0;
  my $kfudge = ($cli->{fudge} || 1)*$kbest;
  foreach (@{$cli->{clis}}) {
    $mpi = $_->profile($rel,%opts,kbest=>$kfudge)
      or $cli->logconfess("profile() failed for client URL $_->{url}: $_->{error}");
    $mp  = defined($mp) ? $mp->_add($mpi) : $mpi;
  }

  ##-- re-compile and -trim
  $mp->compile($opts{score})->trim(kbest=>$kbest, cutoff=>$opts{cutoff});
  return $mp;
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $cli->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + %opts: as for DiaColloDB::compare()
##  + sets $cli->{error} on error
sub compare {
  my ($cli,$rel,%opts) = @_;
  my ($mp,$mpi);
  my $kbest  = $opts{kbest} // 0;
  my $kfudge = ($cli->{fudge} || 1)*$kbest;
  foreach (@{$cli->{clis}}) {
    $mpi = $_->compare($rel,%opts,kbest=>$kfudge)
      or $cli->logconfess("profile() failed for client URL $_->{url}: $!");
    $mp  = defined($mp) ? $mp->_add($mpi) : $mpi;
  }

  ##-- re-compile and -trim
  $mp->compile($opts{score})->trim(kbest=>$kbest, cutoff=>$opts{cutoff});
  return $mp;
}

##==============================================================================
## Footer
1;

__END__




