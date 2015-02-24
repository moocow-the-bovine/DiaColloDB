## -*- Mode: CPerl -*-
## File: DiaColloDB::Client::Distributed.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, client

package DiaColloDB::Client::Distributed;
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
##    ##-- options
##    urls  => \@urls,     ##-- db urls
##    ##
##    ##-- guts
##    clis => \@clis,      ##-- per-url clients
##   )
sub new {
  my $that = shift;
  my $urls = (@_ % 2)==0 ? [] : shift;
  my $cli  = bless({
		    urls=>$urls,
		    @_
		   }, ref($that)||$that);
  return $cli->open() if (defined($cli->{urls}) && @{$cli->{urls}});
  return $cli;
}

##==============================================================================
## I/O: open/close

## $cli_or_undef = $cli->open(\@urls,%opts)
## $cli_or_undef = $cli->open()
##  + calls open() on all urls
sub open {
  my ($cli,$urls) = (shift,shift);
  $cli->{urls} = $urls = ($urls // $cli->{urls});
  my ($c);
  my $clis = $cli->{clis} = [];
  foreach (@$urls) {
    $c = DiaColloDB::Client->new($_,@_)
      or $cli->logconfess("open(): failed to create client for URL '$_': $!");
    push(@$clis, $c);
  }
  return $cli;
}

## $cli_or_undef = $cli->open_file($file_url,%opts)
## $cli_or_undef = $cli->open_file()
##  + opens a local file url
##  + may re-bless() $cli into an appropriate package
##  + OVERRIDE in subclasses supporting file urls
sub open_file {
  $_[0]->logconfess("open_file(): not supported.");
}

## $cli_or_undef = $cli->open_http($http_url,%opts)
## $cli_or_undef = $cli->open_http()
##  + opens a http url
##  + may re-bless() $cli into an appropriate package
##  + OVERRIDE in subclasses supporting http urls
sub open_http {
  $_[0]->logconfess("open_http(): not supported.");
}

## $cli_or_undef = $cli->close()
##  + default just returns $cli
sub close {
  return $_[0];
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
## Profiling

##--------------------------------------------------------------
## Profiling: Generic

## $mprf = $cli->profile($relation, %opts)
##  + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts: as for DiaColloDB::profile()
##  + sets $cli->{error} on error
sub profile {
  my ($cli,$rel) = (shift,shift);
  my ($mp,$mpi);
  foreach (@{$cli->{clis}}) {
    $mpi = $_->profile($rel,@_)
      or $cli->logconfess("profile() failed for client URL $_->{url}: $!");
    $mp  = defined($mp) ? $mp->_add($mpi) : $mpi;
  }

  ##-- re-compile and -trim
  $mp->compile($opts{score})->trim(kbest=>$opts{kbest}, cutoff=>$opts{cutoff});
  return $mp;
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $cli->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + %opts: as for DiaColloDB::compare()
##  + sets $cli->{error} on error
### + TODO
sub diff { $_[0]->compare(@_[1..$#_]); }
sub compare {
  my ($cli,$rel,%opts) = @_;
  $cli->logconfess("compare(): not implemented");
}

##==============================================================================
## Footer
1;

__END__




