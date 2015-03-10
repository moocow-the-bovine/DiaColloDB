## -*- Mode: CPerl -*-
## File: DiaColloDB::Client.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, client

package DiaColloDB::Client;
use DiaColloDB::Persistent;
use DiaColloDB::Client::file;
use DiaColloDB::Client::http;
use DiaColloDB::Client::list;
use URI;
use strict;


##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Persistent);

##==============================================================================
## Constructors etc.

## $cli = CLASS_OR_OBJECT->new(%args)
## $cli = CLASS_OR_OBJECT->new($url, %args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Client: options
##    url  => $url,       ##-- db url
##   )
sub new {
  my $that = shift;
  my $url  = (@_ % 2)==0 ? undef : shift;
  my $cli  = bless({
		    $that->defaults(),
		    url=>$url,
		    @_
		   }, ref($that)||$that);
  return $cli->open() if (defined($cli->{url}));
  return $cli;
}

## %defaults = $CLASS_OR_OBJ->defaults()
##  + called by new() and promote()
sub defaults {
  return qw();
}

## $cli_or_undef = $cli->promote($class,%opts)
##  + promotes $cli to (a subclass of) $class
##  + ensures $class->defaults() keys are set for $cli
##  + client options are clobbered with %opts
sub promote {
  my ($cli,$class,%opts) = @_;
  $cli = $cli->new() if (!ref($cli));
  bless($cli,$class) if (!UNIVERSAL::isa($cli,$class));
  @$cli{keys %opts} = values(%opts);
  %$cli = ($class->defaults,%$cli);
  return $cli;
}

## undef = $obj->DESTROY
##  + destructor calls close() if necessary
sub DESTROY {
  $_[0]->close() if ($_[0]->opened);
}

##==============================================================================
## I/O: open/close

## $cli_or_undef = $cli->open($url,%opts)
## $cli_or_undef = $cli->open()
##  + calls open_file() or open_http() as appropriate
sub open {
  my ($cli,$url) = (shift,shift);
  $url //= $cli->{url};
  if (UNIVERSAL::isa($url,'ARRAY') || $url =~ m{^list://}i) {
    return $cli->open_list($url,@_);
  }
  elsif ($url =~ m{^http://}i) {
    return $cli->open_http($url,@_);
  }
  elsif ($url =~ m{^file://}i || $url !~ m{^[a-zA-Z]+://}) {
    return $cli->open_file($url,@_);
  }
  $cli->logconfess("open(): unsupported URL scheme for $url");
  return undef;
}


## $cli_or_undef = $cli->open_file($file_url,%opts)
## $cli_or_undef = $cli->open_file()
##  + opens a local file url
##  + may re-bless() $cli into an appropriate package
##  + OVERRIDE in subclasses supporting file urls
sub open_file {
  my $cli = shift;
  $cli = $cli->new() if (!ref($cli));
  $cli->close() if ($cli->opened);
  $cli->promote('DiaColloDB::Client::file');
  $cli->logconfess("open_file(): not implemented") if ($cli->can('open_file') eq \&open_file);
  return $cli->open_file(@_)
}

## $cli_or_undef = $cli->open_http($http_url,%opts)
## $cli_or_undef = $cli->open_http()
##  + opens a http url
##  + may re-bless() $cli into an appropriate package
##  + OVERRIDE in subclasses supporting http urls
sub open_http {
  my $cli = shift;
  $cli = $cli->new() if (!ref($cli));
  $cli->close() if ($cli->opened);
  $cli->promote('DiaColloDB::Client::http');
  $cli->logconfess("open_http(): not implemented") if ($cli->can('open_http') eq \&open_http);
  return $cli->open_http(@_)
}

## $cli_or_undef = $cli->open_list($list_url,%opts)
## $cli_or_undef = $cli->open_list(\@urls,   %opts)
## $cli_or_undef = $cli->open_list()
##  + opens a list url
##  + may re-bless() $cli into an appropriate package
##  + OVERRIDE in subclasses supporting list urls
sub open_list {
  my $cli = shift;
  $cli = $cli->new() if (!ref($cli));
  $cli->close() if ($cli->opened);
  $cli->promote('DiaColloDB::Client::list');
  $cli->logconfess("open_list(): not implemented") if ($cli->can('open_list') eq \&open_list);
  return $cli->open_list(@_)
}

## $cli_or_undef = $cli->close()
##  + default just returns $cli
sub close {
  return $_[0];
}

## $bool = $cli->opened()
##  + default just checks for $cli->{url}
sub opened {
  return ref($_[0]) && defined($_[0]{url});
}

##==============================================================================
## db-info

## \%info = $cli->dbinfo()
sub dbinfo {
  my $cli = shift;
  $cli->logconfess("dbinfo(): not implemented");
}

##==============================================================================
## Profiling

##--------------------------------------------------------------
## Profiling: Wrappers

## $mprf = $cli->query($rel,%opts)
##  + get a generic DiaColloDB::Profile::Multi object for $rel
##  + calls $coldb->profile() or $coldb->compare() as appropriate
sub query {
  my ($cli,$rel) = (shift,shift);
  if ($rel =~ s{^(?:d(?:iff)?|co?mp(?:are)?)[\-\/\.\:]?}{}) {
    return $cli->compare($rel,@_);
  }
  return $cli->profile($rel,@_);
}

## $mprf = $cli->profile1(%opts)
##  + get unigram frequency profile for selected items as a DiaColloDB::Profile::Multi object
##  + really just wraps $cli->profile('xf', %opts)
##  + %opts: see profile() method
sub profile1 {
  return $_[0]->profile('xf',@_[1..$#_]);
}


## $mprf = $cli->profile2(%opts)
##  + get co-frequency profile for selected items as a DiaColloDB::Profile::Multi object
##  + really just wraps $cli->profile('cof', %opts)
##  + %opts: see profile() method
sub profile2 {
  return $_[0]->profile('cof',@_[1..$#_]);
}

## $mprf = $cli->compare1(%opts)
##  + get unigram comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + really just wraps $cli->compare('xf', %opts)
##  + %opts: see compare() method
BEGIN { *diff1 = \&compare1; }
sub compare1 {
  return $_[0]->compare('xf',@_[1..$#_]);
}

## $mprf = $cli->compare2(%opts)
##  + get co-frequency comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + really just wraps $cli->profile('cof', %opts)
##  + %opts: see compare() method
BEGIN { *diff2 = \&compare2; }
sub compare2 {
  return $_[0]->compare('cof',@_[1..$#_]);
}

##--------------------------------------------------------------
## Profiling: Generic

## $mprf = $cli->profile($relation, %opts)
##  + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts: as for DiaColloDB::profile()
##  + sets $cli->{error} on error
sub profile {
  my ($cli,$rel,%opts) = @_;
  $cli->logconfess("profile(): not implemented");
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $cli->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + %opts: as for DiaColloDB::compare()
##  + sets $cli->{error} on error
sub diff { $_[0]->compare(@_[1..$#_]); }
sub compare {
  my ($cli,$rel,%opts) = @_;
  $cli->logconfess("compare(): not implemented");
}

##==============================================================================
## Footer
1;

__END__




