## -*- Mode: CPerl -*-
## File: DiaColloDB::Client::file.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db: client: local dbdir

package DiaColloDB::Client::file;
use DiaColloDB::Client;
use URI;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Client);

##==============================================================================
## Constructors etc.

## $cli = CLASS_OR_OBJECT->new(%args)
## $cli = CLASS_OR_OBJECT->new($url, %args)
## + %args, object structure:
##   (
##    ##-- DiaColloDB::Client: options
##    url  => $url,       ##-- local url; query form is used as db parameters
##    ##
##    ##-- DiaColloDB::Client::file
##    db   => $db,        ##-- underlying DiaColloDB object
##   )

##==============================================================================
## I/O: open/close

## $cli_or_undef = $cli->open_file($file_url,%opts)
## $cli_or_undef = $cli->open_file()
##  + opens a local file url
##  + may re-bless() $cli into an appropriate package
##  + OVERRIDE in subclasses supporting file urls
sub open_file {
  my ($cli,$url,%opts) = @_;
  $cli  = $cli->new() if (!ref($cli));
  $cli->close() if ($cli->opened);
  $cli->{url} = $url = ($url // $cli->{url});
  my ($path,%dbopts);
  ($path = $url) =~ s{^file://}{}i;
  %dbopts = URI->new($url)->query_form() if ($path =~ s{\?.*$}{});
  $cli->{db} = DiaColloDB->new(%dbopts,dbdir=>$path)
    or $cli->logconfess("open_file() failed to open DB directory $path: $!");
  return $cli;
}

## $cli_or_undef = $cli->close()
##  + default just returns $cli
sub close {
  my $cli = shift;
  $cli->{db}->close() if ($cli->{db});
  delete @$cli{qw(db)};
  return $cli;
}

## $bool = $cli->opened()
##  + default just checks for $cli->{url}
sub opened {
  return ref($_[0]) && $_[0]{db} && $_[0]{db}->opened();
}

##==============================================================================
## Profiling

##--------------------------------------------------------------
## Profiling: Generic

## $mprf = $cli->profile($relation, %opts)
##  + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts: as for DiaColloDB::profile()
sub profile {
  my $cli = shift;
  $cli->logconfess("profile(): no db opened!") if (!$cli->opened);
  delete $cli->{error};
  $cli->{error} = $cli->{db}{error} if (!defined(my $mp = $cli->{db}->profile(@_)));
  return $mp;
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $cli->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + %opts: as for DiaColloDB::compare()
sub compare {
  my $cli = shift;
  $cli->logconfess("compare(): no db opened!") if (!$cli->opened);
  delete $cli->{error};
  $cli->{error} = $cli->{db}{error} if (!defined(my $mp = $cli->{db}->compare(@_)));
  return $mp;
}

##==============================================================================
## Footer
1;

__END__




