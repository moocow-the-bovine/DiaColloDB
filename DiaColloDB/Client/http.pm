## -*- Mode: CPerl -*-
## File: DiaColloDB::Client::http.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db: client: remote http server

package DiaColloDB::Client::http;
use DiaColloDB::Client;
use URI;
use LWP::UserAgent;
use HTTP::Request;
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
##    url  => $url,       ##-- local url
##    ##
##    ##-- DiaColloDB::Client::http: options
##    user => $user,          ##-- for LWP::UserAgent basic authentication
##    password => $password,  ##-- for LWP::UserAgent basic authentication
##    ##
##    ##-- DiaColloDB::Client::http: guts
##    ua   => $ua,        ##-- underlying LWP::UserAgent
##   )

##==============================================================================
## I/O: open/close

## $cli_or_undef = $cli->open_http($http_url,%opts)
## $cli_or_undef = $cli->open_http()
##  + opens a local file url
##  + may re-bless() $cli into an appropriate package
##  + OVERRIDE in subclasses supporting file urls
sub open_http {
  my ($cli,$url,%opts) = @_;
  $cli = $cli->new() if (!ref($cli));
  $cli->close() if ($cli->opened);
  $url //= $cli->{url};
  if ($url !~ m/profile/) {
    $url .= "/" if ($url !~ m{/$});
    $url .= "profile.perl";
  }
  $cli->{url}   = $url;
  $cli->{ua}  //= LWP::UserAgent->new();
  return $cli;
}

## $cli_or_undef = $cli->close()
##  + default just returns $cli
sub close {
  my $cli = shift;
  $cli->{db}->close() if ($cli->{db});
  return $cli;
}

## $bool = $cli->opened()
##  + default just checks for $cli->{url}
sub opened {
  return ref($_[0]) && $_[0]{ua};
}

##==============================================================================
## Profiling

##--------------------------------------------------------------
## Profiling: Generic: HTTP wrappers

## $obj_or_undef = $cli->jget(\%query_form,$class)
##  + wrapper for http requests
sub jget {
  my ($cli,$form,$class) = @_;
  my $uri = URI->new($cli->{url});
  $uri->query_form($form);
  my $req = HTTP::Request->new('GET',"$uri");
  $req->authorization_basic($cli->{user}, $cli->{password}) if (defined($cli->{user}) && defined($cli->{password}));
  my $rsp = $cli->{ua}->request($req);
  if (!$rsp->is_success) {
    $cli->{error} = $rsp->status_line;
    return undef;
  }
  my $cref = $rsp->content_ref;
  return $class->loadJsonString($cref,utf8=>!utf8::is_utf8($$cref));
}

##--------------------------------------------------------------
## Profiling: Generic

## $mprf_or_undef = $cli->profile($relation, %opts)
##  + get a relation profile for selected items as a DiaColloDB::Profile::Multi object
##  + %opts: as for DiaColloDB::profile()
##  + sets $cli->{error} on error
sub profile {
  my ($cli,$rel,%opts) = @_;
  return $cli->jget({profile=>$rel, %opts, format=>'json'},'DiaColloDB::Profile::Multi');
}

##--------------------------------------------------------------
## Profiling: Comparison (diff)

## $mprf = $cli->compare($relation, %opts)
##  + get a relation comparison profile for selected items as a DiaColloDB::Profile::MultiDiff object
##  + %opts: as for DiaColloDB::compare()
sub compare {
  my ($cli,$rel,%opts) = @_;
  return $cli->jget({profile=>"d$rel", %opts, format=>'json'},'DiaColloDB::Profile::MultiDiff');
}

##==============================================================================
## Footer
1;

__END__




