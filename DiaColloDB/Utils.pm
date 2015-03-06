## -*- Mode: CPerl -*-
##
## File: DiaColloDB::Utils.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: generic DiaColloDB utilities

package DiaColloDB::Utils;
use DiaColloDB::Logger;
use Exporter;
use JSON;
use IO::Handle;
use IO::File;
use IPC::Run;
use Fcntl qw(:DEFAULT SEEK_SET SEEK_CUR SEEK_END);
use Time::HiRes qw(gettimeofday tv_interval);
use Carp;
use strict;

##==============================================================================
## Globals

our @ISA = qw(Exporter DiaColloDB::Logger);
our @EXPORT= qw();
our %EXPORT_TAGS =
    (
     fcntl => [qw(fcflags fcread fcwrite fctrunc fccreat fcperl fcopen)],
     json  => [qw(loadJsonString loadJsonFile saveJsonString saveJsonFile)],
     sort  => [qw(csort_to csortuc_to)],
     run   => [qw(crun opencmd)],
     env   => [qw(env_set env_push env_pop)],
     pack  => [qw(packsize packFilterFetch packFilterStore)],
     math  => [qw($LOG2 log2)],
     regex => [qw(regex)],
     html  => [qw(htmlesc)],
     time  => [qw(s2hms s2timestr)],
    );
our @EXPORT_OK = map {@$_} values(%EXPORT_TAGS);
$EXPORT_TAGS{all} = [@EXPORT_OK];

##==============================================================================
## Functions: Fcntl

## $flags = PACKAGE::fcflags($flags)
##  + returns Fcntl flags for symbolic string $flags
sub fcflags {
  my $flags = shift;
  $flags //= 'r';
  return $flags if ($flags =~ /^[0-9]+$/); ##-- numeric flags are interpreted as Fcntl bitmask
  my $fread  = $flags =~ /[r<]/;
  my $fwrite = $flags =~ /[wa>\+]/;
  my $fappend = ($flags =~ /[a]/ || $flags =~ />>/);
  my $iflags = ($fread
		? ($fwrite ? (O_RDWR|O_CREAT)   : O_RDONLY)
		: ($fwrite ? (O_WRONLY|O_CREAT) : 0)
	       );
  $iflags |= O_TRUNC  if ($fwrite && !$fappend);
  return $iflags;
}

## $bool = fcread($flags)
##  + returns true if any read-bits are set for $flags
sub fcread {
  my $flags = fcflags(shift);
  return ($flags&O_RDONLY)==O_RDONLY || ($flags&O_RDWR)==O_RDWR;
}

## $bool = fcwrite($flags)
##  + returns true if any write-bits are set for $flags
sub fcwrite {
  my $flags = fcflags(shift);
  return ($flags&O_WRONLY)==O_WRONLY || ($flags&O_RDWR)==O_RDWR;
}

## $bool = fctrunc($flags)
##  + returns true if truncate-bits are set for $flags
sub fctrunc {
  my $flags = fcflags(shift);
  return ($flags&O_TRUNC)==O_TRUNC;
}

## $bool = fccreat($flags)
sub fccreat {
  my $flags = fcflags(shift);
  return ($flags&O_CREAT)==O_CREAT;
}

## $str = fcmode($flags)
##  + return perl mode-string for $flags
sub fcperl {
  my $flags = fcflags(shift);
  return (fcread($flags)
	  ? (fcwrite($flags)    ##-- +read
	     ? (fctrunc($flags) ##-- +read,+write
		? '+>' : '+<')  ##-- +read,+write,+/-trunc
	     : '<')
	  : (fcwrite($flags)    ##-- -read
	     ? (fctrunc($flags) ##-- -read,+write
		? '>' : '>>')   ##-- -read,+write,+/-trunc
	     : '<')             ##-- -read,-write : default
	 );
}

## $fh_or_undef = fcopen($file,$flags)
## $fh_or_undef = fcopen($file,$flags,$mode,$perms)
##  + opens $file with fcntl-style flags $flags
sub fcopen {
  my ($file,$flags,$perms) = @_;
  $flags    = fcflags($flags);
  $perms  //= (0666 & ~umask);
  my $mode = fcperl($flags);

  my ($sysfh);
  if (ref($file)) {
    ##-- dup an existing filehandle
    $sysfh = $file;
  }
  else {
    ##-- use sysopen() to honor O_CREAT and O_TRUNC
    sysopen($sysfh, $file, $flags, $perms) or return undef;
  }

  ##-- now open perl-fh from system fh
  open(my $fh, "${mode}&=", fileno($sysfh)) or return undef;
  if (fcwrite($flags) && !fctrunc($flags)) {
    ##-- append mode: seek to end of file
    seek($fh, 0, SEEK_END) or return undef;
  }
  return $fh;
}

##==============================================================================
## Functions: JSON

##--------------------------------------------------------------
## JSON: load

## $data = PACKAGE::loadJsonString( $string,%opts)
## $data = PACKAGE::loadJsonString(\$string,%opts)
sub loadJsonString {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $bufr = ref($_[0]) ? $_[0] : \$_[0];
  return from_json($$bufr, {utf8=>!utf8::is_utf8($$bufr), relaxed=>1, allow_nonref=>1, @_[1..$#_]});
}

## $data = PACKAGE::loadJsonFile($filename_or_handle,%opts)
sub loadJsonFile {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $file = shift;
  my $fh = ref($file) ? $file : IO::File->new("<$file");
  return undef if (!$fh);
  binmode($fh,':raw');
  local $/=undef;
  my $buf = <$fh>;
  close($fh) if (!ref($file));
  return $that->loadJsonString(\$buf,@_);
}

##--------------------------------------------------------------
## JSON: save

## $str = PACKAGE::saveJsonStringString($data)
## $str = PACKAGE::saveJsonStringString($data,%opts)
##  + %opts passed to JSON::to_json(), e.g. (pretty=>0, canonical=>0)'
sub saveJsonString {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $data = shift;
  return to_json($data, {utf8=>1, allow_nonref=>1, allow_unknown=>1, allow_blessed=>1, convert_blessed=>1, pretty=>1, canonical=>1, @_});
}

## $bool = PACKAGE::saveJsonFile($data,$filename_or_handle,%opts)
sub saveJsonFile {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $data = shift;
  my $file = shift;
  my $fh = ref($file) ? $file : IO::File->new(">$file");
  $that->logconfess("saveJsonFile() failed to open file '$file': $!") if (!$fh);
  binmode($fh,':raw');
  $fh->print($that->saveJsonString($data,@_)) or return undef;
  if (!ref($file)) { close($fh) || return undef; }
  return 1;
}

##==============================================================================
## Functions: env


## \%setenv = PACKAGE::env_set(%setenv)
sub env_set {
  my $that   = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my %setenv = @_;
  my ($key,$val);
  while (($key,$val)=each(%setenv)) {
    if (!defined($val)) {
      delete($ENV{$key});
    } else {
      $ENV{$key} = $val;
    }
  }
  return \%setenv;
}

## \%oldvals = PACKAGE::env_push(%setenv)
our @env_stack = qw();
sub env_push {
  my $that   = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my %setenv = @_;
  my %oldenv = map {($_=>$ENV{$_})} keys %setenv;
  push(@env_stack, \%oldenv);
  $that->env_set(%setenv);
  return \%oldenv;
}

## \%restored = PACKAGE::env_pop(%setenv)
sub env_pop {
  my $that    = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $oldvals = pop(@env_stack);
  $that->env_set(%$oldvals) if ($oldvals);
  return $oldvals;
}


##==============================================================================
## Functions: run

## $fh_or_undef = PACKAGE::opencmd($cmd)
## $fh_or_undef = PACKAGE::opencmd($mode,@argv)
##  + does log trace at level $TRACE_RUNCMD
sub opencmd {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  $that->trace("CMD ", join(' ',@_));
  my $fh = IO::Handle->new();
  if ($#_ > 0) {
    open($fh,$_[0],$_[1],@_[2..$#_])
  } else {
    open($fh,$_[0]);
  }
  $that->logconfess("opencmd() failed for \`", join(' ',@_), "': $!") if (!$fh);
  return $fh;
}

## $bool = crun(@IPC_Run_args)
##  + wrapper for IPC::Run::run(@IPC_Run_args) with $ENV{LC_ALL}='C'
sub crun {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  $that->trace("RUN ", join(' ',
			    map {
			      (ref($_)
			       ? (ref($_) eq 'ARRAY'
				  ? join(' ', @$_)
				  : ref($_))
			       : $_)
			    } @_));
  $that->env_push(LC_ALL=>'C');
  my $rc = IPC::Run::run(@_);
  $that->env_pop();
  return $rc;
}

## $bool = csort_to(\@sortargs, \&catcher)
##  + runs system sort and feeds resulting lines to \&catcher
sub csort_to {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my ($sortargs,$catcher) = @_;
  return crun(['sort',@$sortargs], '>', IPC::Run::new_chunker("\n"), $catcher);
}

## $bool = csortuc_to(\@sortargs, \&catcher)
##  + runs system sort | uniq -c and feeds resulting lines to \&catcher
sub csortuc_to {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my ($sortargs,$catcher) = @_;
  return crun(['sort',@$sortargs], '|', [qw(uniq -c)], '>', IPC::Run::new_chunker("\n"), $catcher);
}


##==============================================================================
## Functions: pack filters

## $len = PACKAGE::packsize($packfmt)
## $len = PACKAGE::packsize($packfmt,@args)
##  + get pack-size for $packfmt with args @args
sub packsize {
  use bytes;
  no warnings;
  return length(pack($_[0],@_[1..$#_]));
}

## \&filter_sub = PACKAGE::packFilterStore($pack_template)
## \&filter_sub = PACKAGE::packFilterStore([$pack_template_store, $pack_template_fetch])
## \&filter_sub = PACKAGE::packFilterStore([\&pack_code_store,   \&pack_code_fetch])
##   + returns a DB_File-style STORE-filter sub for transparent packing of data to $pack_template
sub packFilterStore {
  my $that   = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $packas = shift;
  $packas    = $packas->[0] if (UNIVERSAL::isa($packas,'ARRAY'));
  return $packas  if (UNIVERSAL::isa($packas,'CODE'));
  return undef    if (!$packas || $packas eq 'raw');
  if (packsize($packas,0)==packsize($packas,0,0)) {
    return sub {
      $_ = pack($packas,$_) if (defined($_));
    };
  } else {
    return sub {
      $_ = pack($packas, ref($_) ? @$_ : split(/\t/,$_)) if (defined($_));
    };
  }
}

## \&filter_sub = PACKAGE::packFilterFetch($pack_template)
## \&filter_sub = PACKAGE::packFilterFetch([$pack_template_store, $pack_template_fetch])
## \&filter_sub = PACKAGE::packFilterFetch([\&pack_code_store,   \&pack_code_fetch])
##   + returns a DB_File-style FETCH-filter sub for transparent unpacking of data from $pack_template
sub packFilterFetch {
  my $that   = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $packas = shift;
  $packas    = $packas->[1] if (UNIVERSAL::isa($packas,'ARRAY'));
  return $packas  if (UNIVERSAL::isa($packas,'CODE'));
  return undef    if (!$packas || $packas eq 'raw');
  if (packsize($packas,0)==packsize($packas,0,0)) {
    return sub {
      $_ = unpack($packas,$_);
    };
  } else {
    return sub {
      $_ = [unpack($packas,$_)];
    }
  }
}

##==============================================================================
## Math stuff

our ($LOG2);
BEGIN {
  $LOG2 = log(2.0);
}

## $log2 = log2($x)
sub log2 {
  return $_[0]==0 ? -inf : log($_[0])/$LOG2;
}


##==============================================================================
## Functions: regexes

## $re = regex($re_str)
##  + parses "/"-quoted regex $re_str
##  + parses modifiers /[gimsadlu] a la ddc
sub regex {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $re = shift;
  return $re if (ref($re));
  $re =~ s/^\s*\///;

  my $mods = ($re =~ s/\/([gimsadlu]*)\s*$// ? $1 : '');
  if ($mods =~ s/g//g) {
    $re = "^(?${mods}:${re})\$";  ##-- parse /g modifier a la ddc
  } elsif ($mods) {
    $re = "(?${mods}:$re)";
  }
  return qr{$re};
}

##==============================================================================
## Functions: html

## $escaped = htmlesc($str)
sub htmlesc {
  ##-- html escapes
  my $str = shift;
  $str =~ s/\&/\&amp;/sg;
  $str =~ s/\'/\&#39;/sg;
  $str =~ s/\"/\&quot;/sg;
  $str =~ s/\</\&lt;/sg;
  $str =~ s/\>/\&gt;/sg;
  return $str;
}

##==============================================================================
## Functions: time

## $hms       = PACKAGE::s2hms($seconds,$sfmt="%06.3f")
## ($h,$m,$s) = PACKAGE::s2hms($seconds,$sfmt="%06.3f")
sub s2hms {
  shift(@_) if (UNIVERSAL::isa($_[0],__PACKAGE__));
  my ($secs,$sfmt) = @_;
  $sfmt ||= '%06.3f';
  my $h  = int($secs/(60*60));
  $secs -= $h*60*60;
  my $m  = int($secs/60);
  $secs -= $m*60;
  my $s = sprintf($sfmt, $secs);
  return wantarray ? ($h,$m,$s) : sprintf("%02d:%02d:%s", $h,$m,$s);
}

## $timestr = PACKAGE::s2timestr($seconds,$sfmt="%f")
sub s2timestr {
  shift(@_) if (UNIVERSAL::isa($_[0],__PACKAGE__));
  my ($h,$m,$s) = s2hms(@_);
  if ($h==0 && $m==0) {
    $s =~ s/^0+(?!\.)//;
    return "${s}s";
  }
  elsif ($h==0) {
    return sprintf("%2dm%ss",$m,$s)
  }
  return sprintf("%dh%02dm%ss",$h,$m,$s);
}

##==============================================================================
## Footer
1; ##-- be happy