## -*- Mode: CPerl -*-
##
## File: DiaColloDB::PDL::MM.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: DiaColloDB utilities: (temporary) mmaped PDLs

package DiaColloDB::PDL::MM;
use DiaColloDB::Logger;
use PDL;
use PDL::IO::FastRaw;
use File::Temp;

##==============================================================================
## Globals & Constants

our @ISA = qw(PDL DiaColloDB::Logger);
our %MMTMP = qw();		##-- all tempfiles created, for END block
our $LOG_DEFAULT = undef;	##-- default log-level (undef: off)

##==============================================================================
## Constructors etc.

## $mmpdl = CLASS->new($file?, $type?, @dims, \%opts?)
## $mmpdl = $pdl->mmzeroes($file?, $type?, \%opts?)
##  + %opts, %$mmpdl:
##    (
##     file => $template,   ##-- file basename or File::Temp template; default='pdlXXXX'
##     suffix => $suffix,   ##-- File::Temp::tempfile() suffix (default='.pdl')
##     log  => $level,      ##-- logging verbosity (default=$LOG_DEFAULT)
##     temp => $bool,       ##-- delete on END (default: $file =~ /X{4}/)
##     PDL  => $pdl,        ##-- guts: real underlying mmap()ed piddle (must be key 'PDL' for PDL inheritance to work)
##    )
sub new {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $src  = UNIVERSAL::isa($_[0],'PDL') ? shift : undef;
  my $file = !ref($_[0]) && $_[0] !~ /^[0-9]+$/ ? shift : undef;
  my $type = UNIVERSAL::isa($_[0],'PDL::Type') ? shift : (defined($src) ? $src->type : PDL::double());
  my $opts = UNIVERSAL::isa($_[$#_],'HASH') ? pop(@_) : {};
  my @dims = defined($src) ? $src->dims : @_;
  my $suffix = $opts->{suffix} // '.pdl';
  my $temp   = $opts->{temp};
  my $log    = $opts->{log} // $LOG_DEFAULT;
  $file    //= $opts->{file} // 'pdlXXXXX';

  ##-- maybe use File::Temp to get filename
  if ($file =~ /X{4}/) {
    my ($tmpfh,$tmpfile) = File::Temp::tempfile($file, SUFFIX=>$suffix, TMPDIR=>1, UNLINK=>0);
    $tmpfh->close();
    $file  = $tmpfile;
    $temp //= 1;
  }

  ##-- remove any stale data
  $that->unlink($file);

  ##-- track tempfiles for END block
  if ($temp) {
    $MMTMP{$file} = undef;
  } else {
    delete $MMTMP{$file};
  }

  ##-- guts
  $that->vlog($log, "CREATE $file -> $type \[".join(',',@dims)."] (".($temp ? "TEMP" : "KEEP").")");
  my $pdl = PDL::IO::FastRaw::mapfraw($file, {Dims=>\@dims, Datatype=>$type, Creat=>1});
  return bless({
		PDL =>$pdl,
		file=>$file,
		temp=>($temp||0),
		log =>$log,
	       }, ref($that)||$that);
}
BEGIN {
  *PDL::mmzeroes = \&new;
}

## $mmpdl = CLASS->mmtemp($file?, $type?, @dims, \%opts?)
##  + like new(), but always sets $opts->{temp}=1
sub mmtemp {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $opts = UNIVERSAL::isa($_[$#_],'HASH') ? pop(@_) : {};
  $opts->{temp} = 1;
  return $that->new(@_,$opts);
}
BEGIN {
  *PDL::mmtemp = \&mmtemp;
}


## $bool = CLASS->unlink(@basenames)
## $bool = $mmpdl->unlink()
##  + unlinks file(s) generated by mmzeroes($basename)
##  + $basename defaults to $obj->{file} if $
sub unlink {
  my $that = UNIVERSAL::isa($_[0],__PACKAGE__) ? shift : __PACKAGE__;
  my $rc   = 1;
  foreach my $file (grep {defined($_)} ($that->file, @_)) {
    $that->vlog($that->{log}, "UNLINK ${file}") if (UNIVERSAL::isa($that,'HASH'));
    foreach (grep {-e "$file$_"} ('','.hdr')) {
      $rc &&= CORE::unlink("$file$_") or do { $that->logwarn("unlink(): failed to unlink tempfile '$file$_': $!"); 0 };
    }
    delete $MMTMP{$file} if ($rc);
  }
  return $rc;
}

## undef = $obj->DESTROY()
##  + must also handle "pure" piddles created e.g. by $mmpdl->xvals()
sub DESTROY {
  my $obj = shift;
  return if (!UNIVERSAL::isa($obj,'HASH'));
  delete $obj->{pdl};
  $obj->unlink() if ($obj->{temp});
}

##==============================================================================
## Accessors

## $file = $mmdpl->file()
sub file {
  return undef if (!UNIVERSAL::isa($_[0],'HASH'));
  return $_[0]{file};
}

## $istmp = $mmdpl->temp()
sub temp {
  return undef if (!UNIVERSAL::isa($_[0],'HASH'));
  return $_[0]{temp};
}

## $pdl = $mmpdl->_pdl()
sub _pdl {
  return $_[0] if (!UNIVERSAL::isa($_[0],'HASH'));
  return $_[0]{PDL};
}

##==============================================================================
## final cleanup in END block
END {
  #DiaColloDB::PDL::MM->trace("END");
  DiaColloDB::PDL::MM::unlink(keys %MMTMP);
}

##==============================================================================
## Footer
1; ##-- be happy