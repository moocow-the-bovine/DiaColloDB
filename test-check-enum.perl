#!/usr/bin/perl -w

use lib qw(. lib);
use DiaColloDB;
use Getopt::Long;

our $mode = 'check';
our $outbase = 'check-enum.out';
our $help;
GetOptions(
	   'help|h' => \$help,
	   'check|c' => sub {$mode='check'},
	   'churn-array|array|a' => sub {$mode='churn-array'},
	   'churn-hash|hash|H'   => sub {$mode='churn-hash'},
	   'output|o=s' => \$outbase,
	  );

if (!@ARGV || $help) {
  print STDERR <<EOF;
Usage: $0 [OPTIONS] ENUM

 Options:
   -help       # this help message
   -check      # check enum consistency
   -array      # churn via array
   -hash       # churn via hash
   -o BASE     # churn output basename (default=$outbase)

EOF
  exit 1;
}

##==============================================================================
## subs

sub enum_check {
  my $enum = shift;

  ##-- variables
  use bytes;
  my ($sbufr,$sxbufr,$ixbufr) = @$enum{qw(sbufr sxbufr ixbufr)};
  my ($pack_l,$len_l,$pack_i,$len_i,$pack_o,$len_o,$len_sx) = @$enum{qw(pack_l len_l pack_i len_i pack_o len_o len_sx)};

  ##-- read sx records
  DiaColloDB->ensureLog();
  $enum->debug("reading sx records");
  my ($sx_off,$o,$i);
  my $sx_size = length($$sxbufr);
  my $pack_sx = $pack_o.$pack_i;
  my @i2sx    = qw(); ##-- $i2sx[$i] = [$off,$si]
  for ($sx_off=0; $sx_off < $sx_size; $sx_off += $len_sx) {
    ($o,$i)   = unpack($pack_sx,substr($$sxbufr,$sx_off,$len_sx));
    $i2sx[$i] = [$o,$sx_off/$len_sx];
  }
  $enum->debug(scalar(@i2sx), " sx-records loaded");

  ##-- check ix records
  $enum->debug("checking sx<->ix consistency");
  my $ix_size = length($$ixbufr);
  my ($ix_off,$i_off, $sx);
  for ($ix_off=0; $ix_off < $ix_size; $ix_off += $len_o) {
    $i     = $ix_off / $len_o;
    $i_off = unpack($pack_o, substr($$ixbufr, $ix_off, $len_o));

    $sx = $i2sx[$i];
    if ($sx->[0] != $i_off) {
      die("$0: sx<->ix offset mismatch for i=$i, sxi=$sx->[1]: sx-offset=$sx->[0] != ix-offset=$i_off");
    }
  }
  $enum->debug(($ix_size/$len_o)." ix-records checked out ok");

  ##-- check s records
  $enum->debug("checking s<->sx consistency");
  my $s_size  = length($$sbufr);
  my $n_s     = 0;
  my ($s_off,$s_len,$s_buf);
  for ($s_off=0, $i=0; $s_off < $s_size; ++$i) {
    $s_len = unpack($pack_l,substr($$sbufr,$s_off,$len_l));
    $s_buf = substr($$sbufr,$s_off+$len_l,$s_len);
    $s_buf = substr($s_buf,0,16)."..." if (length($s_buf) > 16);

    ##-- check for offset mismatch
    $sx = $i2sx[$i];
    if ($sx->[0] != $s_off) {
      die("$0: s<->sx offset mismatch for i=$i, sxi=$sx->[1], s='$s_buf': sx-offset=$sx->[0] != s-offset=$s_off");
    }

    ##-- update
    $s_off += $len_l + $s_len;
    ++$n_s;
  }
  $enum->debug("$n_s s-records checked out ok");

  $enum->debug("enum $enum->{base} appears consistent");
}

sub churn_array {
  my $enum  = shift;
  my $i2s   = $enum->toArray;
  my $e2    = $enum->new->fromArray($i2s);
  $e2->save($outbase)
    or die("$0: failed to save to '$outbase': $!");
  $e2->info("saved enum $outbase.*");
}

sub churn_hash {
  my $enum  = shift;
  $enum->load();
  my $s2i = $enum->{s2i};
  my $e2  = $enum->new->fromHash($s2i);
  $e2->save($outbase)
    or die("$0: failed to save to '$outbase': $!");
  $e2->info("saved enum $outbase.*");
}


##==============================================================================
## MAIN

DiaColloDB->ensureLog();

##-- open enum
my $efile = shift(@ARGV);
my $enum  = DiaColloDB::EnumFile::MMap->new(base=>$efile)
  or die("$0: open failed for enum '$efile.*': $!");

##-- guts
if ($mode eq 'check') {
  enum_check($enum);
}
elsif ($mode eq 'churn-array') {
  churn_array($enum);
}
elsif ($mode eq 'churn-hash') {
  churn_hash($enum);
}
else {
  die("$0: unknown operation mode '$mode'");
}

