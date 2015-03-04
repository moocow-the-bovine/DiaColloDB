## -*- Mode: CPerl -*-
## File: DiaColloDB::Document::DDCTabs.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, source document, DDC tab-dump

package DiaColloDB::Document::DDCTabs;
use DiaColloDB::Document;
use IO::File;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Document);

##==============================================================================
## Constructors etc.

## $doc = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    ##-- parsing options
##    eosre => $re,       ##-- EOS regex (empty or undef for file-breaks only; default='^$')
##    utf8  => $bool,     ##-- enable utf8 parsing? (default=1)
##    ##
##    ##-- document data
##    date   =>$date,     ##-- year
##    wf     =>$iw,       ##-- index-field for $word attribute (default=0)
##    pf     =>$ip,       ##-- index-field for $pos attribute (default=1)
##    lf     =>$il,       ##-- index-field for $lemma attribute (default=2)
##    tokens =>\@tokens,  ##-- tokens, including undef for EOS
##    meta   =>\%meta,    ##-- document metadata (e.g. author, title, collection, ...)
##   )
## + each token in @tokens is a HASH-ref {w=>$word,p=>$pos,l=>$lemma,...}
sub new {
  my $that = shift;
  my $doc  = $that->SUPER::new(
			       utf8=>1,
			       eosre=>qr{^$},
			       wf=>0,
			       pf=>1,
			       lf=>2,
			       @_, ##-- user arguments
			      );
  return $doc;
}

##==============================================================================
## API: I/O

##--------------------------------------------------------------
## API: I/O: parse

## $bool = $doc->fromFile($filename_or_fh, %opts)
##  + parse tokens from $filename_or_fh
##  + %opts : clobbers %$doc
sub fromFile {
  my ($doc,$file,%opts) = @_;
  $doc = $doc->new() if (!ref($doc));
  @$doc{keys %opts} = values %opts;
  $doc->{label} = ref($file) ? "$file" : $file;
  my $fh = ref($file) ? $file : IO::File->new("<$file");
  $doc->logconfess("fromFile(): cannot open file '$file': $!") if (!ref($fh));
  binmode($fh,':utf8') if ($doc->{utf8});

  my ($wf,$pf,$lf) = @$doc{qw(wf pf lf)};
  my $tokens   = $doc->{tokens};
  @$tokens     = qw();
  my $meta     = $doc->{meta};
  %$meta       = qw();
  my $eos      = undef;
  my $eosre    = $doc->{eosre};
  $eosre       = qr{$eosre} if ($eosre && !ref($eosre));
  my $last_was_eos = 1;
  my ($w,$p,$l);
  while (defined($_=<$fh>)) {
    chomp;
    if ($eosre && $_ =~ $eosre) {
      push(@$tokens,$eos) if (!$last_was_eos);
      $last_was_eos = 1;
      next;
    }
    if (/^%%/) {
      if (/^%%(?:\$DDC:meta\.date_|\$?date)=([0-9]+)/) {
	$doc->{date} = $1;
      }
      if (/^%%\$DDC:meta\.([^=]+)=(.*)$/) {
	$meta->{$1} = $2;
      }
      elsif (/^%%\$DDC:index\[([0-9]+)\]=Token\b/ || /^%%\$DDC:index\[([0-9]+)\]=\S+ w$/) {
	$wf = $doc->{wf} = $1;
      }
      elsif (/^%%\$DDC:index\[([0-9]+)\]=Pos\b/ || /^%%\$DDC:index\[([0-9]+)\]=\S+ p$/) {
	$pf = $doc->{pf} = $1;
      }
      elsif (/^%%\$DDC:index\[([0-9]+)\]=Lemma\b/ || /^%%\$DDC:index\[([0-9]+)\]=\S+ l$/) {
	$lf = $doc->{lf} = $1;
      }
      next;
    }
    ($w,$p,$l) = (split(/\t/,$_))[$wf,$pf,$lf];
    push(@$tokens, {w=>($w//''), p=>($p//''), l=>($l//'')});
    $last_was_eos = 0;
  }
  push(@$tokens,$eos) if (!$last_was_eos);

  $fh->close() if (!ref($file));
  return $doc;
}

##==============================================================================
## Footer
1;

__END__




