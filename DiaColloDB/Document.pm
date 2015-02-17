## -*- Mode: CPerl -*-
## File: DiaColloDB::Document.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, source document

package DiaColloDB::Document;
use DiaColloDB::Logger;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

##==============================================================================
## Constructors etc.

## $doc = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    label  => $label,   ##-- document label (e.g. filename; optional)
##    date   =>$date,     ##-- year
##    eos    =>$eos,      ##-- special token to use for EOS; default=['__$','__$']
##    tokens =>\@tokens,  ##-- tokens, including EOS
##   )
## + each token in @tokens is a HASH-ref {w=>$word,p=>$pos,l=>$lemma,...}, or undef for EOS
sub new {
  my $that = shift;
  my $doc  = bless({
		    label=>undef,
		    date=>0,
		    tokens=>[],
		    @_, ##-- user arguments
		   },
		   ref($that)||$that);
  return $doc;
}

##==============================================================================
## API: I/O

## $bool = $doc->fromFile($filename_or_fh)
##  + parse tokens from $filename_or_fh
sub fromFile {
  my ($doc,$file) = @_;
  $doc->logconfess("fromFile() not implemented for '$file': $!");
}

## $label = $doc->label()
sub label {
  return $_[0]{label} // "$_[0]";
}

##==============================================================================
## Footer
1;

__END__




