## -*- Mode: CPerl -*-
## File: DiaColloDB::Corpus.pm
## Author: Bryan Jurish <moocow@cpan.org>
## Description: collocation db, source corpous

package DiaColloDB::Corpus;
use DiaColloDB::Document;
use DiaColloDB::Document::DDCTabs;
use DiaColloDB::Logger;
use strict;

##==============================================================================
## Globals & Constants

our @ISA = qw(DiaColloDB::Logger);

our $DCLASS_DEFAULT = 'DDCTabs';

##==============================================================================
## Constructors etc.

## $corpus = CLASS_OR_OBJECT->new(%args)
## + %args, object structure:
##   (
##    files => \@files,   ##-- source files
##    dclass => $class,   ##-- DiaColloDB::Document subclass for loading (default=$DCLASS_DEFAULT)
##    cur    => $i,       ##-- index of current file
##   )
sub new {
  my $that = shift;
  my $corpus  = bless({
		       files => [],
		       dclass => $DCLASS_DEFAULT,
		       cur => 0,

		       @_, ##-- user arguments
		      },
		      ref($that)||$that);
  return $corpus;
}

##==============================================================================
## API: open/close

## $bool = $corpus->open(\@ARGV, %opts)
##  + %opts:
##     glob => $bool,     ##-- whether to glob arguments
##     list => $bool,     ##-- whether arguments are file-lists
sub open {
  my ($corpus,$sources,%opts) = @_;
  @{$corpus->{files}} = $opts{glob} ? (map {glob($_)} @$sources) : @$sources;
  if ($opts{list}) {
    ##-- read file-lists
    my $listfiles    = $corpus->{files};
    $corpus->{files} = [];
    foreach my $listfile (@$listfiles) {
      CORE::open(my $fh, "<$listfile")
	or $corpus->logconfess("open failed for list-file '$listfile': $!");
      push(@{$corpus->{files}}, grep {($_//'') ne ''} map {chomp; $_} <$fh>);
      CORE::close($fh);
    }
  }
  $corpus->{cur} = 0;

  ##-- setup document-class
  my $dclass = $corpus->{dclass} || 'DDCTabs';
  $dclass = $corpus->{dclass} = "DiaColloDB::Document::$dclass" if (!UNIVERSAL::isa($dclass,'DiaColloDB::Document'));

  return $corpus;
}

## $bool = $corpus->close()
sub close {
  my $corpus = shift;
  @{$corpus->{files}} = qw();
  $corpus->{cur} = 0;
  return $corpus;
}

##==============================================================================
## API: iteration

## $nfiles = $corpus->size()
sub size {
  return scalar(@{$_[0]{files}});
}

## undef = $corpus->ibegin()
##  + reset iterator
sub ibegin {
  $_[0]{cur}=0;
}

## $bool = $corpus->iok()
##  + true if iterator is valid
sub iok {
  return $_[0]{cur} <= $#{$_[0]{files}};
}

## $label = $corpus->ifile()
##  + current iterator label
sub ifile {
  return $_[0]{files}[$_[0]{cur}];
}

## $doc_or_undef = $corpus->idocument()
## $doc_or_undef = $corpus->idocument($pos)
##  + gets current document
sub idocument {
  my ($corpus,$pos) = @_;
  $pos //= $corpus->{cur};
  return undef if ($pos > $#{$corpus->{files}});
  return $corpus->{dclass}->fromFile($corpus->{files}[$pos]);
}

## $pos = $corpus->inext()
##  + increment iterator
sub inext {
  ++$_[0]{cur};
}


##==============================================================================
## Footer
1;

__END__




