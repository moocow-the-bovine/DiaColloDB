#!/usr/bin/perl -w

my $date = 0;
my (@f);
while (defined($_=<>)) {
  chomp;
  if (/^%%\$DDC:meta\.date_=([0-9]+)/) {
    $date = $1;
  }
  next if (/^%%/);
  if (/^$/) {
    print "\n";
  } else {
    @f = split(/\t/,$_);
    print join("\t", $date, @f[0,2]), "\n";
  }
}
