#!/usr/bin/perl -w

my %dat = qw(); ##-- $label => \@rows
my (%row);
while (<>) {
  chomp;
  if (/BENCH.*: (.*)$/) {
    %row = (qstr=>$1);
  }
  elsif (/: NT=(\d+) ND=(\d+) Nnz=(\d+).*?Nti=(\d+) Ndi=(\d+)/) {
    @row{qw(NT ND Nnz Nti Ndi)} = ($1,$2,$3,$4,$5);
  }
  elsif (m{^(\S+)\s+([0-9\.]+(?:/s)?)\s+.*?--}) {
    @row{qw(label rate)} = ($1,$2);
    $row{rate} = 1.0/$row{rate} if (!($row{rate} =~ s{/s$}{}));
    push(@{$dat{$row{label}}}, {%row});
  }
  elsif (/^--$/) {
    %row = qw();
  }
}

##-- dump data
foreach my $label (sort keys %dat) {
  print STDERR "$0: dumping xb-$label.dat\n";

  open(my $fh, ">:utf8", "xb-$label.dat") or die("$0: failed to open xb-$label.dat: $!");
  my @cols = qw(rate Nti Ndi NT ND Nnz label qstr);
  print $fh join("\t", map {"#".($_+1).":".$cols[$_]} (0..$#cols)), "\n";

  my $rows = $dat{$label};
  foreach my $row (sort {$a->{Nti}*$a->{Ndi} <=> $b->{Nti}*$b->{Ndi}} @$rows) {
    print $fh join("\t", @$row{@cols}), "\n";
  }
  close($fh);
}
