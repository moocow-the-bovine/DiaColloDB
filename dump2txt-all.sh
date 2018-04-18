#!/bin/bash

corpora=(pnn kern zeit)
for c in "${corpora[@]}" ; do
    echo "### $0: BEGIN convert corpus $c ###" >&2
    cat ~/dstar/corpora/$c/build/ddc_dump/corpus-tabs.files \
	| perl -pe "print qq{$HOME/dstar/corpora/$c/build/ddc_dump/};" \
	| ./dump2txt.perl -l -w='$w{l}' -k='$w{p}=~q/^(?:N|TRUNC|VV|ADJ)/ && $w{w} =~ /[[:alpha:]]/ && $w{w} !~ /\./' "$@" -o="$c.txt"
    echo -e "### $0: END convert corpus $c ###\n" >&2
done
