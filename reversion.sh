#!/bin/bash

## + requires perl-reversion from Perl::Version (debian package libperl-version-perl)
## + example call:
##    ./reversion.sh -bump -dryrun

pmfiles=(./DiaColloDB.pm ./PDL-Utils/pdlutils.pd)

exec perl-reversion "$@" "${pmfiles[@]}"
