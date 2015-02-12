#!/bin/bash

if [ $# -lt 2 ] ; then
  echo "USAGE: $0 DBDIR WORD(s)..." >&2
  exit 1
fi
dbdir="$1"
shift;

for w in "$@" ; do
  sudo sh -c 'echo 1 > /proc/sys/vm/drop_caches'; ##-- clear filesystem cache
  echo -en "$w\t";
  cmd=(/usr/bin/time -f '%e' -o /dev/stdout ./coldb-profile.perl "$dbdir" "$w" -strings -nomi -nold -noout);

  t1=`"${cmd[@]}" 2>/dev/null`;
  echo -en "$t1\t";

  t2=`"${cmd[@]}" 2>/dev/null`;
  echo -e "$t2";
done
