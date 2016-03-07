#!/bin/bash

svnroot=`svnroot.sh .`
svntags="${svnroot%/trunk}/tags";

show_help() {
    cat <<EOF >&2
Usage: $0 [OPTIONS]

Options:
  -h           # this help message
  -r           # show tag root
  -l           # list tags
  -t TAG       # tag current version as TAG

EOF
}

list_tags() {
    svn ls "$svntags";
}
set_tag() {
    svntag="$1"
    if [ -z "$svntag" ] ; then
	echo "$0: no TAG specified" >&2
	show_help
	exit 2
    fi
    echo svn cp "$svnroot" "$svntags/$svntag" -m "+ tagged $svntag"
}

case "$1" in
    -h|--help)
	show_help
	;;
    -r|--root)
	echo "$svntags"
	;;
    -l|--list)
	list_tags
	exit $?
	;;
    -t|--tag)
	set_tag "$2"
	exit $?
	;;
    *)
	show_help;
	exit 1;
	;;
esac;
exit 0;
	
