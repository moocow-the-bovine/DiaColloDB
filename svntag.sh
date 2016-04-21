#!/bin/bash

svnroot=`svnroot.sh .`
svntags="${svnroot%/trunk}/tags";
tagprefix=$(basename $(readlink -f $(dirname "$0")))
tagversion=$(perl-reversion -dryrun *.pm | tail -n1 | awk '{print $5}')
dummy=""

show_help() {
    cat <<EOF >&2
Usage: $0 [OPTIONS]

Options:
  -h           # this help message
  -r           # show tag root
  -l           # list tags
  -L           # list branches
  -d           # dummy mode
  -v VERSION   # override tag version (default=$tagversion)
  -p PREFIX    # override tag prefix (default=$tagprefix)
  -t           # tag current version as PREFIX-VERSION
  -t TAG       # tag current version as TAG
  -b TAG       # branch current version as TAG

EOF
}

runcmd() {
    echo "$0: $*" >&2
    test -n "$dummy" && return
    "$@"
}

list_tags() {
    runcmd svn ls "$svntags";
}
set_tag() {
    svntag="$1"
    svnact="$2"
    test -z "$svnact" && svnact="tagged"
    test -z "$svntag" && svntag="${tagprefix}-${tagversion}"
    if [ -z "$svntag" ] ; then
	echo "$0: no TAG specified" >&2
	show_help
	exit 2
    fi
    runcmd svn cp "$svnroot" "$svntags/$svntag" -m "+ $svnact $svntag"
}

while [ $# -gt 0 ] ; do
    case "$1" in
	-h|--help)
	    show_help
	    ;;
	-d|--dry-run|--no-act)
	    dummy=1
	    ;;
	-r|--root)
	    echo "$svntags"
	    ;;
	-l|--list-tags|--list)
	    list_tags
	    exit $?
	    ;;
	-L|--list-branches|--branches)
	    svntags="${svntags%tags}branches"
	    list_tags
	    exit $?
	    ;;
	-t|--tag)
	    set_tag "$2"
	    exit $?
	    ;;
	-b|--branch)
	    if test -z "$2"; then
		echo "$0: TAG argument required for branch creation with --branch" >&2
		exit 1
	    fi
	    svntags="${svntags%tags}branches"
	    set_tag "$2" "created branch"
	    exit $?
	    ;;
	*)
	    echo "$0: unknown option '$1'" >&2
	    show_help;
	    exit 1;
	    ;;
    esac;
    shift
done;
    
