#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>

#include <deque>
#include <vector>
#include <map>
#include <string>
#include <utility>

using namespace std;
typedef size_t id_t;
typedef char   dist_t;
typedef size_t freq_t;

typedef deque<id_t>       win_t;
typedef pair<id_t,id_t>   idpair_t;
typedef pair<idpair_t,dist_t>  cofkey_t;
typedef map<cofkey_t,freq_t>   cof_t;

int main(int argc, char **argv)
{
  char   *buf = NULL;
  size_t  buflen = 0;
  cof_t   cof;
  win_t   win;
  size_t n = 1;

  if (argc > 1) {
    n = strtoul(argv[1],NULL,0);
  }

  while (getline(&buf,&buflen,stdin) != -1) {
    if (buf[0] == '\0' || buf[0] == '\n') {
      //-- eos
      win.clear();
      continue;
    }
    id_t wi = strtoul(buf, NULL, 0);
    for (size_t j=0; j<win.size(); ++j) {
      dist_t    dist = win.size() - j;
      cofkey_t keyij = make_pair(make_pair(wi, win[j]), -dist);
      cofkey_t keyji = make_pair(make_pair(win[j], wi),  dist);
      ++cof[keyij];
      ++cof[keyji];
    }
    if (win.size()==n) {
      win.pop_front();
    }
    win.push_back(wi);
  }

  //-- dump
  for (cof_t::const_iterator cofi=cof.begin(); cofi!=cof.end(); ++cofi) {
    printf("%zi\t%zi\t%zi\t%hhi\n", cofi->second, cofi->first.first.first, cofi->first.first.second, cofi->first.second);
  }

  //-- show memory usage (67.9 %mem on plato (4GB) for kern01,n=5; rss=2749240 vsz=2751540 time=1m14s
  string cmd("ps -o pid,%mem,rss,vsz -p ");
  char cmdbuf[128];
  sprintf(cmdbuf, "ps -o pid,%%mem,rss,vsz -p %u 1>&2", getpid());
  int rc = system(cmdbuf);
  if (rc != 0) {
    fprintf(stderr, "command `%s' failed: %s", cmdbuf, strerror(errno));
  }

  return 0;
}
