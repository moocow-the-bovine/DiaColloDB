#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>

#include <deque>
#include <map>
#include <utility>
#include <vector>
#include <string>

using namespace std;
typedef size_t id_t;
typedef size_t freq_t;

typedef deque<id_t>       win_t;

int main(int argc, char **argv)
{
  char   *buf = NULL;
  size_t  buflen = 0;
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
    for (win_t::const_iterator wj=win.begin(); wj!=win.end(); ++wj) {
      printf("%zi\t%zi\n%zi\t%zi\n", wi,*wj, *wj,wi);
    }
    if (win.size()==n) {
      win.pop_front();
    }
    win.push_back(wi);
  }

  return 0;
}
