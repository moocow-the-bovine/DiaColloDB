#include "tok2cof.h"

//======================================================================
// common typedefs

typedef pair<TermIdT,DateT> WordT;
typedef pair<WordT,WordT> WordPairT;
typedef vector<WordT> SentenceT;
typedef map<WordPairT, FreqT> MapT;


//======================================================================
// subs

#ifndef T2C_GENERATE_ONLY
# define t2c_add(si,sj) \
    ++cof[ std::make_pair(*si,*sj) ]
#else
# define t2c_add(si,sj) \
      fprintf(ofp, "%zu\t%zu\t%zu\t%zu\n", \
              (size_t)si->first, (size_t)si->second, \
              (size_t)sj->first, (size_t)sj->second)
#endif

//#define T2C_DEBUG_NADD 1
#ifdef T2C_DEBUG_NADD
# define DEBUG_NADD(x) x
size_t nadds = 0;
#else
# define DEBUG_NADD(x)
#endif

void addSentence(const SentenceT& sent, MapT& cof, size_t dmax=5)
{
  if (sent.size() < 2) return;
  SentenceT::const_iterator si,sj;

  for (si=sent.begin(); si != sent.end(); ++si) {
    for (sj=max(si-dmax, sent.cbegin()); sj < si; ++sj) {
        t2c_add(si,sj);
        DEBUG_NADD(++nadds);
    }
    for (sj=si+1; sj != min(si+dmax+1, sent.cend()); ++sj) {
        t2c_add(si,sj);
        DEBUG_NADD(++nadds);
    }
  }
}


//======================================================================
int main(int argc, const char **argv)
{
  t2c_init(argc, argv);
  

  //-- guts
  MapT cof;
  SentenceT sent;
  char *buf=NULL, *dbuf, *dtail;
  size_t buflen = 0;
  size_t lineno = 0;
  ssize_t nread;
  WordT   w;
  while ( (nread=getline(&buf,&buflen,ifp)) > 0 ) {
    ++lineno;
    if (buf[0] == '\n') {
      //-- blank line: EOS
      addSentence(sent,cof);
      sent.clear();
    }
    else {
      //-- normal token
      w.first  = strtoul(buf,  &dbuf, 0);
      w.second = strtoul(dbuf, &dtail, 0);
      if (dbuf==buf || dtail==dbuf) {
        fprintf(stderr, "%s: error parsing %s line %zd: %s", prog, ifile, lineno, buf);
        exit(255);
      }
      sent.push_back(w);
    }
  }
  if (!feof(ifp)) {
    fprintf(stderr, "%s: error reading from %s: %s\n", prog, ifile, strerror(errno));
    exit(255);
  }
  if (buf) free(buf);
  addSentence(sent,cof);

#ifndef T2C_GENERATE_ONLY
  //-- dump
  for (MapT::const_iterator ci=cof.begin(); ci != cof.end(); ++ci) {
    fprintf(ofp, "%zu\t%zu\t%zu\t%zu\t%zu\n",
            (size_t)ci->second,
            (size_t)ci->first.first.first,
            (size_t)ci->first.first.second,
            (size_t)ci->first.second.first,
            (size_t)ci->first.second.second);
  }
#endif

  DEBUG_NADD(fprintf(stderr,"%s: number of atomic additions = %zd\n", prog, nadds));
  t2c_finish();
  return 0;
}
