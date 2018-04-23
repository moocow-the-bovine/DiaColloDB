/*-*- Mode: C++ -*-*/

#include "tdmModel.h"
#include "tdmIO.h"
using namespace std;

/*======================================================================
 * Globals & typedefs
 */
const char *prog = "dict-find";

typedef IdDefaultT IdT;
typedef Dict<IdT> DictT;
const IdT NoId = (IdT)-1;

struct mmDict {
  //-- mmap variables
  int            mmfd;
  off_t          mmsize;
  char 		*mmbuf;
  TdmBinHeaderT *mmhead;
  OffT          *mmdoff;
  IdT           *mmdsrt;
  char 		*mmdtrm;

  mmDict()
    : mmfd(-1), mmsize(0), mmbuf(NULL), mmhead(NULL), mmdoff(NULL), mmdsrt(NULL), mmdtrm(NULL)
  {};

  ~mmDict()
  { close(); };
  
  void open(const string& infile, int mode=O_RDONLY)
  {
    close();
    mmfd = ::open(infile.c_str(), mode);
    if (mmfd == -1)
      throw invalid_argument("openDict(): open failed for file '"+infile+"'");

    struct stat statbuf;
    if (fstat(mmfd, &statbuf) != 0)
      throw runtime_error("openDict(): fstat() failed for '" + infile +"'");
    mmsize = statbuf.st_size;

    //-- mmap file data
    int mmprot = (mode==O_RDONLY ? PROT_READ : (PROT_READ|PROT_WRITE));
    mmbuf = (char*)mmap(NULL, mmsize, mmprot, MAP_SHARED, mmfd, 0);
    if (mmbuf == MAP_FAILED)
      throw runtime_error("openDict(): mmap() failed for '"+infile+"'");

    //-- check header
    if ((size_t)mmsize < sizeof(TdmBinHeaderT))
      throw invalid_argument("openDict(): can't map header data from '"+infile+"'");
    mmhead = (TdmBinHeaderT*)mmbuf;
    mmhead->check(sizeof(IdT));

    //-- map dict: offsets
    size_t doffOffset = sizeof(TdmBinHeaderT);
    mmdoff = (OffT*)(&mmbuf[doffOffset]);

    //-- map dict: sort-ids
    size_t dsrtOffset = doffOffset + (mmhead->nterms+1)*sizeof(OffT);
    mmdsrt = (IdT*)(&mmbuf[dsrtOffset]);

    //-- map dict: string data
    size_t dtrmOffset = dsrtOffset + mmhead->nterms*sizeof(IdT);
    mmdtrm = (char*)(&mmbuf[dtrmOffset]);
  };

  void close()
  {
    if (mmbuf != NULL) {
      if (munmap(mmbuf, mmsize) != 0)
	throw runtime_error("closeDict(): munmap() failed");
      ::close(mmfd);
    }
    mmfd = -1;
    mmsize = 0;
    mmbuf = NULL;
    mmhead = NULL;
    mmdoff = NULL;
    mmdsrt = NULL;
    mmdtrm = NULL;
  };

  IdT str2id(const string &term) const;

  inline string id2str(IdT id) const
  {
    if (id >= mmhead->nterms)
      throw range_error("mmDict::id2str(): id out of range");
    return string( &mmdtrm[mmdoff[id]], mmdoff[id+1] - mmdoff[id] );    
  };
};

struct mmDictSortComp {
  const mmDict* dictp;

  mmDictSortComp(const mmDict *dictp_=NULL)
    : dictp(dictp_)
  {};

  inline bool operator()(const IdT& id, const string& str) const
  {
    string  ds( &dictp->mmdtrm[dictp->mmdoff[id]], dictp->mmdoff[id+1] - dictp->mmdoff[id] );
    return ds < str;
  };
};


IdT mmDict::str2id(const string& term) const
{
  IdT *idp = lower_bound(mmdsrt, mmdsrt+mmhead->nterms, term, mmDictSortComp(this));
  if (idp == mmdsrt+mmhead->nterms) return NoId;
  if (id2str(*idp) != term) return NoId;
  return *idp;
};


/*======================================================================
 * MAIN
 */

int main(int argc, const char **argv)
{
  std::vector<std::string> args(argv, argv + argc);
  prog = argv[0];

  //-- mmap dict
  string infile(args.size() > 1 ? args[1] : "-");
  mmDict dict;
  dict.open(infile, O_RDONLY);
  
  //-- map words
  for (size_t ai=2; ai < args.size(); ++ai) {
    const string& w = args[ai];
    IdT id = dict.str2id(w);
    cout << id << '\t' << w << '\n';
  }
 
  //-- done
  dict.close();
  return 0;
}
