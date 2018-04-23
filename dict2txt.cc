/*-*- Mode: C++ -*-*/

#include "tdmModel.h"
#include "tdmIO.h"
using namespace std;

/*======================================================================
 * Globals & typedefs
 */
const char *prog = "dict2txt";

/*======================================================================
 * MAIN
 */
typedef IdDefaultT IdT;
typedef Dict<IdT> DictT;

int main(int argc, const char **argv)
{
  std::vector<std::string> args(argv, argv + argc);
  prog = argv[0];

  //-- args dict
  string infile(args.size() > 1 ? args[1] : "-");
  string outfile(args.size() > 2 ? args[2] : "-");

  //-- mmap variables
  int mode = O_RDONLY;
  int            mmfd;
  off_t          mmsize;
  char 		*mmbuf;
  TdmBinHeaderT *mmhead;
  OffT          *mmdoff;
  IdT           *mmdsrt;
  char 		*mmdtrm;
  
  //-- mmap dict
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

  ostream *osp = ostream_open(outfile);
#if 0
  //-- dump (by id)
  for (IdT id=0; id < mmhead->nterms; ++id) {
    string tstr( &mmdtrm[mmdoff[id]], mmdoff[id+1]-mmdoff[id] );
    *osp << id << '\t' << tstr << '\n';
  }
#else
  //-- dump (sorted)
  for (size_t qsi=0; qsi < mmhead->nterms; ++qsi) {
    IdT id = mmdsrt[qsi];
    string tstr( &mmdtrm[mmdoff[id]], mmdoff[id+1]-mmdoff[id] );
    *osp << id << '\t' << tstr << '\n';
  }
#endif
  ostream_close(osp);
 
  //-- done
  if (munmap(mmbuf, mmsize) != 0)
    throw runtime_error("closeDict(): munmap() failed");
  ::close(mmfd);
  mmfd = -1;
  mmsize = 0;
  mmbuf = NULL;
  mmhead = NULL;
  mmdoff = NULL;
  mmdsrt = NULL;
  mmdtrm = NULL;
  
  return 0;
}
