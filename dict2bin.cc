/*-*- Mode: C++ -*-*/

#include "tdmModel.h"
#include "tdmIO.h"
using namespace std;

/*======================================================================
 * Globals & typedefs
 */
const char *prog = "dict2bin";

struct StringPtrIndexComp {
  const vector<const string*> *basep;

  StringPtrIndexComp(const vector<const string*>& base)
    : basep(&base)
  {};
  
  inline bool operator()(const size_t ai, const size_t bi) const
  {
    const string* ap = (*basep)[ai];
    const string* bp = (*basep)[bi];
#if 0
    if      (ai==NULL && bi==NULL) return false;
    else if (ai==NULL)             return true;
    else if (bi==NULL)             return false;
#endif
    return (*ap) < (*bp);
  };
};


/*======================================================================
 * MAIN
 */
typedef IdDefaultT IdT;
typedef Dict<IdT> DictT;

int main(int argc, const char **argv)
{
  std::vector<std::string> args(argv, argv + argc);
  prog = argv[0];

  //-- load dict
  string infile(args.size() > 1 ? args[1] : "-");
  DictT dict;
  dict.loadTextFile(infile,true);

  //-- dump to binary
  fprintf(stderr, "TRACE: dump binary dict\n");
  string outfile(args.size() > 2 ? args[2] : "-");
  ostream *osp = ostream_open(outfile);

  //-- dump: header
  TdmBinHeaderT head(sizeof(IdT),0,dict.size(),0,0);
  head.save(*osp);

  //-- dump: offsets
  OffT off=0;
  for (const auto &dvi : dict.dVec) {
    osp->write((const char*)&off, sizeof(OffT));
    off += dvi->size();
  }
  osp->write((const char*)&off, sizeof(OffT));

  //-- dump: string-sorted IDs
  vector<IdT> dsvec(dict.dVec.size(), 0);
  for (IdT i=0; i < dsvec.size(); ++i)
    dsvec[i] = i;
  TDM_PARALLEL_NS::sort(dsvec.begin(), dsvec.end(), StringPtrIndexComp(dict.dVec));
  for (const auto &dsi : dsvec) {
    osp->write((const char*)&dsi, sizeof(IdT));
  }

  //-- dump: string data
  for (const auto &dvi : dict.dVec) {
    osp->write(dvi->data(), dvi->size());
  }

  //-- done
  ostream_close(osp);
  
  return 0;
}
