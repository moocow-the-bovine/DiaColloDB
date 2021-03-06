#include "cof2bin.h"

//======================================================================
// debug
#if C2B_DEBUG
# define CBDEBUG(x) x
#else
# define CBDEBUG(x)
#endif

//======================================================================
// Types
typedef uint32_t TermIdT;
typedef uint16_t DateT;
typedef uint32_t FreqT;
typedef uint32_t PosT;

#define SCNTerm SCNu32
#define SCNDate SCNu16
#define SCNFreq SCNu32

#define cofPRITerm PRIu32
#define cofPRIDate PRIu16
#define cofPRIFreq PRIu32

//======================================================================
// local types & constants

typedef map<TermIdT,FreqT> TermFreqMapT;
typedef map<DateT,FreqT>   DateFreqMapT;

const DateT   NODATE = (DateT)-1;
const TermIdT NOTERM = (TermIdT)-1;

//======================================================================
// cofCompiler struct (stateful)
/*
## $cof = $cof->loadTextFh($fh,%opts)
##  + loads from text file as saved by saveTextFh():
##      N                       ##-- 1 field : N
##      FREQ ID1 DATE           ##-- 3 fields: un-collocated portion of $f1
##      FREQ ID1 DATE ID2       ##-- 4 fields: co-frequency pair (ID2 >= 0)
##      FREQ ID1 DATE ID2 DATE2 ##-- 5 fields: redundant date (used by create(); DATE2 is ignored)
##  + supports semi-sorted input: input fh must be sorted by $i1,$d1
##    and all $i2 for each $i1,$d1 must be adjacent (i.e. no intervening ($j1,$e1) with $j1 != $i1 or $e1 != $d1)
##  + supports multiple lines for pairs ($i1,$d1,$i2) provided the above conditions hold
##  + supports loading of $cof->{N} from single-value lines
##  + %opts: clobber %$cof
*/
struct cofCompiler {
    string infile;
    string outbase;
    
    FILE *fin;
    FILE *fr1; // $r1 : [$end2]            @ $i1				: constant (logical index)
    FILE *fr2; // $r2 : [$end3,$d1,$f1]*   @ end2($i1-1)..(end2($i1+1)-1)       : sorted by $d1 for each $i1
    FILE *fr3; // $r3 : [$i2,$f12]*        @ end3($d1-1)..(end3($d1+1)-1)       : sorted by $i2 for each ($i1,$d1)
    FILE *frN; // $rN : [$fN]              @ $date - $ymin                      : totals by date
    FreqT fmin;

    //-- iteration variables
    PosT pos1; //-- offset in fr1
    PosT pos2; //-- offset in fr2
    PosT pos3; //-- offset in fr3

    TermIdT i1_cur;
    DateT   d1_cur;
    FreqT   f1;

    FreqT   f12;
    TermIdT i1;
    DateT   d1;
    TermIdT i2;
    //DateT d2;
    FreqT   f;

    FreqT   N;
    FreqT   N1;

    TermFreqMapT f12map;
    DateFreqMapT fNmap;

    //----------------------------------------------------------
    cofCompiler(FreqT fmin_=0)
        : fin(NULL), fr1(NULL), fr2(NULL), fr3(NULL), frN(NULL)
    {
        clear();
        fmin = fmin_;
    };

    cofCompiler(const string& infilename, const string& outbasename, FreqT fmin_=0)
        : fin(NULL), fr1(NULL), fr2(NULL), fr3(NULL), frN(NULL)
    {
        clear();
        fmin = fmin_;
        open(infilename,outbasename);
    };

    ~cofCompiler()
    { close(); };

    //----------------------------------------------------------
    inline void clear()
    {
        close();
        infile.clear();
        outbase.clear();
        fmin = 0;
        pos1 = 0;
        pos2 = 0;
        pos3 = 0;
        i1_cur = NOTERM;
        d1_cur = NODATE;
        f1  = 0;
        f12 = 0;
        i1  = NOTERM;
        d1  = NODATE;
        f   = 0;
        N   = 0;
        N1  = 0;
        f12map.clear();
        fNmap.clear();
    };

    //----------------------------------------------------------
    inline FILE* openOut(const string& filename, const char* mode="wb") const
    {
        FILE *f = fopen(filename.c_str(), mode);
        if (f == NULL)
            throw runtime_error(Format("open failed for output file '%s': %s", filename.c_str(), strerror(errno)));
        return f;
    };

    //----------------------------------------------------------
    void open(const string& infilename, const string& outbasename)
    {
        infile  = infilename;
        outbase = outbasename;

        close();
        if (infile.empty() || infile=="-") {
            infile = "-";
            fin    = stdin;
        } else if ( !(fin = fopen(infile.c_str(),"r")) ) {
            throw std::runtime_error(Format("open failed for input file '%s': %s", infile.c_str(), strerror(errno)));
        }

        if (outbase.empty())
            throw std::runtime_error("no output basename specified");
        fr1 = openOut(outbase + ".dba1");
        fr2 = openOut(outbase + ".dba2");
        fr3 = openOut(outbase + ".dba3");
        frN = openOut(outbase + ".dbaN");
    };

    //----------------------------------------------------------
    void close()
    {
        if (fin && fin != stdin) { fclose(fin); fin=NULL; }
        if (fr1) { fclose(fr1); fr1=NULL; }
        if (fr2) { fclose(fr2); fr2=NULL; }
        if (fr3) { fclose(fr3); fr3=NULL; }
        if (frN) { fclose(frN); frN=NULL; }
    };

    //----------------------------------------------------------
    template <typename T>
    inline void writeBin(T val, FILE* fp)
    {
        val = binval(val);
        fwrite(&val, sizeof(T),1, fp);
    };

    //----------------------------------------------------------
    // guts for inserting records from $i1_cur,$d1_cur,%f12,$pos1,$pos2 : call on changed ($i1_cur,$d1_cur)
    void insert()
    {
        if (i1_cur != NOTERM) {
            if (i1_cur != pos1) {
                //-- we've skipped one or more $i1 because it had no collocates (e.g. kern01 i1=287123="Untier/1906")
                for (size_t i=0; i < (i1_cur-pos1); ++i) {
                    writeBin<PosT>(pos2, fr1);
                }
                pos1 = i1_cur;
            }

            //-- dump r3-records for (i1_cur,d1_cur,*)
            f1 = 0;
            for (TermFreqMapT::const_iterator f12i=f12map.begin(); f12i!=f12map.end(); ++f12i) {
                f   = f12i->second;
                f1 += f;
                if (f < fmin || f12i->first < 0) continue; //-- skip here so we can track "real" marginal frequencies
                writeBin<TermIdT>(f12i->first, fr3);
                writeBin<FreqT>(f, fr3);
                ++pos3;
            }

            //-- dump r2-record for ($i1_cur,$d1_cur), and track $fN by date
            if (d1_cur != NODATE) {
                writeBin<PosT>(pos3,   fr2);
                writeBin<DateT>(d1_cur, fr2);
                writeBin<FreqT>(f1,     fr2);
                fNmap[d1_cur] += f1;
                ++pos2;
            }

            //-- maybe dump r1-record for $i1_cur
            if (i1 != i1_cur) {
                writeBin<PosT>(pos2, fr1);
                pos1 = i1_cur+1;
            }
            N += f1;
        }
        i1_cur = i1;
        d1_cur = d1;
        f12map.clear();
    };

    //----------------------------------------------------------
    void compile()
    {
        char *linebuf  = NULL;
        size_t linelen = 0;
        size_t lineno  = 0;
        ssize_t nread;
        int nscanned;

        i1_cur = NOTERM;
        d1_cur = NODATE;
        f1     = 0;
        while ( true ) {
            ++lineno;
            nread = getline(&linebuf,&linelen,fin);
            if (nread <= 0) {
                if (!feof(fin))
                    throw runtime_error(Format("error reading from %s: %s\n", infile.c_str(), strerror(errno)));
                break;
            }

            d1 = NODATE;
            i2 = NOTERM;
            nscanned = sscanf(linebuf,
                              "%" SCNFreq " %" SCNTerm " %" SCNDate " %" SCNTerm
                              ,   &f12,        &i1,         &d1,         &i2);
            if (nscanned == EOF) {
                //-- 0 columns: blank line: skip it
                continue;
            } else if (nscanned == 1) {
                //-- 1 column: N
                N += f12;
            } else if (nscanned == 2) {
                //-- 2 columns: not supported
                throw runtime_error(Format("failed to parse %s input line %zd\n", infile.c_str(), lineno));
            }

            if (i1 != i1_cur || d1 != d1_cur)
                insert(); //-- insert record(s) for ($i1_cur,$d1_cur)

            f12map[i2] += f12; //-- buffer co-frequencies for ($i1_cur,$d1_cur); track un-collocated frequencies as $i2=-1
        }
        i1 = NOTERM;
        d1 = NODATE;
        insert(); //-- write record(s) for final ($i1_cur,$d1_cur)

        //-- create $rN by date (include gaps)
        DateT ymin = fNmap.empty() ? 0 : fNmap.begin()->first;
        DateT ymax = fNmap.empty() ? 0 : fNmap.rbegin()->first;
        for (DateT y=ymin; y <= ymax; ++y) {
            DateFreqMapT::const_iterator yi = fNmap.find(y);
            FreqT yf = yi==fNmap.end() ? 0 : yi->second;
            writeBin<FreqT>(yf, frN);
        }

        //-- write constants
        N = max(N1,N);
        FILE* fconst = openOut(outbase + ".const");
        fprintf(fconst, "%zu %zu\n", (size_t)ymin, (size_t)N);
        fclose(fconst);
    };
};

//======================================================================
int main(int argc, const char **argv)
{
    try {
        c2b_init(argc, argv);

        cofCompiler compiler(infile,outbase,fmin);
        compiler.compile();
        compiler.close();
        
        c2b_finish();
        return 0;
    }
    catch (exception &e) {
        fprintf(stderr, "%s: EXCEPTION: %s\n", prog, e.what());
        return -1;
    }
}
