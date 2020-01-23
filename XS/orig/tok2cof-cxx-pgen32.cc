#include "tok2cof.h"
#include <omp.h> //-- OpenMP

//======================================================================
// debug
//#define T2C_MR_DEBUG 1
#if T2C_MR_DEBUG
# define MRDEBUG(x) x
#else
# define MRDEBUG(x)
#endif

//======================================================================
// Types
typedef uint32_t TermIdT;
typedef uint16_t DateT;
typedef uint32_t FreqT;

//======================================================================
// constants
const TermIdT EOS = (TermIdT)-1;

//======================================================================
// WordT struct
struct __attribute__((__packed__)) WordT {
    TermIdT tid;
    DateT   date;

    WordT()
        : tid(0), date(0)
    {};

    WordT(TermIdT tid_, DateT date_)
        : tid(tid_), date(date_)
    {};

    WordT(const WordT& w)
        : tid(w.tid), date(w.date)
    {};

    inline void decode()
    {
        tid  = binval(tid);
        date = binval(date);
    };

    inline bool eos() const
    { return tid == EOS; };

    inline bool operator<(const WordT& w) const
    { return tid < w.tid || (tid==w.tid && date < w.date); };
    
    inline bool operator<=(const WordT& w) const
    { return tid < w.tid || (tid==w.tid && date <= w.date); };
    
    inline bool operator==(const WordT& w) const
    { return tid==w.tid && date==w.date; };
};


//======================================================================
// common typedefs

//typedef pair<TermIdT,DateT> WordT;
typedef pair<WordT,WordT> WordPairT;
typedef vector<WordT> SentenceT;
typedef map<WordPairT, FreqT> MapT;


//======================================================================
// buffering

//-- T2C_GEN_BUFSIZE : number of co-occurrence records to buffer in memory before writing
#ifdef T2C_GEN_BUFSIZE
const size_t MaxBufSize = T2C_GEN_BUFSIZE;
#else
//const size_t MaxBufSize = 0; //-- no buffering
//const size_t MaxBufSize = (1<<10); //-- 1K
const size_t MaxBufSize = (1<<12); //-- 4K
//const size_t MaxBufSize = (1<<13); //-- 8K
//const size_t MaxBufSize = (1<<16); //-- 64K
//const size_t MaxBufSize = (1<<17); //-- 128K
//const size_t MaxBufSize = (1<<18); //-- 256K
//const size_t MaxBufSize = (1<<20); //-- 1M
//const size_t MaxBufSize = (1<<21); //-- 2M
#endif

typedef vector<WordPairT> WordPairBufferT;


//======================================================================
// utils

omp_lock_t output_lock;

struct WorkerT {
    int  thrid;
    FILE *fin;
    FILE *fout;

    char *linebuf;
    char *datebuf;

    size_t linelen;
    size_t lineno;
    ssize_t nread;

    WordT     w;
    SentenceT sent;
    WordPairBufferT pbuf;

    WorkerT(int thrid_, FILE *fin_, FILE *fout_)
        : thrid(thrid_), fin(fin_), fout(fout_),
          linebuf(NULL), datebuf(NULL),
          linelen(0),lineno(0),nread(0)
    {};

    ~WorkerT()
    {
        if (fin && fin != stdin) { fclose(fin); fin=NULL; }
        //if (fout && fout != stdout) { fclose(fout); fout=NULL; }
        if (linebuf) { free(linebuf); linebuf=NULL; }
    };

    inline bool GetToken()
    {
        nread = getline(&linebuf,&linelen,fin);
        if (nread <= 0) {
            if (!feof(fin))
                throw runtime_error(Format("error reading from %s: %s\n", ifile, strerror(errno)));
            return false;
        }
        if (linebuf[0] == '\n') {
            //-- blank line: EOS
            w.tid = EOS;
        }
        else {
            //-- normal token
            w.tid  = strtoul(linebuf,  &datebuf,  0);
            w.date = strtoul(datebuf,  NULL,      0);
        }
        return true;
    };

    void flushBuffer()
    {
        omp_set_lock(&output_lock);
        for (WordPairBufferT::const_iterator bi=pbuf.begin(); bi != pbuf.end(); ++bi) {
            fprintf(ofp, "%zu\t%zu\t%zu\t%zu\n",
                    (size_t)bi->first.tid,
                    (size_t)bi->first.date,
                    (size_t)bi->second.tid,
                    (size_t)bi->second.date);
        }
        omp_unset_lock(&output_lock);
        pbuf.clear();
    };
    
    void addSentence(size_t dmax=5)
    {
        if (sent.size() < 2) return;
        SentenceT::const_iterator si,sj;
        
        for (si=sent.begin(); si != sent.end(); ++si) {
            for (sj=max(si-dmax, sent.cbegin()); sj < si; ++sj) {
                pbuf.push_back( std::make_pair(*si,*sj) );
            }
            for (sj=si+1; sj != min(si+dmax+1, sent.cend()); ++sj) {
                pbuf.push_back( std::make_pair(*si,*sj) );
            }
        }

        if (pbuf.size() >= MaxBufSize)
            flushBuffer();
    };

};


//======================================================================
// worker-thread callbacks

void cbWorker(size_t inbytes)
{
    //-- worker code goes here
    int thrid = omp_get_thread_num();
    int nthr  = omp_get_num_threads();

    //-- worker locals: open
    WorkerT worker(thrid, fopen(ifile, "r"), ofp);
    if (!worker.fin)
        throw runtime_error(Format("thread #%d open failed for %s: %s", thrid, ifile, strerror(errno)));

    long off_lo = thrid * (inbytes / nthr);
    long off_hi = (thrid == nthr-1) ? inbytes : (thrid+1) * (inbytes/nthr);
    MRDEBUG(fprintf(stderr, "%s[%d/%d] : range=%4zd - %4zd [FILE=%p]\n", prog, thrid,nthr, off_lo,off_hi, worker.fin));

    //-- worker guts: initialize; scan for next EOS
    fseek(worker.fin, off_lo, SEEK_SET);
    if (off_lo != 0) {
        for ( ; !worker.w.eos(); worker.GetToken() ) ;
    }

    //-- worker guts: main loop
    while ( worker.GetToken() ) {
        if (worker.w.tid==EOS) {
            worker.addSentence();
            worker.sent.clear();
            //MRDEBUG(fprintf(stderr, "%s[%d/%d] : EOS at offset=%ld\n", prog, thrid,nthr, ftell(worker.fin)));
            if (ftell(worker.fin) >= off_hi) break;
        } else {
            worker.sent.push_back(worker.w);
        }
    }
    worker.addSentence();
    worker.sent.clear();
    worker.flushBuffer();
    MRDEBUG(fprintf(stderr, "%s[%d/%d] : worker done at off=%ld\n", prog, thrid,nthr, ftell(worker.fin)));

    //-- worker cleanup (in destructor)
}


//======================================================================
int main(int argc, const char **argv)
{
    try {
        t2c_init(argc, argv);
  
        //-- get total input file size
        struct stat statbuf;
        if (fstat(fileno(ifp), &statbuf) != 0)
            throw std::runtime_error(Format("%s: fstat() failed for file '%s': %s", prog, ifile, strerror(errno)));
        size_t inbytes = statbuf.st_size;
        MRDEBUG(fprintf(stderr, "%s: nbytes=%zd\n", prog, inbytes));

        //-- OpenMP: parallelize
        omp_init_lock(&output_lock);
        #pragma omp parallel
        {
            cbWorker(inbytes);               
        }
        omp_destroy_lock(&output_lock);
 

        t2c_finish();
        return 0;
    }
    catch (exception &e) {
        fprintf(stderr, "%s: EXCEPTION %s\n", prog, e.what());
        return -1;
    }
}
