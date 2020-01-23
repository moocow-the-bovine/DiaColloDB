//-*- Mode: C++ -*-

#ifndef COF2BIN_H
#define COF2BIN_H

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h> // PRIu32 etc.
#include <errno.h>

#include <unistd.h>
#include <sys/types.h>

#include <string>
#include <map>
#include <vector>
#include <algorithm>

using namespace std;

//======================================================================
// globals
const char *prog    = "PROGRAM";
const char *infile  = NULL; //"-";
const char *outbase = NULL; //"cof.d/cof";

size_t fmin = 2;

//======================================================================
// timing

struct TimerT {
  struct timespec m_ts;

  TimerT()
  { Start(); }

  inline void Start()
  { clock_gettime(CLOCK_REALTIME, &m_ts); };

  double Elapsed(const TimerT& t0) const
  {
    /* Perform the carry for the later subtraction by updating Y. */
    time_t  t0_tv_sec   = t0.m_ts.tv_sec;
    long    t0_tv_nsec  = t0.m_ts.tv_nsec;
    if (m_ts.tv_nsec < t0_tv_nsec) {
        long nsec = (t0_tv_nsec - m_ts.tv_nsec) / 1000000000 + 1;
        t0_tv_nsec -= 1000000000 * nsec;
        t0_tv_sec += nsec;
    }
    if (m_ts.tv_nsec - t0_tv_nsec > 1000000000) {
        long nsec = (m_ts.tv_nsec - t0_tv_nsec) / 1000000000;
        t0_tv_nsec += 1000000000 * nsec;
        t0_tv_sec -= nsec;
    }

    /* Compute the time remaining to wait. 'tv_nsec' is certainly positive. */
    double elapsed = (m_ts.tv_sec - t0_tv_sec) + ((double)(m_ts.tv_nsec - t0_tv_nsec) / (double)1000000000);
    if (m_ts.tv_sec < t0_tv_sec)
        elapsed *= -1;

    return elapsed;
  };
};

TimerT t_started;

//======================================================================
// Format (printf substitute)

#include <stdarg.h>

string Format(const char *fmt, ...)
{
  char *buf = NULL;
  size_t len=0;
  va_list ap;
  va_start(ap,fmt);
  len = vasprintf(&buf,fmt,ap);
  va_end(ap);
  string s(buf,len);
  if (buf) free(buf);
  return s;
};



/*======================================================================
 * generic utilities: byte-order and swapping
 *  see https://stackoverflow.com/questions/4239993/determining-endianness-at-compile-time/4240029
 *  and https://codereview.stackexchange.com/questions/64797/byte-swapping-functions
 */

#if defined(__BYTE_ORDER) && __BYTE_ORDER == __BIG_ENDIAN || \
    defined(__BIG_ENDIAN__) || \
    defined(__ARMEB__) || \
    defined(__THUMBEB__) || \
    defined(__AARCH64EB__) || \
    defined(_MIBSEB) || defined(__MIBSEB) || defined(__MIBSEB__)
// It's a big-endian target architecture
# define T2C_BIG_ENDIAN 1
//# warning "native byte-order: big-endian"
#elif defined(__BYTE_ORDER) && __BYTE_ORDER == __LITTLE_ENDIAN || \
    defined(__LITTLE_ENDIAN__) || \
    defined(__ARMEL__) || \
    defined(__THUMBEL__) || \
    defined(__AARCH64EL__) || \
    defined(_MIPSEL) || defined(__MIPSEL) || defined(__MIPSEL__)
// It's a little-endian target architecture
# define T2C_LITTLE_ENDIAN 1
//# warning "native byte-order: little-endian"
#else
# error "Can't determine architecture byte order!"
#endif

inline uint16_t _bswap16(uint16_t a)
{
  a = ((a & 0x00FF) << 8) | ((a & 0xFF00) >> 8);
  return a;
}
inline uint32_t _bswap32(uint32_t a)
{
  a = ((a & 0x000000FF) << 24) |
      ((a & 0x0000FF00) <<  8) |
      ((a & 0x00FF0000) >>  8) |
      ((a & 0xFF000000) >> 24);
  return a;
}
inline uint64_t _bswap64(uint64_t a)
{
  a = ((a & 0x00000000000000FFULL) << 56) | 
      ((a & 0x000000000000FF00ULL) << 40) | 
      ((a & 0x0000000000FF0000ULL) << 24) | 
      ((a & 0x00000000FF000000ULL) <<  8) | 
      ((a & 0x000000FF00000000ULL) >>  8) | 
      ((a & 0x0000FF0000000000ULL) >> 24) | 
      ((a & 0x00FF000000000000ULL) >> 40) | 
      ((a & 0xFF00000000000000ULL) >> 56);
  return a;
}

union swapfU { float f; uint32_t i; };
inline float _bswapf(float f)
{
  swapfU u;
  u.f = f;
  u.i = _bswap32(u.i);
  return u.f;
};

union swapgU { double g; uint64_t i; };
inline double _bswapg(double g) {
  swapgU u;
  u.g = g;
  u.i = _bswap64(u.i);
  return u.g;
};


//-- val           = bin2native(val_bigEndian)
//-- val_bigEndian = native2bin(val)
#if T2C_BIG_ENDIAN
# define _bin2native(sz)
# define _native2bin(sz) _bswap ## sz
#else
# define _bin2native(sz) _bswap ## sz
# define _native2bin(sz) _bswap ## sz
#endif

/*
template<typename T>
inline T binval(T val)
{ throw std::runtime_error("abstract binval() template called"); }

template<> inline uint16_t binval(uint16_t val) { return _bin2native(16)(val); };
template<> inline uint32_t binval(uint32_t val) { return _bin2native(32)(val); };
template<> inline uint64_t binval(uint64_t val) { return _bin2native(64)(val); };
template<> inline float    binval(float    val) { return _bin2native(f)(val); };
template<> inline double   binval(double   val) { return _bin2native(g)(val); };
*/

inline uint16_t binval(uint16_t val) { return _bin2native(16)(val); };
inline uint32_t binval(uint32_t val) { return _bin2native(32)(val); };
inline uint64_t binval(uint64_t val) { return _bin2native(64)(val); };
inline float    binval(float    val) { return _bin2native(f)(val); };
inline double   binval(double   val) { return _bin2native(g)(val); };

/*======================================================================
 * generic utilities: memory status
 */
#include <fstream>

void showMemoryStats(const string& prefix="Memory Usage: ")
{
  string filename = Format("/proc/%d/status", getpid());
  ifstream ifs(filename, ios_base::binary);
  string line;
  size_t vmpeak=0,vmhwm=0;
  while (ifs) {
    getline(ifs,line);
    if      (line.find("VmPeak:")==0) vmpeak = strtoul(line.c_str()+strlen("VmPeak:"), NULL, 0);
    else if (line.find("VmHWM:")==0)  vmhwm  = strtoul(line.c_str()+strlen("VmHWM:"), NULL, 0);
  }
  ifs.close();
  fprintf(stderr, "%sVmPeak=%zd kB ; VmHWM=%zd kB\n", prefix.c_str(), vmpeak, vmhwm);
}


//======================================================================
// common hacks

//--------------------------------------------------------------
void c2b_init(int argc, const char **argv)
{
    //-- parse options
    prog = *argv;
    for (int argi=1; argi < argc; ++argi) {
        string arg(argv[argi]);
        if (arg == "-h" || arg == "-help") {
            fprintf(stderr,
                    "\n"
                    "Usage: %s [OPTIONS] [INFILE.dat=- [OUTBASE=cof.d/cof]]\n"
                    "\n"
                    "Options:\n"
                    "  -h, -help       # this help message\n"
                    "  -f, -fmin FMIN  # minimum co-occurrence frequency (default=%zd)\n"
                    "\n",
                    prog, (size_t)fmin);
            exit(1);
        }
        else if (arg == "-f" || arg == "-cfmin" || arg == "-fmin") {
            fmin = strtoul(argv[argi+1], NULL, 0);
            ++argi;
        }
        else if (arg[0] != '-') {
            if (infile == NULL) {
                infile = argv[argi];
            } else if (outbase == NULL) {
                outbase = argv[argi];
            } else {
                fprintf(stderr, "%s WARNING: unhandled non-option argument '%s'", prog, argv[argi]);
            }
        }
        else {
            fprintf(stderr, "%s WARNING: unknown argument '%s'", prog, argv[argi]);
        }
    }

    
    if (!infile || !*infile)
        infile = "-";
    if (!outbase || !*outbase) 
        outbase = "cof.d/cof";

    fprintf(stderr, "%s: %s -> %s\n", prog, infile, outbase);
    t_started.Start();
}

//--------------------------------------------------------------
void c2b_finish()
{
    //-- timing
    showMemoryStats(Format("%s: memory usage: ", prog));
    fprintf(stderr, "%s: operation completed in %g second(s)\n", prog, TimerT().Elapsed(t_started));
}


#endif /* COF2BIN_H */
