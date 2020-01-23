//-*- Mode: C++ -*-

#ifndef TOK2COF_H
#define TOK2COF_H

#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <errno.h>

#include <string>
#include <map>
#include <vector>
#include <algorithm>

using namespace std;

//======================================================================
// globals
const char *prog = "PROGRAM";
const char *ifile = "-";
const char *ofile = "-";
FILE *ifp = NULL;
FILE *ofp = NULL;

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


//======================================================================
// mmap (ganked from ddcMMap.h)

#include <sys/stat.h> /* fstat */
#include <sys/mman.h> /* mmap */
#include <fcntl.h> /* O_RDONLY */
#include <unistd.h> /* open, close */


/** convenience wrapper for mapping entire files into virtual memory using mmap() */
template<typename T=char>
class t2cMMap {
public:
    //----------------------------------------------------------
    // data
    std::string m_file;    ///< underlying filename
    size_t m_nbytes;  ///< number of bytes mapped
    T *m_data;    ///< file data

public:
    //----------------------------------------------------------
    // constructors etc.

    /// default constructor
    t2cMMap()
        : m_nbytes(0), m_data(NULL)
    {};

    t2cMMap(const std::string &filename, int mode = O_RDONLY)
        : m_nbytes(0), m_data(NULL)
    { open(filename, mode); };

    /// default destructor calls close()
    ~t2cMMap() { close(); };

public:
    //----------------------------------------------------------
    // accessors

    /// returns the name of the currently mapped file, if any
    inline const std::string &filename() const { return m_file; };

    /// returns number of bytes mapped, or 0
    inline size_t nbytes() const { return m_nbytes; };

    /// returns a pointer to the mmap()ed file data, or NULL
    inline T *data() const { return m_data; };

public:
    //----------------------------------------------------------
    // high-level properties

    /// returns the number of objects of type T mapped, or 0
    inline size_t size() const { return m_nbytes / sizeof(T); };

    /// STL-esque wrapper for (nbytes()==0)
    inline bool empty() const { return m_nbytes == 0; };

    /// returns true iff m_data is non-NULL
    inline bool opened() const { return m_data != NULL; };

public:
    //----------------------------------------------------------
    // typecast operators

    /// implicit typecast to bool
    inline operator bool() const { return opened(); };

    /// implicit typecast to T*
    inline operator T *() const { return m_data; };

    /// indexing operator
    inline T &operator[](size_t idx) const { return m_data[idx]; };

public:
    //----------------------------------------------------------
    // guts

    /// map a named file \a filename
    t2cMMap &open(const std::string &filename, int mode = O_RDONLY) {
        if (opened()) close();
        //ddcLogTrace("t2cMMap::open("+filename+")");
        m_file = filename;

        //-- open the file (temporarily)
        int mmfd = ::open(filename.c_str(), mode);
        if (mmfd == -1)
            throw std::invalid_argument(
                Format("t2cMMap::open(): open failed for '%s': %s", filename.c_str(), strerror(errno)));

        //-- get total file size
        struct stat statbuf;
        if (fstat(mmfd, &statbuf) != 0)
            throw std::runtime_error(
                Format("t2cMMap::open(): fstat() failed for file '%s': %s", filename.c_str(), strerror(errno)));
        m_nbytes = statbuf.st_size;

        if (m_nbytes > 0) {
            //-- get mmap protection flags
            int mmprot = 0;
            switch (mode) {
                case O_RDONLY:
                    mmprot = PROT_READ;
                    break;
                case O_WRONLY:
                    mmprot = PROT_WRITE;
                    break;
                case O_RDWR:
                    mmprot = (PROT_READ | PROT_WRITE);
                    break;
                default:
                    throw std::invalid_argument(Format("t2cMMap::open(): unknown open mode '%d'", mode));
            }

            //-- underlying mmap call
            m_data = (T *) mmap(NULL, m_nbytes, mmprot, MAP_SHARED, mmfd, 0);
            if (m_data == MAP_FAILED)
                throw std::runtime_error(
                    Format("t2cMMap::open(): mmap() failed for file '%s': %s", filename.c_str(), strerror(errno)));
        }

        //-- we can now safely close the fd
        if (::close(mmfd) != 0)
            throw std::runtime_error(
                Format("t2cMMap::open(): close() failed for file '%s': %s", filename.c_str(), strerror(errno)));

        return *this;
    };

    /// unmap current file (if any)
    void close() {
        if (m_data != NULL) {
            //ddcLogTrace("t2cMMap::close("+m_file+")");
            if (munmap(m_data, m_nbytes) != 0)
                throw std::runtime_error(
                    Format("t2cMMap::close(): munmap() failed for file '%s': %s", m_file.c_str(), strerror(errno)));
        }
        m_file.clear();
        m_data = NULL;
        m_nbytes = 0;
    };

public:
    //----------------------------------------------------------
    // STL-style iterators

    typedef T *iterator;
    typedef const T *const_iterator;

    inline iterator begin() const { return m_data; };

    inline iterator end() const { return m_data + size(); };
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


//-- val = bin2native(val_bigEndian)
#if T2C_BIG_ENDIAN
# define _bin2native(sz)
#else
# define _bin2native(sz) _bswap ## sz
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
void t2c_init(int argc, const char **argv)
{
    prog = *argv;
    if (argc > 1) ifile = argv[1];
    if (argc > 2) ofile = argv[2];

    if (strcmp(ifile,"-")==0) {
        ifp = stdin;
    } else if ( !(ifp = fopen(ifile,"r")) ) {
        fprintf(stderr, "%s: open failed for input file '%s': %s", prog, ifile, strerror(errno));
        exit(1);
    }

    if (strcmp(ofile,"-")==0) {
        ofp = stdout;
    } else if ( !(ofp = fopen(ofile,"w")) ) {
        fprintf(stderr, "%s: open failed for output file '%s': %s", prog, ofile, strerror(errno));
        exit(2);
    }

    fprintf(stderr, "%s: %s -> %s\n", prog, ifile, ofile);
    t_started.Start();
}

//--------------------------------------------------------------
void t2c_finish()
{
    //-- timing
    showMemoryStats(Format("%s: memory usage: ", prog));
    fprintf(stderr, "%s: operation completed in %g second(s)\n", prog, TimerT().Elapsed(t_started));
  
    //-- cleanup
    if (ifp && ifp != stdin) fclose(ifp);
    if (ofp && ofp != stdout) fclose(ofp);  
}


#endif /* TOK2COF_H */


