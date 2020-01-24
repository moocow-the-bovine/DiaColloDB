#include "../utils.h"

//-- BEGIN test-bits
#ifndef MY_NBITS
# define MY_NBITS 32
#endif

#if MY_NBITS == 32
typedef uint32_t MyIntT;
#elif MY_NBITS == 64
typedef uint64_t MyIntT;
#else
# error unsupported value for MY_NBITS
typedef unsigned int MyIntT;
#endif

int main(int argc, const char **argv)
{
    MyIntT i = 0;
    printf("MyIntT: type=%s ; size=%zd byte(s) ; val=%d\n", typestr<MyIntT>(), sizeof(MyIntT), (int)i);
    return 0;
}
