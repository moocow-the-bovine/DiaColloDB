#include "../utils.h"

//-- type names (now in utils.h)
#if 0
template<typename T> const char* typestr() { return typeid(T).name(); };
template<> const char* typestr<uint8_t>() { return "uint8_t"; };
template<> const char* typestr<uint16_t>() { return "uint16_t"; };
template<> const char* typestr<uint32_t>() { return "uint32_t"; };
template<> const char* typestr<uint64_t>() { return "uint64_t"; };
template<> const char* typestr<float>() { return "float"; };
template<> const char* typestr<double>() { return "double"; };
#endif

//-- type-specific format strings (now in utils.h)
#if 0
template<typename T>
const char* scanItemFormat()
{ throw runtime_error(Format("scanItemFormat(): unsupported type `%s'", typestr<T>())); };

//template<> const char* scanItemFormat<uint8_t>() { return SCNu8; };
template<> const char* scanItemFormat<uint16_t>() { return SCNu16; };
template<> const char* scanItemFormat<uint32_t>() { return SCNu32; };
template<> const char* scanItemFormat<uint64_t>() { return SCNu64; };
template<> const char* scanItemFormat<float>() { return "f"; };
template<> const char* scanItemFormat<double>() { return "g"; };
#endif

//-- test func
template<typename IntT,typename FloatT>
struct Foo {
    typedef IntT MyIntT;
    typedef FloatT MyFloatT;

    Foo() {};
    ~Foo() {};

    static std::string className()
    { return string("Foo<IntT=") + typestr<IntT>() + string(",FloatT=") + typestr<FloatT>() + ">"; };

    char scanFormatBuf[32];
    const char* scanFormat()
    {
        //throw runtime_error(Format("scanFormat() not defined for Foo<IntT=%s,FloatT=%s>", typeid(IntT).name(), typeid(FloatT).name()));
        //throw runtime_error(Format("scanFormat() not defined for Foo<IntT=%s,FloatT=%s>", typeid(MyIntT).name(), typeid(MyFloatT).name()));
        //throw runtime_error(string("scanFormat() not defined for ") + className());
        //return NULL;

        sprintf(scanFormatBuf, "%%%s %%%s",
                scanItemFormat<IntT>(), scanItemFormat<FloatT>());
        return scanFormatBuf;
    };

    void main()
    { printf("%s : scanFormat=\"%s\"\n", className().c_str(), scanFormat()); };
};

typedef Foo<uint16_t,float> Foo16;
typedef Foo<uint32_t,float> Foo32;
typedef Foo<uint64_t,double> Foo64;

/*-- works --*/
#if 0
template<>
const char* Foo<uint32_t,float>::scanFormat()
{ return "%" SCNu32 " %f"; };

template<>
const char* Foo<uint64_t,double>::scanFormat()
{ return "%" SCNu64 " %g"; };

#elsif 0

/*-- template specialization using typedef? --> also works */
template<>
const char* Foo32::scanFormat()
{ return "%" SCNu32 " %f"; };

template<>
const char* Foo64::scanFormat()
{ return "%" SCNu64 " %g"; };

#else
/*-- template specialization using typedef, format in base class */

#endif

int main() {
    Foo16 f16;
    Foo32 f32;
    Foo64 f64;

    printf("typeid(uint32_t)=%s\n", typeid(uint32_t).name());
    printf("typestr(uint32_t)=%s\n", typestr<uint32_t>());

    //f16.main();
    f32.main();
    f64.main();

    return 0;
}
