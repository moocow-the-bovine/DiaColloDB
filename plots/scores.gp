eps = 0.5;
f1  = 100.0;
N   = 100000.0;
log2(x) = log(x)/log(2.0);

f(f2,f12)  = f12;
fm(f2,f12) = 1000000 * f12/N;
lf(f2,f12) = log2(f12);
lfm(f2,f12) = log2(fm(f2,f12));
mi(f2,f12) = f2 < f12 ? 1/0 : log2(f12+eps) * log2( ((f12+eps)*(N+eps)) / ((f1+eps)*(f2+eps)) );
ld(f2,f12) = f2 < f12 ? 1/0 : 14 + log2( (2*(f12+eps)) / ((f1+eps)+(f2+eps)) );

set xlabel "f2";
set ylabel "f12";
set zlabel "score";
set xrange [100:10000];
set yrange [1:100];
#set logscale x 10;
#set logscale y 10;
#set zrange [0:100];

set xtic rotate by -45;

load "common.gp";
