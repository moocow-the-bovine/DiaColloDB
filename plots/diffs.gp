diff(a,b) = a-b;
adiff(a,b) = abs(a-b);
min(a,b) = a < b ? a : b;
max(a,b) = a > b ? a : b;
sum(a,b) = a+b;
avg(a,b) = (a+b)/2.0;
havg0(a,b) = a<=0 || b<=0 ? 0 : (2.0*a*b)/(a+b);
havg(a,b) = avg(havg0(a,b),avg(a,b));
nthRoot(x,n) = sgn(x) * abs(x)**(1.0/n);
gavg0(a,b) = nthRoot(a*b,2);
gavg(a,b) = avg(gavg0(a,b),avg(a,b));
lavgd(a,b) = min(a,b) < 1 ? (1-min(a,b)) : 0;
lavg(a,b) = exp( log((a+lavgd(a,b))*(b+lavgd(a,b)))/2.0 ) - lavgd(a,b);

set xlabel "a";
set ylabel "b";
set zlabel "diff";
set xrange [0:10];
set yrange [0:10];
set zrange [0:10];

load "common.gp";
