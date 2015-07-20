load "scores.gp";
set title "lf(f2,f12) [N=100k,f1=100]";
set cbrange [0:lf(10000,100)];
splot lf(x,y) notitle;
