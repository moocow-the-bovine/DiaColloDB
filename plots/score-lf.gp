load "scores.gp";
set title "lf(f2,f12) [N=100k,f1=100]";
set cbrange [lf(0,1):lf(0,100)];
splot lf(x,y) notitle;
