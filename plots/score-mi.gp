load "scores.gp";
set title "mi(f2,f12) [N=100k,f1=100]";
set cbrange [mi(10000,1):mi(100,100)];
splot mi(x,y) notitle;
