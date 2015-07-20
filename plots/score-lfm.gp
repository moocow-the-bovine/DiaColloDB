load "scores.gp";
set title "lfm(f2,f12) [N=100k,f1=100]";
set cbrange [0:lfm(10000,100)];
splot lfm(x,y) notitle;
