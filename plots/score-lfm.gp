load "scores.gp";
set title "lfm(f2,f12) [N=100k,f1=100]";
set cbrange [lfm(0,1):lfm(0,100)];
splot lfm(x,y) notitle;
