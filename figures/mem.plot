set output "mem.pdf"
set terminal pdf
set termoption dashed

set key left top

set ylabel 'MiB'
set y2label 'MiB (memory)'
set xlabel '#commits'
set y2tics
set xrange [0:4110]
set palette defined (0 "#d33682", 1 "#268bd2", 2 "#859900")

set style line 1 lt 1 lc rgb "#d33682" lw 2
set style line 2 lt 1 lc rgb "#268bd2" lw 2
set style line 3 lt 1 lc rgb "#859900" lw 2
set style line 4 lt 1 lc rgb "#cb4b16" lw 2
set style line 5 lt 1 lc rgb "#d33682" lw 2
set style line 6 lt 4 lc rgb "#268bd2" lw 2
set style line 7 lt 2 lc rgb "#859900" lw 2
set style line 8 lt 2 lc rgb "#cb4b16" lw 2

plot 'quit-1.dat' using 4:($2/1024) with lines title "Quit repo size" ls 1, \
     'quit-gc-1.dat' using 4:($2/1024) with lines title "Quit with gc repo size" ls 2, \
     'quit-1.dat' using 4:($3/1024) with lines title "Quit memory" axes x1y2 ls 3, \
     'quit-gc-1.dat' using 4:($3/1024) with lines title "Quit with gc memory" axes x1y2 ls 4
