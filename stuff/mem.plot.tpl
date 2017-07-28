set output "mem.pdf"
set terminal pdf
set termoption dashed

set key left top

set ylabel 'MiB'
set y2label 'MiB (memory)'
set xlabel '#commits'
set y2tics
set xrange [0:{{ maxCommits }}]

set key autotitle columnhead

set style line 1 lt 1 lw 1
set style line 2 lt 7 lw 1

plot {% for scenario in scenarios %}     '{{ scenario.file }}' using 4:($2/1024) with lines title "{{ scenario.title }} repo size" ls 1 lt rgb "{{ scenario.color }}",\
     '{{ scenario.file }}' using 4:($3/1024) with lines title "{{ scenario.title }} memory" axes x1y2 ls 2 lt rgb "{{ scenario.color }}",\
{% endfor %}

#plot 'quit-1.dat' using 4:($2/1024) with lines title "Quit repo size" ls 1, \
#     'quit-gc-1.dat' using 4:($2/1024) with lines title "Quit with gc repo size" ls 2, \
#     'quit-1.dat' using 4:($3/1024) with lines title "Quit memory" axes x1y2 ls 3, \
#     'quit-gc-1.dat' using 4:($3/1024) with lines title "Quit with gc memory" axes x1y2 ls 4, \
#     'quit-1.dat' using 4:($5/50) with dots title "add" axes x1y1 ls 7, \
#     'quit-1.dat' using 4:7 with dots title "delete" axes x1y2 ls 8, \
