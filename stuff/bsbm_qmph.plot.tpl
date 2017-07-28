set output "bsbm_qmph.pdf"
set terminal pdf

set ylabel 'QMPH'
set boxwidth 0.5

#set style data histogram
#set style histogram errorbars cluster gap 1
#set style fill solid

#set logscale y 10
set yrange [0:]

set style histogram errorbars gap 1 lw 1
set style data histograms
#plot "ctcf.dat" using 2:3:xtic(1)
set key autotitle columnhead

set xtics rotate by 45 right

plot {% for scenario in scenarios %} '{{ file_qmph }}' using {{ scenario.column-1 }}:{{ scenario.column }}:xtic(1) title "{{ scenario.setup }}" lt rgb "{{ scenario.color }}",\
{% endfor %}
