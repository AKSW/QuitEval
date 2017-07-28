set output "bsbm.pdf"
set terminal pdf

set ylabel 'queries per second (qps)'
set boxwidth 0.5

#set style data histogram
#set style histogram errorbars cluster gap 1
#set style fill solid

set logscale y 10
set yrange [0.01:5000]

set style histogram errorbars gap 1 lw 1
set style data histograms
#plot "ctcf.dat" using 2:3:xtic(1)
set key autotitle columnhead

set xtics rotate by 45 right

{% for scenario in scenarios %}# {{scenario.setup}}
{% endfor %}
plot {% for scenario in scenarios %} '{{ file }}' using {{ scenario.column }}:{{ scenario.column+1 }}:xtic(1) lt rgb "{{ scenario.color }}",\
{% endfor %}
