set output "bsbm_aqet.pdf"
set terminal pdf

set ylabel 'average query execution time (aqet)'
set boxwidth 0.5

#set style data histogram
#set style histogram errorbars cluster gap 1
#set style fill solid

set logscale y 10
set yrange [0.01:5000]

set style histogram errorbars gap 1 lw 1
set style data histograms
set key autotitle columnhead

set xtics rotate by 45 right

{% for scenario in scenarios %}# {{scenario.setup}}
{% endfor %}
plot {% for scenario in scenarios %} '{{ file }}' using {{ scenario.column }}:{{ scenario.column+1 }}:xtic(1) lt rgb "{{ scenario.color }}",\
{% endfor %}
