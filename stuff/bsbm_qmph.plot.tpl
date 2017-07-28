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
unset key

set xtics rotate by 45 right

plot '{{ file_qmph }}' using 2:3:xtic(1) lt rgb "#000"
