#!/bin/sh

file=$LOGDIR/mem-$RUNDIR pid=$storePID ; while true; do date +%s | awk '{ printf $1 " "}' | tee -a $file ; du -s $RUNDIR | awk '{ printf $1 " "}' | tee -a $file; ps -o rss $pid | grep -v RSS | tee -a $file ; sleep 2 ;done
