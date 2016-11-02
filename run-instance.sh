#!/bin/sh

run_monitor () {
    RUNDIR=$1
    LOGDIR=$2
    storePID=$3

    file=$LOGDIR/mem-$RUNDIR
    pid=$storePID

    while true; do date +%s | awk '{ printf $1 " "}' | tee -a $file ; du -s $RUNDIR | awk '{ printf $1 " "}' | tee -a $file; ps -o rss $pid | grep -v RSS | tee -a $file ; sleep 2 ;done
}

run_store () {
    PARAM=$1
    quit-store $PARAM &
    storePID=$!
    return $storePID
}

run_bsbm () {
    RUNDIR=$1
    LOGDIR=$2

    cd ../bsbmtools-0.2/
    ./testdriver http://localhost:5000/sparql -runs 200 -w 40 -dg "urn:bsbm" -o ../quit-eval/$LOGDIR/$RUNDIR.xml -ucf usecases/exploreAndUpdate/sparql.txt -udataset dataset_update.nt -u http://localhost:5000/sparql
    cp run.log ../quit-eval/$LOGDIR/$RUNDIR-run.log
    cd ../quit-eval/
}

terminate () {
    storePID=$1
    echo "terminate instance of store: $storePID"
    pgrep -P $storePID | xargs --no-run-if-empty ps fp;
    CPIDS=$(pgrep -P $storePID); kill -TERM $storePID $CPIDS
    #CPIDS=$(pgrep -P $storePID); (sleep 5 && kill -KILL $storePID $CPIDS &); kill -TERM $storePID $CPIDS
    echo "wait for store to terminate"
    wait $storePID
    echo "terminated store: $storePID"
}

#trap 'kill $(jobs -p)' EXIT SIGINT
#trap "terminate $storePID $watchPID; exit 0" INT

RUNDIR="quit"$PARAMS"-"$i
LOGDIR=$RUNDIR"-logs"

mkdir $RUNDIR
mkdir $LOGDIR
git init $RUNDIR
git config gc.auto 256
cp stuff/.gitattributes $RUNDIR/
cp stuff/config.ttl $RUNDIR/
sed "s/.$/<urn:bsbm> ./g" ../bsbmtools-0.2/dataset.nt | sort -u > $RUNDIR/graph.nq
cd $RUNDIR
git add .gitattributes config.ttl graph.nq
git commit -m "init graph"
git tag init-graph

run_store $PARAMS
storePID=$?
echo "store: " $storePID

cd ..

sleep 10

run_monitor $RUNDIR $LOGDIR $storePID &
watchPID=$!
echo "watch: " $watchPID

sleep 10

run_bsbm $RUNDIR $LOGDIR

sleep 10

terminate $storePID

exit 0
