#!/bin/sh

prepare_repository () {
    REPOSITORY=$1
    git init $REPOSITORY
    cp stuff/.gitattributes $REPOSITORY/
    sed "s/.$/<urn:bsbm> ./g" ../bsbmtools-0.2/dataset.nt | sort -u > $REPOSITORY/graph.nq
    cd $REPOSITORY
    git add .gitattributes graph.nq
    git commit -m "init graph"
    git tag init-graph
    cd ..
}

prepare_workingdirectory () {
    RUNDIR=$1

    cp stuff/config.ttl $RUNDIR/
}

prepare_workingdirectory_bare () {
    REPOSITORY=$1
    RUNDIR=$2

    git clone --bare $REPOSITORY $RUNDIR
    prepare_workingdirectory $RUNDIR
}

run_monitor () {
    RUNDIR=$1
    LOGDIR=$2
    storePID=$3

    file=$LOGDIR/mem-$RUNDIR
    pid=$storePID

    while true; do date +%s | awk '{ printf $1 " "}' | tee -a $file ; du -s $RUNDIR | awk '{ printf $1 " "}' | tee -a $file; ps -o rss $pid | grep -v RSS | tee -a $file ; sleep 2 ;done
}

run_store () {
    PARAMS=$@
    echo "run store with params:" $PARAMS
    quit-store --pathspec $PARAMS &
    storePID=$!
    echo "store in run_store:" $storePID
    return $storePID
}

run_bsbm () {
    RUNDIR=$1
    LOGDIR=$2

    cd ../bsbmtools-0.2/
    ./testdriver http://localhost:5000/sparql -runs 1500 -w 40 -dg "urn:bsbm" -o ../quit-eval/$LOGDIR/$RUNDIR.xml -ucf usecases/exploreAndUpdate/sparql.txt -udataset dataset_update.nt -u http://localhost:5000/sparql
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

PARAMS=$@

BARE=true

DIR_PARAMS=""
for var in "$PARAMS"
do
    DIR_PARAMS=$DIR_PARAMS"$var"
done

RUNDIR="quit"$DIR_PARAMS"-"$i

if $BARE; then
    RUNDIR_WD=$RUNDIR"_wd"
else
    RUNDIR_WD=$RUNDIR
fi

LOGDIR=$RUNDIR"-logs"

mkdir $RUNDIR

if $BARE; then
    mkdir $RUNDIR_WD
fi

mkdir $LOGDIR

prepare_repository $RUNDIR_WD

if $BARE; then
    prepare_workingdirectory_bare $RUNDIR_WD $RUNDIR
else
    prepare_workingdirectory $RUNDIR
fi

cd $RUNDIR
git config gc.auto 256

#for bash: export storePID
run_store $PARAMS
storePID=$?
echo "store: " $storePID

cd ..

sleep 10

run_monitor $RUNDIR $LOGDIR $storePID & monitorPID=$!
echo "monitor: " $monitorPID

jobs -l

sleep 10

run_bsbm $RUNDIR $LOGDIR

sleep 10

terminate $storePID

jobs -l

#kill %run_monitor
kill $monitorPID

sleep 2
