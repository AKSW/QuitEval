#!/bin/sh

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

quit-store $PARAMS &
storePID=$!
echo "store: " $storePID

cd ..

sleep 10

export storePID
export LOGDIR
export RUNDIR
./watch.sh &
watchPID=$!
echo "watch: " $watchPID

sleep 10

cd ../bsbmtools-0.2/
./testdriver http://localhost:5000/sparql -runs 200 -w 40 -dg "urn:bsbm" -o ../quit-eval/$LOGDIR/$RUNDIR.xml -ucf usecases/exploreAndUpdate/sparql.txt -udataset dataset_update.nt -u http://localhost:5000/sparql
cp run.log ../quit-eval/$LOGDIR/$RUNDIR-run.log
cd ../quit-eval/

sleep 10

kill $watchPID
sleep 2
pkill -TERM -P $storePID
