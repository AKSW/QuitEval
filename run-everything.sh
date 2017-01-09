#!/bin/sh

kill_group() {
    PGID=$1
    echo "kill group: $PGID"
    kill -TERM -$PGID
}

PGID=$( ps opgid= $$ | tr -d ' ' )

echo "group is: "$PGID

# Ctrl-C trap. Catches INT signal
trap "kill_group $PGID; exit 0" INT

if [ -n "$1" ]; then start=$1; else start=1 ; fi
if [ -n "$2" ]; then end=$2; else end=10; fi

echo `seq -w $start $end`

for i in `seq -w $start $end` ;
do
    export i

    # --garbagecollection
    ./run-instance.sh -gc
    pgrep -g $PGID | xargs --no-run-if-empty ps fp;

    ./run-instance.sh
    pgrep -g $PGID | xargs --no-run-if-empty ps fp;

    # --disableversioning
    ./run-instance.sh -nv
    pgrep -g $PGID | xargs --no-run-if-empty ps fp;
done
