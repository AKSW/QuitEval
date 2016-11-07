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

for i in "01" "02" "03" "04" "05" "06" "07" "08" "09" "10" ;
do
    export i

    ./run-instance.sh
    pgrep -g $PGID | xargs --no-run-if-empty ps fp;

    ./run-instance.sh -nv
    pgrep -g $PGID | xargs --no-run-if-empty ps fp;

    ./run-instance.sh -gc
    pgrep -g $PGID | xargs --no-run-if-empty ps fp;
done
