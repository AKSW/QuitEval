#!/bin/sh

for i in "01" "02" "03" "03" "04" "05" "06" "07" "08" "09" "10" ;
do
    export i

    PARAMS=""
    export PARAMS

    ./run-instance.sh

    PARAMS="-nv"
    export PARAMS

    ./run-instance.sh

    PARAMS="-gc"
    export PARAMS

    ./run-instance.sh
done
