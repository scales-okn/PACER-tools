#!/bin/bash

dir=$1
shift
while getopts s:e: flag; do
    case "$flag" in
        s)  
            startdir=${OPTARG}
            ;;
        e)
            enddir=${OPTARG}
            ;;
    esac
done

shift 4
for courtdir in $dir/*; do 
    if [ ! $(basename $courtdir) \< $(basename $startdir) ] && [ ! $(basename $courtdir) \> $(basename $enddir) ]
    then
        echo "Running on ${courtdir}"
        python parse_pacer.py $courtdir/html/ $courtdir/json/ "$@";
    else
        echo "Skipping ${courtdir}";
    fi
done
