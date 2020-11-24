#!/bin/bash
# After reading the directory path, passes all arguments through to parse_pacer.py; use -f for force and -d for debug (-fd for both)
# Example: bash parse_all.sh ../../data/pacer -fd
# See arguments documented in parse_pacer.py

dir=$1
shift
for courtdir in $dir/*/; do 
    echo $courtdir;
    python parse_pacer.py $courtdir/html/ $courtdir/json/ "$@"; 
done
