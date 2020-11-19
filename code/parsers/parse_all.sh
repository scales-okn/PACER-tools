#!/bin/bash
# Passes all arguments through to parse_pacer.py, use -f for force and -d for debug (-fd for both)
# Example: bash parse_all.sh -fd
# See arguments documented in parse_pacer.py
for courtdir in ../../data/pacer/*/; do 
    echo $courtdir;
    python parse_pacer.py $courtdir/html/ $courtdir/json/ "$@"; 
done
