#!/bin/bash

rm disambiguator.csv
echo 'Running consolidation for disambiguator'
python3 consolidate.py $1
