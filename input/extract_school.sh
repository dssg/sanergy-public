#!/usr/bin/env bash

sheetFirst="SF_check"

#"Bad" columns
#Figure them out using csvcut -n ...
#COLS="$(printf "%s" {23..34}, 35)"

mkdir tmp
#Loop over routes and
in2csv --sheet "$sheetFirst" $INPUT1 | tail -n +2 > tmp/school_by_route.csv

csvcut -n tmp/school_by_route.csv
#csvcut -C "$COLS" tmp/school_by_route.csv > $OUTPUT0 #tmp/school_by_route.csv

rm -r data/input/tmp/

