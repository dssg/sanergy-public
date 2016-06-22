#!/usr/bin/env bash

routeFirst="Kayaba"
routes=( "Kiambiu" "KNCC" "KR CC" "Mathare" "Kingstone" "Paradise" "Ruben" "Tassia" "Simbacool")
routeTruck="Truck"
routeTuk="Tuktuk"

#"Bad" columns
#Figure them out using csvcut -n ...
COLS="$(printf "%s" {23..34}, 35)"
COLSTRUCK="$(printf "%s" {25..35}, 36)"
COLSTUK="$(printf "%s" {23..34}, 35)"

mkdir tmp
#Loop over routes and
in2csv --sheet "$routeFirst" $INPUT1 | tail -n +2 > tmp/wheelcart.csv
for route in "${routes[@]}"; do
    in2csv --sheet "$route" --no-header-row $INPUT1 | tail -n +3 >> tmp/wheelcart.csv
done

in2csv --sheet "$routeTruck" $INPUT1 | tail -n +2 > tmp/truck.csv
in2csv --sheet "$routeTuk" $INPUT1 | tail -n +2 > tmp/tuktuk.csv

csvcut -C "$COLS" tmp/wheelcart.csv > $OUTPUT0 #tmp/wheelCut.csv
csvcut -C "$COLSTRUCK" tmp/truck.csv > $OUTPUT1 #tmp/truckCut.csv
csvcut -C "$COLSTUK" tmp/tuktuk.csv > $OUTPUT2 #tmp/tukCut.csv

rm -r data/input/tmp/
