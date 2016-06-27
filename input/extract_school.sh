#!/usr/bin/env bash

sheetFirst="SF_check"

#Figure them out using csvcut -n ...
#COLS="$(printf "%s" {23..34}, 35)"

mkdir tmp
#Read the xlsx file to a csv:
in2csv --sheet "$sheetFirst" $INPUT1 | tail -n +2 > tmp/school_by_route.csv

#Check the file and prepare the postgresql schema
#in2csv --sheet "$sheetFirst" /mnt/data/sanergy/input/Logistics_schedule.xlsx | tail -n +1 > tmp/school_by_route.csv
#tail -n +1 tmp/school_by_route.csv | iconv -t ascii | tr [:upper:] [:lower:]| tr ' ' '_' |tr '-' '_' | csvsql -i postgresql 

#csvcut -C "$COLS" tmp/school_by_route.csv > $OUTPUT0 #tmp/school_by_route.csv

rm -r data/input/tmp/

