#!/usr/bin/env bash

# Downloads hourly weather data for a given station
# Find the codes and available dates at ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv

# Inputs:
#  1. Directory Name where you'll store the data
DIRNAME=$1
#  2. USAF code
USAF=$2
#  3. WBAN code 
WBAN=$3
 

# grab beginning and ending years for the given station
(cd $DIRNAME && wget -N 'ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-history.csv')
begin=$(cat ${DIRNAME}/isd-history.csv | grep -E "${USAF}.*${WBAN}" | cut -d, -f10 | cut -c2-5)
end=$(cat ${DIRNAME}/isd-history.csv | grep -E "${USAF}.*${WBAN}" | cut -d, -f11 | cut -c2-5)


for ((year=$begin; year<=$end; year++))
do
	(cd $DIRNAME && wget -N "ftp://ftp.ncdc.noaa.gov/pub/data/noaa/isd-lite/${year}/${USAF}-${WBAN}-${year}.gz") 
done
