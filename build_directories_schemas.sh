#!/usr/bin/env bash

# load database credentials (stored in default_profile)
eval $(cat default_profile | sed 's/^/export /')

# create directories on analytics server using data and psql
# directories that appear in the input Drakefile
cat input/Drakefile | grep -E "(^data/|^psql/)" | cut -d ' ' -f1 | 
parallel 'dirname {} | xargs mkdir -p'

# create schema on postgres server
cat input/Drakefile_"$DEPT" | grep -E "^psql/" | cut -d '/' -f4 |
tr [:upper:] [:lower:] | sort | uniq |
parallel 'echo "create schema if not exists {};" | psql -f-'
