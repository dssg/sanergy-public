#!/bin/bash

# This script assumes you added your private key to ssh

# load database credentials (stored in default_profile)
eval $(cat default_profile | sed 's/^/export /')

# randomly choose a port between 50,000 and 59,999 (typically open)
# choose a port for R and a port for jupyter notebook
r_port=$(awk -v min=50000 -v max=59999 'BEGIN{srand(); print int(min+rand()*(max-min+1))}')
echo "R port: " $r_port
ssh -fNL $r_port:localhost:8787 $USER_AND_SERVER

jupyter_port=$(awk -v min=50000 -v max=59999 'BEGIN{srand(); print int(min+rand()*(max-min+1))}')
echo "Jupyter port: " $jupyter_port
ssh -fNL $jupyter_port:localhost:8787 $USER_AND_SERVER


