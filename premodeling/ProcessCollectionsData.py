"""
A script that performs the following tasks:
1. Read Collections data from postgres
2. Rename primary variables (e.g., Toilet__c to ToiletId)
3. Recode primary variables (e.g., OpenTime to numeric)
4. Remove potential erroneous observations (e.g., Collection dates in 1900)
5. Produce summary statistics based on primary variables, export results
6. Return the cleaned Collections data to a research table in postgres
"""

# Connect to the database
import dbconfig
import psycopg2

# Visualizing the data
import matplotlib

# Analyzing the data
import pandas as pd
import pprint, re
import numpy as np

# Timeseries data
from pandas import Series, Panel

# Connect to the postgres database
conn = psycopg2.connect(host=dbconfig.config['host'],
			user=dbconfig.config['user'],
			password=dbconfig.config['password'],
			port=dbconfig.config['port'])

# Load the collections data to a pandas dataframe
collects = pd.read_sql('SELECT * FROM public."Collection_Data__c",					conn,
			coerce_float=True,
			params=None)
print(list(collects.columns.values))
conn.close()

