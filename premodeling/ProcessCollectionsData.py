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

# Helper functions
RULES = [("^(Toilet__c|ToiletID)$","ToiletID"),
		("Toilet_External_ID__c","ToiletExID"),
		("(.*)(Faeces)(.*)","\\1Feces\\3"),
		("__c","")]

def standardize_variable_names(table, RULES):
	"""
	Script to standardize the variable names in the tables
	PARAM DataFrame table: A table returned from pd.read_sql
	PARAM list[tuples]: A list of tuples with string replacements, i.e., (string, replacement)
	RET table
	"""
	variableNames = list(table.columns.values)
	standardizedNames = {} # Pandas renames columns with a dictionary object
	for v in variableNames:
		f = v
		for r in RULES:
			f = re.sub(r[0],r[1],f)
		print '%s to %s' %(v,f)
		standardizedNames[v] = f
	table = table.rename(columns=standardizedNames)
	return table

# Connect to the postgres database
conn = psycopg2.connect(host=dbconfig.config['host'],
			user=dbconfig.config['user'],
			password=dbconfig.config['password'],
			port=dbconfig.config['port'])

print('connected to postgres')
# Load the collections data to a pandas dataframe
collects = pd.read_sql('SELECT * FROM public."Collection_Data__c" limit 10',conn,coerce_float=True,params=None)
collects = standardize_variable_names(collects, RULES)

# Load the toilet data to pandas
toilets = pd.read_sql('SELECT * FROM public."tblToilet" limit 10',conn,coerce_float=True,params=None)
toilets = standardize_variable_names(toilets, RULES)
print(list(toilets.columns.names))

# Note the unmerged toilet records
pprint.pprint(list(set(toilets['ToiletID'])-set(collects['ToiletID'])))
# Merge the collection and toilet data

collect_toilets = pd.merge(collects,
				toilets,
				on="ToiletID")
print(collect_toilets.shape)



conn.close()

