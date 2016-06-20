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
from sqlalchemy import create_engine

# Visualizing the data
import matplotlib

# Analyzing the data
import pandas as pd
import pprint, re
import numpy as np

# Timeseries data
from pandas import Series, Panel

# Constants
URINE_DENSITY = 1.0
FECES_DENSITY = 0.6

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

engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s' %(dbconfig.config['user'],
							dbconfig.config['password'],
							dbconfig.config['host'],
							dbconfig.config['port']))
conn = engine.connect()
print('connected to postgres')

# Load the collections data to a pandas dataframe
collects = pd.read_sql('SELECT * FROM input."Collection_Data__c"', conn, coerce_float=True, params=None)
collects = standardize_variable_names(collects, RULES)

# Load the toilet data to pandas
toilets = pd.read_sql('SELECT * FROM input."tblToilet"', conn, coerce_float=True, params=None)
toilets = standardize_variable_names(toilets, RULES)

# Load the toilet data to pandas
schedule = pd.read_sql('SELECT * FROM input."FLT_Collection_Schedule__c"', conn, coerce_float=True, params=None)
schedule = standardize_variable_names(schedule, RULES)

# Convert toilets opening/closing time numeric:
toilets.loc[(toilets['OpeningTime']=="30AM"),['OpeningTime']] = "0030"
toilets['OpeningTime'] = pd.to_numeric(toilets['OpeningTime'])
toilets['ClosingTime'] = pd.to_numeric(toilets['ClosingTime'])
toilets['TotalTime'] = toilets['ClosingTime'] - toilets['OpeningTime']  
print(toilets[['OpeningTime','ClosingTime','TotalTime']].describe())

# Convert the container data to numeric
toilets['UrineContainer'] = pd.to_numeric(toilets['UrineContainer'].str.replace("L",""))
toilets['FecesContainer'] = pd.to_numeric(toilets['FecesContainer'].str.replace("L",""))
print("Feces: %i-%iL" %(np.min(toilets['FecesContainer']), np.max(toilets['FecesContainer'])))
print("Urine: %i-%iL" %(np.min(toilets['UrineContainer']), np.max(toilets['UrineContainer'])))

# Note the unmerged toilet records
pprint.pprint(list(set(toilets['ToiletID'])-set(collects['ToiletID'])))

# Merge the collection and toilet data
collect_toilets = pd.merge(collects,
				toilets,
				on="ToiletID")
print(collect_toilets.shape)

# Merge the collection and toilet data
collect_toilets = pd.merge(left=collect_toilets,
				right=schedule,
				how="left",
				left_on=["ToiletID","Collection_Date"],
				right_on=["ToiletID","Planned_Collection_Date"])
print(collect_toilets.shape)

# Calculate the percentage of the container full (urine/feces)
collect_toilets['UrineContainer_percent'] = ((collect_toilets['Urine_kg_day']/URINE_DENSITY)/collect_toilets['UrineContainer'])*100
collect_toilets['FecesContainer_percent'] = ((collect_toilets['Feces_kg_day']/FECES_DENSITY)/collect_toilets['FecesContainer'])*100
print(collect_toilets[['FecesContainer_percent','UrineContainer_percent']].describe())

# Push merged collection and toilet data to postgres
print(collect_toilets.loc[1,['UrineContainer','UrineContainer_percent']])
collect_toilets.to_sql(name='toiletcollection', 
			schema="premodeling",
			con=engine,
			chunksize=10000)
print('end');
