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
import pprint, re, datetime
import numpy as np
from scipy import stats

# Timeseries data
from pandas import Series, Panel

import copy

# Constants
URINE_DENSITY = 1.0
FECES_DENSITY = 1.0
OUTLIER_KG_DAY = 400

COLUMNS_COLLECTION_SCHEDULE1 = ['"flt_name"','"flt-location"','"responsible_wc"','"crew_lead"','"field_officer"','"franchise_type"','"route_name"','"sub-route_number"',
'"mon"','"tue"','"wed"','"thur"','"fri"','"sat"','"sun"','"extra_containers"','"open?"']
COLUMNS_COLLECTION_SCHEDULE2 = copy.deepcopy(COLUMNS_COLLECTION_SCHEDULE1)
COLUMNS_COLLECTION_SCHEDULE2.remove('"extra_containers"')
COLUMNS_COLLECTION_SCHEDULE2.remove('"open?"')
COLUMNS_COLLECTION_SCHEDULE2.extend(['"extra_container?"','"open"'])
#Put in the sql format
SQL_COL_COLLECTION1=",".join(COLUMNS_COLLECTION_SCHEDULE1)
SQL_COL_COLLECTION2=",".join(COLUMNS_COLLECTION_SCHEDULE2)

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

conn.execute("DROP TABLE IF EXISTS premodeling.toiletcollection")

# Load the collections data to a pandas dataframe
collects = pd.read_sql('SELECT * FROM input."Collection_Data__c"', conn, coerce_float=True, params=None)
collects = standardize_variable_names(collects, RULES)

# Drop the route variable from the collections data
collects = collects.drop('Collection_Route',1)

# Change outier toilets to none
collects.loc[(collects['Urine_kg_day']>OUTLIER_KG_DAY),'Urine_kg_day']=None
collects.loc[(collects['Feces_kg_day']>OUTLIER_KG_DAY),'Feces_kg_day']=None
collects.loc[(collects['Total_Waste_kg_day']>OUTLIER_KG_DAY),'Total_Waste_kg_day']=None

# Load the toilet data to pandas
toilets = pd.read_sql('SELECT * FROM input."tblToilet"', conn, coerce_float=True, params=None)
toilets = standardize_variable_names(toilets, RULES)

# Load the toilet data to pandas
schedule = pd.read_sql('SELECT * FROM input."FLT_Collection_Schedule__c"', conn, coerce_float=True, params=None)
schedule = standardize_variable_names(schedule, RULES)

# Correct the schedule_status variable, based on Rosemary (6/21)
print(schedule['Schedule_Status'].value_counts())
schedule.loc[(schedule['Schedule_Status']=="School"),'Schedule_Status']="DC school is closed"
schedule.loc[(schedule['Schedule_Status']=="#N/A"),'Schedule_Status']="Remove record from table"
schedule.loc[(schedule['Schedule_Status']=="Closed"),'Schedule_Status']="Closed by FLI"
schedule.loc[(schedule['Schedule_Status']=="`Collect"),'Schedule_Status']="Collect"
schedule.loc[(schedule['Schedule_Status']=="Closure Chosen by FLO"),'Schedule_Status']="Closed by FLO"
schedule.loc[(schedule['Schedule_Status']=="Collect"),'Schedule_Status']="Collect"
schedule.loc[(schedule['Schedule_Status']=="Closed by FLO"),'Schedule_Status']="Closed by FLO"
schedule.loc[(schedule['Schedule_Status']=="Daily"),'Schedule_Status']="Collect"
schedule.loc[(schedule['Schedule_Status']=="Demolished"),'Schedule_Status']="Closed by FLI"
schedule.loc[(schedule['Schedule_Status']=="NULL"),'Schedule_Status']="Remove record from table"
schedule.loc[(schedule['Schedule_Status']=="Periodic"),'Schedule_Status']="Periodic"
schedule.loc[(schedule['Schedule_Status']=="DC school is closed"),'Schedule_Status']="DC school is closed"
schedule.loc[(schedule['Schedule_Status']=="no"),'Schedule_Status']="Closed by FLO"
schedule.loc[(schedule['Schedule_Status']=="Closed by FLI"),'Schedule_Status']="Closed by FLI"
print(schedule['Schedule_Status'].value_counts())

# Load the collection schedule data.
schedule_wheelcart = pd.read_sql('SELECT ' + SQL_COL_COLLECTION1 + '  FROM input."collection_schedule_wheelbarrow"', conn, coerce_float=True, params=None)
schedule_tuktuk = pd.read_sql('SELECT ' + SQL_COL_COLLECTION1 + '  FROM input."collection_schedule_tuktuk"', conn, coerce_float=True, params=None)
schedule_truck = pd.read_sql('SELECT ' + SQL_COL_COLLECTION2 + '  FROM input."collection_schedule_truck"', conn, coerce_float=True, params=None)
schedule_truck.rename(columns={'"extra_container?"':'"extra_containers"','"open"':'"open?"'},inplace=True)
schedule_wheelcart.append(schedule_truck)
schedule_wheelcart.append(schedule_tuktuk)
schedule_wheelcart = standardize_variable_names(schedule_wheelcart,RULES)
print(schedule_wheelcart.shape)

# Drop columns that are identical between the Collections and FLT Collections records
schedule = schedule.drop('CreatedDate',1)
schedule = schedule.drop('CurrencyIsoCode',1)
schedule = schedule.drop('Day',1)
schedule = schedule.drop('Id',1)
schedule = schedule.drop('Name',1)
schedule = schedule.drop('SystemModstamp',1)
print(schedule.keys())

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
collect_toilets['duplicated'] = collect_toilets.duplicated(subset=['Id'])
print('merge collections and toilets: %i' %(len(collect_toilets.loc[(collect_toilets['duplicated']==True)])))

# Merge the collection and toilet data
collect_toilets = pd.merge(left=collect_toilets,
				right=schedule,
				how="left",
				left_on=["ToiletID","Collection_Date"],
				right_on=["ToiletID","Planned_Collection_Date"])
print(collect_toilets.shape)
collect_toilets['duplicated'] = collect_toilets.duplicated(subset=['Id'])
print('merge collections and schedule: %i' %(len(collect_toilets.loc[(collect_toilets['duplicated']==True)])))
duplicate_ids = set(collect_toilets.loc[(collect_toilets['duplicated']==True),'ToiletID'])
print(collect_toilets.loc[(collect_toilets['ToiletID'].isin(list(duplicate_ids))),['ToiletID','Planned_Collection_Date','Collection_Date','Route_Name']])
collect_toilets = collect_toilets[(collect_toilets['duplicated']==False)]
print(collect_toilets.shape)

#collect_toilets = pd.merge(left=collect_toilets,
#					right=schedule_wheelcart,
#					how="left",
#					left_on=["ToiletExID"],
#					right_on=["flt_name"])

#print(collect_toilets.shape)
#print(schedule_wheelcart.keys())
#collect_toilets['duplicated'] = collect_toilets.duplicated(subset=['Id'])
#print('merge collections and schedule wheelcart: %i' %(len(collect_toilets.loc[(collect_toilets['duplicated']==True)])))
#collect_toilets = collect_toilets.loc[(collect_toilets['duplicated']==False)]
#print(collect_toilets.shape)
#duplicate_ids = set(collect_toilets.loc[(collect_toilets['duplicated']==True),'ToiletID'])
#pprint.pprint(duplicate_ids)
#print(collect_toilets.loc[(collect_toilets['ToiletID'].isin(list(duplicate_ids))),['ToiletID','Id','duplicated','sub-route_name','route_name','open?']])
#print(collect_toilets.loc[(collect_toilets['Id']=='a0AD000000TcGuGMAV')])


# Removing observations that are outside of the time range (See notes from Rosemary meeting 6/21)
collect_toilets = collect_toilets.loc[(collect_toilets['Collection_Date'] > datetime.datetime(2011,11,20)),]
print(collect_toilets.shape)

# Update negative weights as zero (See notes from Rosemary meeting 6/21)
collect_toilets.loc[(collect_toilets['Urine_kg_day'] < 0),['Urine_kg_day']]=None
collect_toilets.loc[(collect_toilets['Feces_kg_day'] < 0),['Feces_kg_day']]=None
collect_toilets.loc[(collect_toilets['Total_Waste_kg_day'] < 0),['Total_Waste_kg_day']]=None

# Calculate the percentage of the container full (urine/feces)
collect_toilets['waste_factor'] = 25.0 # Feces container size is 35 L
collect_toilets.loc[(collect_toilets['FecesContainer']==45),['waste_factor']]=37.0 # Feces container size is 45 L

collect_toilets['UrineContainer_percent'] = ((collect_toilets['Urine_kg_day']/URINE_DENSITY)/collect_toilets['UrineContainer'])*100
collect_toilets['FecesContainer_percent'] = ((collect_toilets['Feces_kg_day']/FECES_DENSITY)/collect_toilets['waste_factor'])*100
print(collect_toilets[['FecesContainer_percent','UrineContainer_percent']].describe())

# Push merged collection and toilet data to postgres
print(collect_toilets.loc[1,['UrineContainer','UrineContainer_percent']])
conn.execute('DROP TABLE IF EXISTS premodeling."toiletcollection"')
collect_toilets.to_sql(name='toiletcollection',
			schema="premodeling",
			con=engine)
print('end');
