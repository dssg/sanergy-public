"""
A script that performs the following tasks:
1. Read Collections data from postgres
2. Rename primary variables (e.g., Toilet__c to ToiletId)
3. Recode primary variables (e.g., OpenTime to numeric)
4. Remove potential erroneous observations (e.g., Collection dates in 1900)
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

# Geospatial magic
import geopandas as gp
from shapely.geometry import Point
from shapely.geometry.base import BaseGeometry
from geopandas import GeoSeries, GeoDataFrame

COORD_SYSTEM = "21037" #One of the 4 systems in Kenya..., the one that contains Nairobi, apparently
#4326 is WGS84, but that one is not linear but spherical -> difficult to compute distances
COORD_WGS = "4326"

# Timeseries data
from pandas import Series, Panel
from tqdm import tqdm

import copy

# Constants
URINE_CAPACITY = 22.0
OUTLIER_KG_DAY = 400
MONTHS_WITH_SCHOOL_HOLIDAYS = [4,8,12]

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

# Load the weather data to pandas
weather = pd.read_sql('SELECT * FROM input."weather"', conn, coerce_float=True, params=None)
# Transform some of the variables
weather['year'] =weather['year'].apply(str)
weather['year'] =weather['year'].replace(to_replace=',',value="")
weather['day'] =weather['day'].apply(str)
weather['month']=weather['month'].apply(str)
weather['date_str']=weather['year']+weather['month']+weather['day']

print(weather[['year','month','day','date_str']].head())
weather['date']=pd.to_datetime(weather['date_str'], format='%Y%m%d')
weather['air_temp'] = weather['air_temp']/float(10) # units are in celsius and scaled by 10
weather['precipitation_6hr'] = weather['liquid_precipitation_depth_dimension_six_hours'] # annoyingly long variable name

weather = weather.loc[(weather['year']>=2010)] # focus the weather data on 2010 forward

# Aggregate the data by date (year/month/day)
byTIME = weather.groupby('date')
# Construct descriptive statistics across the 24hr coverage per day
aggTIME = byTIME[['air_temp',
                  'dew_point_temp',
                  'sea_level_pressure',
                  'wind_speed_rate',
                  'precipitation_6hr']].agg({'mean':np.mean,'min':np.min,'max':np.max,'sd':np.std})
# Rename/flatten the columns
aggTIME.columns = ['_'.join(col).strip() for col in aggTIME.columns.values]
# Bring date back into the dataset (bing, bang, boom)
weather = aggTIME.reset_index()

print('loading collections')
# Load the collections data to a pandas dataframe
collects = pd.read_sql('SELECT * FROM input."Collection_Data__c"', conn, coerce_float=True, params=None)
collects = standardize_variable_names(collects, RULES)

# Several days are missing from the data, we append those and sort the data :-p
ADD_ROWS = {'ToiletID':[], 'Collection_Date':[]}
ToiletID = list(set(collects['ToiletID'].tolist()))
for tt in tqdm(ToiletID):
	temp = collects.loc[(collects['ToiletID']==tt)]
	min_days = min(temp['Collection_Date'])
	max_days = max(temp['Collection_Date'])
	all_days = list(set(pd.date_range(start=min_days, end=max_days, freq="D").tolist()) - set(temp['Collection_Date'].tolist()))
	ADD_ROWS['ToiletID'].extend([tt]*len(all_days))
	ADD_ROWS['Collection_Date'].extend(all_days)
ADDING_ROWS = pd.DataFrame(ADD_ROWS)
ADDING_ROWS.head()
print('With missing days: %i' %(len(collects)))
collects = collects.append(ADDING_ROWS)
print('Adding in the missing days: %i' %(len(collects)))
collects = collects.sort_values(by=['ToiletID','Collection_Date'])

# Drop the route variable from the collections data
collects = collects.drop('Collection_Route',1)

# Create a variable capturing the assumed days since last collection
collects = collects.sort_values(by=['ToiletID','Collection_Date'])

collects['Feces_Collected'] = 1
collects.loc[((collects['Feces_kg_day']==None)|(collects['Feces_kg_day']<=0)),['Feces_Collected']] = 0
print(collects['Feces_Collected'].value_counts())

collects['Urine_Collected'] = 1
collects.loc[((collects['Urine_kg_day']==None)|(collects['Urine_kg_day']<=0)),['Urine_Collected']] = 0
print(collects['Urine_Collected'].value_counts())

# Change outier toilets to none
collects.loc[(collects['Urine_kg_day']>OUTLIER_KG_DAY),['Urine_kg_day']]=None
collects.loc[(collects['Feces_kg_day']>OUTLIER_KG_DAY),['Feces_kg_day']]=None
collects.loc[(collects['Total_Waste_kg_day']>OUTLIER_KG_DAY),['Total_Waste_kg_day']]=None

print(collects['Feces_kg_day'].describe())

#byGROUP = collects.groupby('ToiletID')

#print('applying days since variable')

def countDaysSinceWeight(x):
    """
    A function to count the number of days since the last
    recorded weight, either in Feces or in Urine, from the
    collections data.
    Args:
	DF X:	The Collections Data, reindexed with a groupby
	on the ToiletID variable.
    Return:
	DF X:	Returns the Collections Data, with the days_since
	variable for each waste type.
    """
    count_feces = 0
    count_urine = 0
    x['Feces_days_since'] = 0
    x['Urine_days_since'] = 0
    for ii in x['Feces_Collected'].index:
	#print(x.loc[ii,'Feces_Collected'])
        if (x.loc[ii,'Feces_Collected'] == 1):
            x.loc[ii,'Feces_days_since'] = 0
            count_feces = 0
        else:
            x.loc[ii,'Feces_days_since'] = count_feces
        if (x.loc[ii,'Urine_Collected'] == 1):
            x.loc[ii,'Urine_days_since'] = 0
            count_urine = 0
        else:
            x.loc[ii,'Urine_days_since'] = count_urine
        count_feces+=1
        count_urine+=1

    #print(x['days_since'].describe())
    return(x)

# Error that needs to be addressed
#byGROUP = byGROUP.apply(countDaysSinceWeight)
#collects = byGROUP.reset_index()
#print(collects['Feces_days_since'].describe())
#print(collects['Urine_days_since'].describe())

# Load the toilet data to pandas
toilets = pd.read_sql('SELECT * FROM input."tblToilet"', conn, coerce_float=True, params=None)
toilets = standardize_variable_names(toilets, RULES)

# Add in the density of Sanergy toilets surrounding a toilet (by meters)
gToilets = toilets.loc[(toilets.duplicated(subset='ToiletID')==False),['ToiletID','Latitude','Longitude']]
geometry = [Point(xy) for xy in zip(gToilets.Longitude, gToilets.Latitude)]
crs = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
gdf = gp.GeoDataFrame(gToilets[['ToiletID','Longitude','Latitude']], crs=crs, geometry=geometry)
gdf.to_crs(epsg=COORD_SYSTEM,inplace=True)
#BaseGeometry.distance(gdf.loc[109].geometry, gdf.loc[217].geometry)
TOILETS = gToilets['ToiletID'].index

print('Looping through the ID list: %i' %(len(TOILETS)))
for tt in tqdm(TOILETS):
	neighbors = [gt for gt in TOILETS if ((BaseGeometry.distance(gdf.loc[tt].geometry,gdf.loc[gt].geometry) < 5.0)&(gt!=tt))]
	gdf.loc[tt,'5m'] = len(neighbors)

	#neighbors = [gt for gt in TOILETS if ((BaseGeometry.distance(gdf.loc[tt].geometry,gdf.loc[gt].geometry) < 25.0)&(gt!=tt))]
	#gdf.loc[tt,'25m'] = len(neighbors)
    
	neighbors = [gt for gt in TOILETS if ((BaseGeometry.distance(gdf.loc[tt].geometry,gdf.loc[gt].geometry) < 50.0)&(gt!=tt))]
	gdf.loc[tt,'50m'] = len(neighbors)

	#neighbors = [gt for gt in TOILETS if ((BaseGeometry.distance(gdf.loc[tt].geometry,gdf.loc[gt].geometry) < 100.0)&(gt!=tt))]
	#gdf.loc[tt,'100m'] = len(neighbors)

print(gdf[['5m','50m']].describe())
toilets = pd.merge(toilets,
		   gdf[['ToiletID','5m','50m']],
		   on='ToiletID')

# Load the schedule data to pandas
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
schedule_wheelcart['flt-location'] = schedule_wheelcart['flt-location'].str.upper()
schedule_wheelcart['flt_name'] = schedule_wheelcart['flt_name'].str.upper()

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

collect_toilets = pd.merge(left=collect_toilets,
			   right=weather,
			   how="left",
			   left_on=['Collection_Date'],
			   right_on=['date'])

print(collect_toilets.shape)
collect_toilets['duplicated'] = collect_toilets.duplicated(subset=['Id'])
print('merge collections and weather: %i' %(len(collect_toilets.loc[(collect_toilets['duplicated']==True)])))
collect_toilets = collect_toilets.loc[(collect_toilets['duplicated']==False)]
print(collect_toilets.shape)

# Removing observations that are outside of the time range (See notes from Rosemary meeting 6/21)
collect_toilets = collect_toilets.loc[(collect_toilets['Collection_Date'] > datetime.datetime(2011,11,20)),]
print(collect_toilets.shape)

# Update negative weights as zero (See notes from Rosemary meeting 6/21)
# Update zero weights as NONE as well (see notes from Lauren meeting 6/30)
# Update keep the zero weights (see zero weight investigation)
collect_toilets.loc[((collect_toilets['Urine_kg_day'] <= 0)&(collect_toilets['Missed_Collection_Code'].isnull()==False)),['Urine_kg_day']]=None
collect_toilets.loc[((collect_toilets['Feces_kg_day'] <= 0)&(collect_toilets['Missed_Collection_Code'].isnull()==False)),['Feces_kg_day']]=None
collect_toilets.loc[((collect_toilets['Total_Waste_kg_day'] <= 0)&(collect_toilets['Missed_Collection_Code'].isnull()==False)),['Total_Waste_kg_day']]=None

# Calculate the percentage of the container full (urine/feces)
collect_toilets['waste_factor'] = 29.0 # Feces container size is 35 L
collect_toilets.loc[(collect_toilets['FecesContainer'].isin([40,45])),'waste_factor']=37.0 # Feces container size is 45 L

collect_toilets['UrineContainer_percent'] = ((collect_toilets['Urine_kg_day'])/URINE_CAPACITY)*100
collect_toilets['FecesContainer_percent'] = ((collect_toilets['Feces_kg_day'])/collect_toilets['waste_factor'])*100
print(collect_toilets[['FecesContainer_percent','UrineContainer_percent']].describe())

# Incorporating the school closure variable
collect_toilets['year'] = collect_toilets['Collection_Date'].dt.year
collect_toilets['month'] = collect_toilets['Collection_Date'].dt.month
collect_toilets['day'] = collect_toilets['Collection_Date'].dt.day

collect_toilets['School_Closure'] = False
collect_toilets.loc[(collect_toilets['month'].isin(MONTHS_WITH_SCHOOL_HOLIDAYS)),'School_Closure'] = True
print(collect_toilets['School_Closure'].value_counts())

# Push merged collection and toilet data to postgres
print(collect_toilets.loc[1,['UrineContainer','UrineContainer_percent']])
conn.execute('DROP TABLE IF EXISTS premodeling."toiletcollection"')
collect_toilets.to_sql(name='toiletcollection',
			schema="premodeling",
			con=engine,
			chunksize=10000)
print('end');
