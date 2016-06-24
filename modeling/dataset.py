"""
A script that is imported by run.py to manage dataset extraction and writing results to the postgres database.
1. A function to connect to the database
2. A function that takes a query and returns a dataset
3. A function that writes a dataset to postgres
"""

# Connect to the database
import dbconfig
import psycopg2
from sqlalchemy import create_engine

# Analyzing the data
import pandas as pd

# Helper functions
import re, pprint

# For logging errors
import logging

log = logging.getLogger(__name__)
engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s' %(dbconfig.config['user'],
							dbconfig.config['password'],
							dbconfig.config['host'],
							dbconfig.config['port']))
try:
	conn = engine.connect()
	print('connected to postgres')
except:
	log.warning('Failure to connect to postgres')

def grab_collections_data(db, response, features, start_date, end_date, label):
	"""
		A function to return a postgres query as a Pandas data frame
		Args:
		  DICT DB		A connection objection, database name/table
		  DICT RESPONSE		The variable to be predicted 
					e.g., Feces container between 30% and  40% full:
						{'type':'binary',
						 'variable':'FecesContainer_percent',
						 'split':{'and':[('>',30),('<',40)]}
		  DICT[dict] FEATURES	The variables of interest and any subsets on those variables
					e.g., Not the school franchise types:
						{'and':[('=','school')]}
						Or school and commercial:
						{"or":[('=',"school"),('=',"commerical")]}
		  DT START_DATE		A minimum or equal to start date
					(e.g., "Collection_Date" >= '2012-01-01')
		  DT END_DATE		A maximum or equal to end date
					(e.g., "Collection_Date" <= '2012-01-01')
		  DICT LABEL		Apply a label to the RESPONSE variable
	
		Returns:
		  Pandas DataFrame based on the query	
	"""
	# Create the list of all variables requested from the database
	list_of_variables = [response['variable']] + features.keys() + ['Collection_Date','ToiletID']
	list_of_variables = ['"'+lv+'"' for lv in list_of_variables]
	log.info('Requestion variable(s): %s' %(','.join(list_of_variables)))

	# Determine the conditions statement for the data request
	conditions = []
	for feat in features:
		if bool(features[feat])==True:
			# Dictionary is not empty, parse the split values into a statement
			for split in features[feat]:
				statement = split.join(['("%s"%s%s)' %(feat,sp[0],sp[1]) 
						for sp in features[feat][split]])
				conditions.append('('+statement+')')
	if len(conditions)>0:
		conditions = 'and'.join(conditions)
		conditions = 'where '+conditions
	else:
		conditions = ""				
	# Create the SQL statement requesting the data
	statement = "select %s from %s.%s %s" %(','.join(list_of_variables),
						db['database'],
						db['table'],
						conditions)
	dataset = pd.read_sql(statement, 
				con=db['connection'], 
				coerce_float=True, 
				params=None)
	return(dataset)

# Experiments
db={'connection':conn, 'table':'toiletcollection', 'database':'premodeling'}
response = {'type':'binary','variable':'Feces_kg_day','split':{'and':[('<=',10),('>',3)]}}
features = {'Urine_kg_day':{'and':[('<=',10),('>',3)]}}
start_date = '2012-01-01'
end_date = '2014-01-01'
label = False
data = grab_collections_data(db, response, features, start_date, end_date, label)
print(data)
