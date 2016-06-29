"""
A script that is imported by run.py to manage dataset extraction and writing results to the postgres database.
1. A function to connect to the database
2. A function that takes a query and returns two datasets (e.g., labels and features)
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
from datetime import date, timedelta

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

def write_statement(vardict):
	"""
	A function to write a conditional statement based on the conditions in a variable
	Args
	  DICT[dict] VARDICT	A dictionary of variable names, where the values are conditions
	Returns
	  LIST[str] Conditions	A list of condition statements
	"""
	conditions = []
	for feat in vardict:
		if bool(vardict[feat])==True:
			# Dictionary is not empty, parse the split values into a statement
			for split in vardict[feat]:
				if ((split=='and')|(split=="or")):
					statement = split.join(['("%s"%s%s)' %(feat,sp[0],sp[1]) 
							for sp in vardict[feat][split]])
				elif (split=='not'):
					statement = split + ' "%s"%s%s' %(feat,
							vardict[feat][split][0],
							vardict[feat][split][1])
				elif (split=='list'):
					statement = '"%s"' %(feat) + "=any('{%s}')" %(','.join(vardict[feat][split]))
				conditions.append('('+statement+')')  
	pprint.pprint(conditions)
	return(conditions)

def demand_daily_data(db, rows=[], feature='', function='lag', unique=['ToiletID','Collection_Date'], conditions=None):
	"""
	A function to generate by day variables for a feature

	Args:
	   DICT DB		Connection object (see grab_collections_data)
	   LIST ROWS		List of rows
	   STR FEATURE		A feature name to create daily records for
	   STR FUNCTION		Apply either the LAG or LEAVE function (in the future, maybe some other functions)
	   LIST UNIQUE		List of unique identifiers
	   STR CONDITIONS	Apply the conditions string (see grab_collections_data)
	Returns:
	   DF DAILY_DATA	Pandas data frame of daily variables
	"""
	
	# Reprocess the unique list to account for capitalization
	unique = ','.join(['"%s"' %(uu) for uu in unique])

	# Construct the sql statement using window functions (e.g., OVER and LAG/LEAVE)	
	statement = 'SELECT %s' %(unique)
	for rr in rows:
		statement += ', %s("%s", %i, NULL) OVER(order by %s) as "%s_%s%i" ' %(function, 
										      feature,
										      rr,
										      unique,
										      feature,
										      function,
										      rr)
	# Complete the statement
	statement += "FROM %s.%s %s ORDER BY %s" %(db['database'],
				  		   db['table'],
					  	   conditions,
						   unique)
	# Execute the statement
	daily_data = pd.read_sql(statement, 
				con=db['connection'], 
				coerce_float=True, 
				params=None)
	# Return the lagged/leave data
	return(daily_data)

def grab_collections_data(db, response, features, unique, label):
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
				(e.g., Not the school franchise types:
					{'and':[('=','school')]}
					Or school and commercial:
					{"or":[('=',"school"),('=',"commerical")]}
	  DICT[dict] UNIQUE	The unique variables for the dataset
				(e.g., {'Collection_Date':{}}, {'ToiletID':{}}
	  DICT LABEL		Apply a label to the RESPONSE variable

	Returns:
	  DF Y_LABELS		Pandas dataframe for the response variables
	  DF X_FEATURES		Pandas dataframe for the feature variables
	"""
	# Create the list of all variables requested from the database
	list_of_variables = [response['variable']]+features.keys()+unique.keys()
	list_of_variables = ['"'+lv+'"' for lv in list_of_variables]
	log.info('Requestion variable(s): %s' %(','.join(list_of_variables)))

	# Determine the conditions statement for the data request
	conditions = []
	conditions.extend(write_statement(unique))
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
	# Retrieve the dataset from postgres
	dataset = pd.read_sql(statement, 
				con=db['connection'], 
				coerce_float=True, 
				params=None)
	# Incorporate DAILY features (function reuses 'conditions', 'unique' variables, and 'db' 
	daily_data = demand_daily_data(db,
			rows=[1,2,3], 
			feature=response['variable'], 
			function='lag', 
			unique=unique.keys(), 
			conditions=conditions)
	# Merge the DAILY features with DATASET
	dataset = pd.merge(dataset,
			   daily_data,
			   how='inner',
			   on=unique.keys())

	# Return the response variable
	if (bool(response['split'])==True):
		statement = ""
		for split in response['split']:
			if (split=='and'):
				statement = '&'.join(['(dataset["%s"]%s%s)' %(response['variable'],sp[0],sp[1]) 
						for sp in response['split'][split]])
			elif (split=='or'):
				statement = '|'.join(['(dataset["%s"]%s%s)' %(response['variable'],sp[0],sp[1]) 
						for sp in response['split'][split]])
		dataset['response'] = False
		dataset.loc[(eval(statement)),"response"] = True
	else:
		dataset['response'] = dataset[response['variable']]
	# Divide the dataset into a LABELS and FEATURES dataframe so that they link by UNIQUE variables
	y_labels = dataset[["response",response['variable']]+unique.keys()]
	x_features = dataset.drop(["response",response['variable']]+unique.keys(), axis=1)
	# Insert tables into database
	db['connection'].execute('DROP TABLE IF EXISTS modeling."labels"')
	y_labels.to_sql(name='labels',
			schema="modeling",
			con=db['connection'],
			chunksize=1000)
	db['connection'].execute('DROP TABLE IF EXISTS modeling."features"')
	x_features.to_sql(name='features',
			schema="modeling",
			con=db['connection'],
			chunksize=1000)

	return(y_labels, x_features)

# Experiments
#############
def test():
	db={'connection':conn, 
		'table':'toiletcollection',
		 'database':'premodeling'}
	response = {'variable':'Feces_kg_day',
			'split':{'and':[('>',3),('<',7)]}}
	features = {'Urine_kg_day':{'and':[('<=',10),('>',3)],
				    'not':('=',5),
				    'list':['4','7','8','5']}}
	unique = {'ToiletID':{'list':['a08D000000i1KgnIAE']},
		  'Collection_Date':{'and':[('>',"'2012-01-01'"),('<',"'2014-01-01'")]}}
	label = False

	y,x = grab_collections_data(db, response, features, unique, label)

	print('\nThe LABELS (y) dataframe, includes both the RESPONSE variable and the original ("%s")' %(response['variable']))
	pprint.pprint(y.keys())
	print(y.head())

	print('\nThe FEATURES (x) dataframe includes %i variables, %i rows of data (unique identifiers: %s)' %(len(x.keys()), len(x), ','.join(unique.keys()) ))
	pprint.pprint(x.keys())
	print(x.head())
