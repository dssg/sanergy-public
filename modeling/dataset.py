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

def grab_collections_data(db, response, features, unique, label):
	"""
		A function to return a postgres query as a Pandas data frame
		Args:
		  DICT DB		A connection objection, database name/table
		  DICT RESPONSE		The variable to be predicted 
					e.g., Feces container between 30% and  40% full:
						{'variable':'FecesContainer_percent',
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
		  Pandas DataFrame based on the query	
	"""
	# Create the list of all variables requested from the database
	list_of_variables = [response['variable']]+features.keys()+unique.keys()
	list_of_variables = ['"'+lv+'"' for lv in list_of_variables]
	log.info('Requestion variable(s): %s' %(','.join(list_of_variables)))

	# Determine the conditions statement for the data request
	conditions = []
	conditions = write_statement(features)
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
	return(dataset)

# Experiments
db={'connection':conn, 'table':'toiletcollection', 'database':'premodeling'}
response = {'variable':'Feces_kg_day','split':{'and':[('>',3),('<',7)]}}
features = {'Urine_kg_day':{'and':[('<=',10),('>',3)],'not':('=',5),'list':['4','7','8','5']}}
unique = {'ToiletID':{'list':['a08D000000i1KgnIAE']}, 'Collection_Date':{'and':[('>',"'2012-01-01'"),('<',"'2014-01-01'")]}}
label = False
data = grab_collections_data(db, response, features, unique, label)
print(data)
