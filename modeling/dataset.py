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
import re

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

def return_from_database(conn, parameters):
	"""
		A function to return a postgres query as a Pandas data frame
		Args:
			CONN A connection objection
			PARAMETERS dict[variables, database, table, conditions]
		Returns:
			Pandas DataFrame based on the query	
	"""
	if isinstance(parameters['variables'],list):
		parameters['variables'] = ','.join(['"%s"' %(pr) for pr in parameters['variables']]) 
	statement = 'select %s from %s.%s %s' %(parameters['variables'],
						parameters['database'],
						parameters['table'],
						parameters['conditions'])
	dataset = pd.read_sql(statement, 
				con=conn, 
				coerce_float=True, 
				params=None)
	return(dataset)

# Experiments
db = return_from_database(conn, {'variables':'*',
				'database':'premodeling',
				'table':'toiletcollection',
				'conditions':'limit 10'})
print(db.shape)

db = return_from_database(conn, {'variables':['ToiletID','Feces_kg_day','Urine_kg_day'],
				'database':'premodeling',
				'table':'toiletcollection',
				'conditions':'limit 10'})
print(db.shape)
