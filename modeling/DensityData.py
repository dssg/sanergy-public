# Looking at the Density Data
# Analyzing the data
import pandas as pd
import pprint, re, datetime
import numpy as np
from scipy import stats
from sklearn import linear_model

# Connect to the database
import dbconfig
import psycopg2
from sqlalchemy import create_engine

# Visualizing the data
import matplotlib


print('Density: Class')
print('loadData')

class Density:
	def __init__(self):
		self.engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s' %(dbconfig.config['user'],
											dbconfig.config['password'],
											dbconfig.config['host'],
											dbconfig.config['port']))
		self.conn = self.engine.connect()
		print('connected to postgres')

		self.collects = pd.DataFrame()
		self.density = pd.DataFrame()

	def loadData(self):
		# Load the toilet collection data to pandas
		collects = pd.read_sql('SELECT * FROM premodeling.toiletcollection', self.conn, coerce_float=True, params=None)
		pprint.pprint(collects.keys())

		collects = collects[['ToiletID','ToiletExID','Collection_Date','Area','Feces_kg_day','year','month']]
		pprint.pprint(collects.keys())

		# Load the density data to pandas
		density = pd.read_sql('SELECT * FROM premodeling.toiletdensity', self.conn, coerce_float=True, params=None)
		pprint.pprint(density.keys())

		# Return the data
		self.collects = collects
		self.density = density
		return(collects, density)
