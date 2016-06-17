"""
The purpose of this code is to move the tables provided by Sanergy from the PUBLIC schema to the INPUT schema. 
"""

## Import basic libraries for communicating with the postgres database
from sqlalchemy import create_engine
import dbconfig 
import psycopg2

## Import helper functions
import pprint, re

## Constants
MOVE_TO = u"input"
MOVE_FROM = u"public"
KNOWN_TABLES = [u'Collection_Data__c',
		u'_IPA_tbl_system_user',
		u'_IPA_tbl_toilet',
		u'_IPA_tbl_transactions',
		u'FLT_Collection_Schedule__c',
		u'_IPA_tbl_user_card',
		u'tblToilet',
		u'_IPA_tbl_user']

engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s' %(dbconfig.config['user'],
							dbconfig.config['password'],
							dbconfig.config['host'],
							dbconfig.config['port']))
connection = engine.connect()
result = connection.execute("select table_schema, table_name from information_schema.tables;")
result = [res for res in result.fetchall() if ((res[0]==MOVE_FROM)&(res[1] in KNOWN_TABLES))]

for kt in result:
	print('ALTER TABLE %s."%s" SET SCHEMA %s' %(MOVE_FROM, kt[1], MOVE_TO))
	connection.execute('ALTER TABLE %s."%s" SET SCHEMA %s' %(MOVE_FROM, kt[1], MOVE_TO))
print('end')
