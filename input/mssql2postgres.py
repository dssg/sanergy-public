import dateutil
from argparse import ArgumentParser

import sqlalchemy
import pymssql
import pandas as pd
from tqdm import tqdm

import dbconfig

p = ArgumentParser()
p.add_argument("-m", "--mssql", help="credentials file for MS SQL Server")
p.add_argument("-p", "--pgsql", help="credentials file for PostgreSQL server")
args = p.parse_args()

mssql = dbconfig.engine_generator(args.mssql, "mssql+pymssql")
pgsql = dbconfig.engine_generator(args.pgsql, "postgres")

tables = pd.read_sql("""SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_TYPE='BASE TABLE'""", mssql)

for table in tqdm(tables['TABLE_NAME']):
    if pymssql.__version__ < '2.2.0':
        # The pymssql driver unfortunately has a bug that doesn't allow reading
        # reads dates as datetime objects. We'll parse them back. Ref:
        # https://groups.google.com/forum/?fromgroups#!topic/pymssql/1y3-h1tusiA
        datetime_cols_to_convert = pd.read_sql("""
            SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{}' AND DATA_TYPE = 'datetime2'
            """.format(table), mssql)['COLUMN_NAME']
    else:
        datetime_cols_to_convert = []
    df = pd.read_sql("SELECT * FROM " + table, mssql)
    for col in datetime_cols_to_convert:
        df[col] = pd.to_datetime(df[col])
    df.to_sql(table, pgsql, index=False, schema='input', if_exists='replace')
