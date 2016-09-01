import dateutil
from argparse import ArgumentParser

import sqlalchemy
import pymssql
import pandas as pd
from tqdm import tqdm

def engine_generator(pass_file='pgpass', engine_type='postgresql'):
    """ Generate a sqlalchemy engine to database. Parses a password file in the
    format that psql expects. `address:port:user:database:password`

    :param str pass_file: file with the credential information
    :return sqlalchemy.engine object engine: object created  by create_engine() in sqlalchemy                         
    :rtype sqlalchemy.engine                                                                                          
    """
    with open(pass_file, 'r') as f:
        passinfo = f.read()
    passinfo = passinfo.strip().split(':')
    host_address = passinfo[0]
    port = passinfo[1]
    user_name = passinfo[2]
    name_of_database = passinfo[3]
    user_password = ''.join(passinfo[4:])
    sql_eng_str = engine_type+"://"+user_name+":"+user_password+"@"+host_address+'/'+name_of_database
    engine = sqlalchemy.create_engine(sql_eng_str)
    return engine

p = ArgumentParser()
p.add_argument("-m", "--mssql", help="credentials file for MS SQL Server")
p.add_argument("-p", "--pgsql", help="credentials file for PostgreSQL server")
args = p.parse_args()

mssql = engine_generator(args.mssql, "mssql+pymssql")
pgsql = engine_generator(args.pgsql, "postgres")

tables = pd.read_sql("""SELECT TABLE_NAME FROM information_schema.tables WHERE TABLE_TYPE='BASE TABLE'""", mssql)

for table in tqdm(tables['TABLE_NAME'][1:]):
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
