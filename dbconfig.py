import sqlalchemy

def parse_pass(pass_file):
    """ Parse a configuration file with bash-like assignments into a dict

    :param str pass_file: file with the credential information
    :return dictionary with host, port, database, user & password keys
    :rtype dict
    """
    with open(pass_file, 'r') as f:
        passinfo = f.read()
    lines = passinfo.strip().split('\n')
    opts = ['host','port','database','user','password']
    config = dict()
    for line in lines:
        var, val = line.split('=', 1)
        for opt in opts:
            if var.lower().endswith(opt):
                config[opt] = val.strip()
                break
    return config

def engine_generator(pass_file='default_profile', engine_type='postgresql'):
    """ Return a SQL Alchemy engine given a profile and engine type

    :param str pass_file: file with the credential information
    :param str engine_type: SQL database type, e.g., postgresql or mssql+pymssql
    :rtype sqlalchemy.engine"""
    config = parse_pass(pass_file)
    sql_eng_str = engine_type+"://"+config['user']+":"+config['password']+"@"+config['host']+'/'+config['database']
    engine = sqlalchemy.create_engine(sql_eng_str)
    return engine

# For backwards compatibility, also create a dictionary named `config` with
# the postgres setup and credentials
config = parse_pass('default_profile')
