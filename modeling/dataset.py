"""
A script that is imported by run.py to manage dataset extraction and writing results to the postgres database.
1. A function to connect to the database
2. A function that takes a query and returns two datasets (e.g., labels and features)
3. A function that writes a dataset to postgres
"""

# Connect to the database
import sanergy.input.dbconfig as dbconfig
import psycopg2
from sqlalchemy import create_engine

# Analyzing the data
import pandas as pd
import numpy as np

# Helper functions
import re, pprint
from datetime import datetime, date, timedelta

# For logging errors
import logging



def get_db(config, log):
	engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s' %(dbconfig.config['user'],
	dbconfig.config['password'],
	dbconfig.config['host'],
	dbconfig.config['port']))
	try:
		conn = engine.connect()
		log.info('connected to postgres')
	except:
		log.warning('Failure to connect to postgres')
	db = {'connection': conn, 'table': config['db']['table'], 'database': config['db']['database']}
	return(db)

def temporal_split(config_cv, day_of_week=None, floating_window=False):
	"""
	A function to produce a list of temporal folds for modeling.
	Args
	   DATE START_DATE	The start date for the experiment
	   DATE END_DATE	... end date for the experiment
	   DICT TRAIN_ON	Dictionary specifying the amount of time to train on per fold.
				 The function uses the date.timedelta function, here the keys
				 will correspond to units of time, and the value will be the
				 delta. (e.g., {'microseconds':500, 'days':5})
	   DICT TEST_ON		Dictionary specifying the amount of time to test on per fold.
				 Identical to the train_on variable.
	   NUM DAY_OF_WEEK	Corresponds to the Day of Week value (Monday to Sunday, 0-6),
				 that each fold should start from.
	   BOOL FLOATING_WINDOW	Should the splits reflect a floating window or training on
				 all data to the fake today - test window, testing on the test window?
	Returns
	   LIST[dict]	List of test and train time ranges per fold
	"""
	start_date = config_cv['start_date']
	end_date = config_cv['end_date']
	fake_freq = config_cv['fake_freq']
	train_on = config_cv['train_on']
	test_on = config_cv['test_on']
	log = logging.getLogger("Sanergy Collection Optimizer")

	# Compute the full date range (in days)
	start_date = datetime.strptime(start_date,'%Y-%m-%d')
	end_date = datetime.strptime(end_date,'%Y-%m-%d')
	date_range = end_date - start_date

	list_of_dates = []
	for day in pd.date_range(start=start_date, end=end_date, freq=fake_freq).tolist():
		#print(('Fake Today:', day))
		fold = {'train_start':day-timedelta(days=train_on),
			'train_end':day-timedelta(days=1),
			'test_start':day,
			'test_end': day+timedelta(days=test_on),
			'window_start': day-timedelta(days=train_on),
			'window_end': day +timedelta(days=test_on)}
		#pprint.pprint(fold)
		# Adjust the training window
		if floating_window==False:
			fold['window_start'] = day-timedelta(days=train_on)
			fold['train_start'] = day-timedelta(days=train_on)
		# Test whether the day is the right day
		if bool(day_of_week):
			if (day.weekday() != day_of_week):
				fold = {}
		if bool(fold):
			# Do not extend past the dataset
			if (end_date >= fold['window_end']):
				list_of_dates.append(fold)
	report = 'Total %i folds from %s to %s' %(len(list_of_dates),
						start_date.strftime('%Y-%m-%d'),
						end_date.strftime('%Y-%m-%d'))
	log.info(report)
	#print(report)
	return(list_of_dates)


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

def grab_collections_data(db, config, log ):
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
    {"or":[('=',"school"),('=',"commercial")]}
    DICT[dict] UNIQUE	The unique variables for the dataset
    (e.g., {'Collection_Date':{}}, {'ToiletID':{}}
    DICT LAGGED		The variables to be lagged are keys, the direction, and
    number of rows forward or back are values.
    (e.g., {'Feces_kg_day':{'function':'lag',
    'rows':[1,2]}}
    Returns:
    DF Y_LABELS		Pandas dataframe for the response variables
    DF X_FEATURES		Pandas dataframe for the feature variables
    """
    config_Xy = config['Xy']
    response_f = config_Xy['response_f']
    response_u = config_Xy['response_u']
    features = config_Xy['features']
    unique = config_Xy['unique']
    lagged = config_Xy['lagged']
    RESPONSE_RENAMER = {'response_f':'response', 'response_u':'response'}

    # Create the list of all variables requested from the database
    list_of_variables = [response_f['variable']]+[response_u['variable']]+features.keys()+unique.keys()
    list_of_predictors = features.keys()
    list_of_variables = ['"'+lv+'"' for lv in list_of_variables]
    log.info('Request variable(s): %s' %(','.join(list_of_variables)))

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
	log.debug("Retrieved the dataset.")
	# Incorporate DAILY features (function reuses 'conditions', 'unique' variables, and 'db'
	for ll in lagged.keys():
		daily_data = demand_daily_data(db,
				rows=lagged[ll]['rows'],
				feature=ll,
				function=lagged[ll]['function'],
				unique=unique.keys(),
				conditions=conditions)
		# Merge the DAILY features with DATASET
		dataset = pd.merge(dataset,
				   daily_data,
				   how='inner',
				   on=unique.keys())
		dataset['duplicated']=dataset[unique.keys()].duplicated()
		print(dataset['duplicated'].value_counts())
		dataset = dataset.loc[dataset['duplicated']==False]
		dataset = dataset.drop(["duplicated"], axis=1)
	# Return the response variable
    #Binary response variable? Ignore this.
	# if (bool(response['split'])==True):
	# 	statement = ""
	# 	for split in response['split']:
	# 		if (split=='and'):
	# 			statement = '&'.join(['(dataset["%s"]%s%s)' %(response['variable'],sp[0],sp[1])
	# 					for sp in response['split'][split]])
	# 		elif (split=='or'):
	# 			statement = '|'.join(['(dataset["%s"]%s%s)' %(response['variable'],sp[0],sp[1])
	# 					for sp in response['split'][split]])
	# 	dataset['response'] = False
	# 	dataset.loc[(eval(statement)),"response"] = True
	# else:
	dataset['response_f'] = dataset[response_f['variable']]
    dataset['response_u'] = dataset[response_u['variable']]

    #Link ToiletIds to areas. For now, just link by the route. For now, assume the route is available.
    toilet_route = dataset[[config['cols']['toiletname'], config['cols']['date'], config['cols']['route']]]
    db['connection'].execute('DROP TABLE IF EXISTS modeling."toilet_route"')
    toilet_route.to_sql(name='toilet_route',
    schema="modeling",
    con=db['connection'],
    chunksize=20000)
    dataset.drop([config['cols']['route']], axis=1, inplace = True)

    #Code the categorical/string variables to dummies
    str_vars = [row for row, tp in dataset.dtypes.iteritems() if tp == 'object']
    vars_to_dummify = set(list_of_predictors).intersection(str_vars)
    dataset = pd.get_dummies(dataset, columns = vars_to_dummify, drop_first = True)


    # Divide the dataset into a LABELS and FEATURES dataframe so that they link by UNIQUE variables
    dataset = dataset.sort_values(by=unique.keys())
    db['connection'].execute('DROP TABLE IF EXISTS modeling."dataset"')
    dataset.to_sql(name='dataset',
    schema="modeling",
    con=db['connection'],
    chunksize=20000)



    x_features = dataset.drop(['response_f', 'response_u',response_f['variable'], response_u['variable']], axis=1)

    yf_labels = dataset[['response_f']+unique.keys()].rename(columns=RESPONSE_RENAMER)
    yu_labels = dataset[['response_u']+unique.keys()].rename(columns=RESPONSE_RENAMER)
    # Insert tables into database
    return(yf_labels, yu_labels, x_features, toilet_route)

def grab_from_features_and_labels(db, fold, config):

    """
    A function that subsets the features df and labels df stored in the Postgres, into train and test features and labels, based on the fold info (train start, train end, test start, test end )

    Args
    DICT FOLD start and end date for both train and test set, in the fomat{"train":(start, end),"test":(start, end)}
    Returns
    df features train
    df labels train
    df features test
    df labels test
    """
    RESPONSE_RENAMER = {'response_f':'response', 'response_u':'response'}
    dataset = pd.read_sql('select * from modeling.dataset where (("Collection_Date" >= '+"'"+fold['train_start'].strftime('%Y-%m-%d')+"'"+') and ("Collection_Date" <= '+"'"+fold['test_end'].strftime('%Y-%m-%d')+"'"+'))', db['connection'], coerce_float=True, params=None)
    toilet_routes = pd.read_sql('select * from modeling.toilet_route', db['connection'], coerce_float=True, params=None)

    #TODO: Fix this...
    dataset = dataset.fillna(0) #A hack to make it run for now...
    #Drop the toilets that do not have contiguous data.
    # Note that missing collections are filled with NaN'd rows, so if a toilet is not contiguous, it must mean that it appeared or disappeared during the fold period -> ignore it.
    toilet_groups = dataset.groupby(config['cols']['toiletname'])
    toilets = dataset[config['cols']['toiletname']].unique()
    number_of_days = max(toilet_groups.size())
    contiguous_toilets = [t for t in toilets if (toilet_groups.size()[t] == number_of_days)]
    dataset = dataset.loc[dataset[config['cols']['toiletname']].isin(contiguous_toilets)]
    #Sort for the purposes of later functions...
    dataset = dataset.sort_values(by=['Collection_Date','ToiletID'])


    features_train = dataset.loc[((dataset['Collection_Date']>=fold["train_start"]) & (dataset['Collection_Date']<=fold["train_end"]))].drop(['response_f','response_u',config['Xy']['response_f']['variable'], config['Xy']['response_u']['variable']],axis=1)
    features_test = dataset.loc[((dataset['Collection_Date']>=fold["test_start"]) & (dataset['Collection_Date']<=fold["test_end"]))].drop(['response_f','response_u',config['Xy']['response_f']['variable'], config['Xy']['response_u']['variable']],axis=1)

    labels_train_u = dataset.loc[((dataset['Collection_Date']>=fold["train_start"]) & (dataset['Collection_Date']<=fold["train_end"])),['response_u','Collection_Date','ToiletID']].rename(columns=RESPONSE_RENAMER)
    labels_train_f = dataset.loc[((dataset['Collection_Date']>=fold["train_start"]) & (dataset['Collection_Date']<=fold["train_end"])),['response_f','Collection_Date','ToiletID']].rename(columns=RESPONSE_RENAMER)
    labels_test_f = dataset.loc[((dataset['Collection_Date']>=fold["test_start"]) & (dataset['Collection_Date']<=fold["test_end"])),['response_f','Collection_Date','ToiletID']].rename(columns=RESPONSE_RENAMER)
    labels_test_u = dataset.loc[((dataset['Collection_Date']>=fold["test_start"]) & (dataset['Collection_Date']<=fold["test_end"])),['response_u','Collection_Date','ToiletID']].rename(columns=RESPONSE_RENAMER)
    return(features_train, labels_train_f, labels_train_u, features_test, labels_test_f, labels_test_u, toilet_routes)

def format_features_labels(features_big,labels_big):

	"""
	A function that takes in the features and labels df as created in the function
	grab_from_features_and_labels. It drops the unnecessary columns in the features
	and labels dataframes, and it changes NaN values to 0,
	so that the final dataframes features and labels can be used by models in sklearn.


	Args
		df features_big
		df labels_big
	Returns
		df features
		df/np.array? labels


	NOTE: this is written specifically for the very first pass through the pipeline.
	Will have to update this function to be able to deal with later, more general, features table sizes.
	"""

	labels=labels_big['response'].fillna(0).values
	#labels=(labels_big['response'].fillna(0)).values.flatten(); # put zeros in place of NaN
	#features=features_big.iloc[:,[4,5,6]]
	features=features_big.iloc[:,4:]
	features=features.fillna(0);

	return(features,labels)

def create_enveloping_fold(folds):
	"""
	Create a fold that subsumes all folds in [folds], that is, it starts where the first fold starts and ends where the last fold ends. Make the test and train folds of the enveloping fold the same.
	Warning: This assumes that the folds form a contiguous block. If it's not contiguous, the enveloping fold takes
	essentially the convex hull, in 1D that means the smallest interval that comprises all folds.
	"""
	all_start = min([fold['train_start'] for fold in folds] + [fold['test_start'] for fold in folds])
	all_end = max([fold['train_end'] for fold in folds] + [fold['test_end'] for fold in folds])
	window_start = min([fold['window_start'] for fold in folds])
	window_end = max([fold['window_end'] for fold in folds])
	enveloping_fold = {'train_start': all_start,
	'train_end': all_end,
	'test_start': all_start,
	'test_end': all_end,
	'window_start': window_start,
	'window_end': window_end}
	return(enveloping_fold)

def create_future(fold, features_old, cfg_parameters):
	"""
	Just for testing purposes.
	Sets up a replicate of the last day(s) data to create new data for testing. But in reality,
	we should be able to create features for the upcoming days from past data, so this would not be needed???
	"""
	last_day = fold['window_end']
	next_days = [last_day + timedelta(days=i) for i in xrange(1,(cfg_parameters['prediction_horizon'] +1 ))]
	old_features_unique = features_old.drop_duplicates(subset='ToiletID')
	l_future_features = []
	for day in  next_days:
		next_day_features = old_features_unique.copy()
		next_day_features["Collection_Date"] = day
		l_future_features.append(next_day_features)
	future_features = pd.concat(l_future_features, ignore_index=True)
	return(future_features)
