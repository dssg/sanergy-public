def grab_from_features_and_labels(fold):

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

	import dataset
	features=pd.read_sql('SELECT * FROM modeling."features"', dataset.conn, coerce_float=True, params=None)
	labels=pd.read_sql('SELECT * FROM modeling."labels"', dataset.conn, coerce_float=True, params=None)

	features_train = features.loc[((features['Collection_Date']>=fold["train_start"]) & (features['Collection_Date']<=fold["train_end"]))]
    features_test = features.loc[((features['Collection_Date']>=fold["test_start"]) & (features['Collection_Date']<=fold["test_end"]))]
    labels_train = labels.loc[((labels['Collection_Date']>=fold["train_start"]) & (labels['Collection_Date']<=fold["train_end"]))]
    labels_test = labels.loc[((labels['Collection_Date']>=fold["test_start"]) & (labels['Collection_Date']<=fold["test_end"]))]
    
    return(features_train, labels_train, features_test, labels_test)