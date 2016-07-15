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
		df labels


	NOTE: this is written specifically for the very first pass through the pipeline.
	Will have to update this function to be able to deal with later, more general, features table sizes.
	"""

			  labels=labels_big.iloc[:, [1]]
              labels=labels.fillna(0); # put zeros in place of NaN
              features=features_big.iloc[:,[4,5,6]]
              features=features.fillna(0);

              return(features,labels)