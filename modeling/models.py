#!/usr/bin/env python
import logging
import pdb
import numpy as np
import datetime

from sklearn import svm, ensemble, tree, linear_model, neighbors, naive_bayes
from sklearn.feature_selection import SelectKBest
import statsmodels.tsa

from sanergy.modeling.dataset import grab_from_features_and_labels, format_features_labels

log = logging.getLogger(__name__)


class ConfigError(NameError):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class Model(object):
    """
    A class for collection scheduling model(s)
    """

    def __init__(self, modeltype_waste, modeltype_schedule, config, parameters_waste=None, parameters_schedule=None, ):
        """
        Args:
        """
        self.parameters_waste=parameters_waste
        self.parameters_schedule=parameters_schedule
        self.modeltype_waste=modeltype_waste
        self.modeltype_schedule=modeltype_schedule
        self.config = config


    def run(self, train_x, train_y, test_x):
        waste_model = WasteModel(self.modeltype_waste, self.parameters_waste, self.config, train_x, train_y) #Includes gen_model?
        waste_matrix = waste_model.predict(test_x)[0]
        schedule_model = ScheduleModel(waste_matrix, self.modeltype_schedule, self.parameters_schedule, train_x, train_y, confgi) #For simpler models, can ignore train_x and train_y?
        collection_matrix = schedule_model.compute(waste_matrix, test_x) #Again, might be able to ignore test_x
        return collection_matrix, schedule_model


class WasteModel(object):
    """
    Predict the waste matrix: a dataframe with toilets in rows and days in columns. Values are predicted waste weights.
    Args:
      train_x: training features
      train_y: training target variable
      test_x: testing features
      model (str): model type
      parameters: hyperparameters for model
    Returns:
      result_y: predictions on test set
      modelobj: trained model object
    """
    def __init__(self, modeltype, parameters, config, train_x = None, train_y = None):
        self.parameters = parameters
        self.modeltype = modeltype
        self.config = config
        if (train_x is not None) and (train_y is not None):
            self.gen_model(train_x, train_y)

    def predict(self, test_x):
        features = test_x.drop([self.config['cols']['toiletname'], self.config['cols']['date']], axis=1)
        result_y = self.trained_model.predict(test_x)
        waste_matrix = self.form_the_waste_matrix(test_x[[self.config['cols']['toiletname'], self.config['cols']['date']]], result_y, self.config['implementation']['prediction_horizon'][0] )

        return waste_matrix, result_y

    def form_the_waste_matrix(self, indices, y, horizon):
        """
        Args:
          indices (DataFrame): Includes the date and toilet_id
          y (vector?): a vector of predictions, each index correpsonding to the toilet and date from indices
          horizon (int): how long in the future should the  waste_matrix go?
        """

        #Take the next prediction horizon days, extract them from features.
        today = indices[self.config['cols']['date']].min() #The first day in the features
        #7 (or horizon) days from today
        next_days = [today + datetime.timedelta(days=delta) for delta in range(0,horizon)]
        #Append y to indices
        waste_matrix = indices.copy()
        waste_matrix['y'] = y
        #Sort the indices and only include the ones within the following days
        waste_matrix.sort_values(by=self.config['cols']['date'], inplace=True)
        waste_matrix = waste_matrix[waste_matrix[self.config['cols']['date']].isin(next_days)]
        #Pivot the waste matrix
        waste_matrix = waste_matrix.pivot(index = self.config['cols']['toiletname'], columns = self.config['cols']['date'],values='y')

        return waste_matrix

    def gen_model(self, train_x, train_y):
        log.info("Training {0} with {1}".format(self.modeltype, self.parameters))
        self.trained_model = self.define_model()
        #Strip the index parameters (e.g., toilet id and day) from the train data
        labels=train_y['response'].fillna(0).values
        #For features, assume they have already been subsetted, use everything except toilet_id, day...
        features = train_x.drop([config['cols']['toiletname'], config['cols']['date']], axis=1)

        #fit the model...
        self.trained_model.fit(features, labels)
        return self.trained_model

    def define_model(self):
        if self.modeltype == "AR":
            return statsmodels.tsa.ar_model.AR(
                max_order=self.parameters['max_order'])
        if self.modeltype == "RandomForest":
            return ensemble.RandomForestRegressor(
                n_estimators=self.parameters['n_estimators'])
            #return ensemble.RandomForestClassifier(
            #    n_estimators=self.parameters['n_estimators'])
        else:
            raise ConfigError("Unsupported model {0}".format(self.modeltype))

class ScheduleModel(object):
    """
    Based on the waste matrix, create the collection schedule. The same format as the waste matrix, but values are 0/1 (skip/collect)
    """

def run_models_on_folds(folds, loss_function, db, experiment):
    losses = []
    log = logging.getLogger("Sanergy Collection Optimizer")
    for i_fold, fold in enumerate(folds):
        #log.debug("Fold {0}: {1}".format(i_fold, fold))
        features_train_big, labels_train_big, features_test_big, labels_test_big = grab_from_features_and_labels(db, fold)
        features_train,labels_train=format_features_labels(features_train_big,labels_train_big)
        features_test,labels_test=format_features_labels(features_test_big,labels_test_big)

        # 5. Run the models
        #print(features_train.shape)
        #print(labels_train.shape)
        model = Model(experiment.model,experiment.parameters)
        yhat, trained_model = model.run(features_train, labels_train, features_test)

        # 6. From the loss function
        losses.append(loss_function.evaluate(yhat, labels_test))

        """
        TODO:
        7. We have to save the model results and the evaluation in postgres
        Experiment x Fold, long file
        """
    return(losses)
