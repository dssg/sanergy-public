#!/usr/bin/env python
import logging
import pdb
import numpy as np

from sklearn import (svm, ensemble, tree,
                     linear_model, neighbors, naive_bayes)
from sklearn.feature_selection import SelectKBest
import statsmodels.tsa 


log = logging.getLogger(__name__)


class ConfigError():
    pass

class Model(object):
    """ 
    a class for the model
    """

    def __init__(self, modeltype,params=None):
        """
        Args:
        """
        self.params=params
        self.modeltype=modeltype
        self.trained_model=None



    def run(train_x, train_y, test_x, model, parameters):

        if model == "AR":

        else:

            results, modelobj = gen_model(train_x, train_y, test_x, model,
                                      parameters)

        return results, modelobj


    def gen_model(train_x, train_y, test_x, model, parameters):
        """Trains a model and generates risk scores.
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

        log.info("Training {} with {}".format(model, parameters))
        modelobj = define_model(model, parameters)
        modelobj.fit(train_x, train_y)
        result_y = modelobj.predict(test_x)

        return result_y, modelobj


    def define_model(model, parameters):
        if model == "AR":
            return statsmodels.tsa.ar_model.AR(
                max_order=parameters['max_order'])
        if model == "RandomForest":
            return ensemble.RandomForestClassifier(
                n_estimators=parameters['n_estimators'])
        else:
            raise ConfigError("Unsupported model {}".format(model))
