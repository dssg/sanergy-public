#!/usr/bin/env python
import logging
import pdb
import numpy as np

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
    a class for the model
    """

    def __init__(self, modeltype,parameters=None):
        """
        Args:
        """
        self.parameters=parameters
        self.modeltype=modeltype
        self.trained_model=None



    def run(self, train_x, train_y, test_x):
        if self.modeltype == "AR":
            pass
        else:
            results, modelobj = self.gen_model(train_x, train_y, test_x)
            return results, modelobj


    def gen_model(self, train_x, train_y, test_x):
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

        log.info("Training {0} with {1}".format(self.modeltype, self.parameters))
        self.trained_model = self.define_model()
        self.trained_model.fit(train_x, train_y)
        result_y = self.trained_model.predict(test_x)

        return result_y, self.trained_model


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
        print(yhat[0:5])

        # 6. From the loss function
        losses.append(loss_function.evaluate(yhat, labels_test))

        """
        TODO:
        7. We have to save the model results and the evaluation in postgres
        Experiment x Fold, long file
        """
    return(losses)
