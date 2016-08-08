#!/usr/bin/env python
import logging
import pdb
import numpy as np
import pandas as pd
import datetime
import sys

from sklearn import svm, ensemble, tree, linear_model, neighbors, naive_bayes
from sklearn.svm import SVR
from sklearn.feature_selection import SelectKBest
import statsmodels.tsa

from sanergy.modeling.dataset import grab_from_features_and_labels, format_features_labels
from sanergy.modeling.output import write_evaluation_into_db

log = logging.getLogger(__name__)


class ConfigError(NameError):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

class FullModel(object):
    """
    A class for collection scheduling model(s)
    """

    def __init__(self, config, modeltype_waste, modeltype_schedule="simple", parameters_waste=None, parameters_schedule=None):
        """
        Args:
        """
        self.parameters_waste=parameters_waste
        self.parameters_schedule=parameters_schedule
        self.modeltype_waste=modeltype_waste
        self.modeltype_schedule=modeltype_schedule
        self.config = config


    def run(self, train_x, train_y, test_x, waste_past = None):
        """
        Args:
          waste_past: A past waste matrix. Currently not used?
        """
        today = test_x[self.config['cols']['date']].min() #The first day in the features
        next_days = [today + datetime.timedelta(days=delta) for delta in range(0,self.config['implementation']['prediction_horizon'][0])]

        if  self.modeltype_schedule=='StaticModel':
            self.schedule_model = ScheduleModel(self.config, self.modeltype_schedule, self.parameters_schedule, waste_past, train_x, train_y) #For simpler models, can ignore train_x and train_y?
            collection_matrix, collection_vector = self.schedule_model.compute_schedule(None, next_days)
        else:
            self.waste_model = WasteModel(self.modeltype_waste, self.parameters_waste, self.config, train_x, train_y) #Includes gen_model?
            waste_matrix, waste_vector, y = self.waste_model.predict(test_x)
            self.schedule_model = ScheduleModel(self.config, self.modeltype_schedule, self.parameters_schedule, train_y, train_x, train_y) #For simpler models, can ignore train_x and train_y?
            #Use train_y for waste_past
            collection_matrix, collection_vector = self.schedule_model.compute_schedule(waste_matrix, next_days)
        return collection_matrix, waste_matrix, collection_vector, waste_vector


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
        self.v_response = self.config['Xy']['response']['variable']
        if self.v_response in self.config['Xy']['lagged']:
            self.v_lag = self.config['Xy']['lagged'][self.v_response]['rows']
        else:
            self.v_lag = []



    def predict(self, test_x):
        """
        Assume test_x (and test_y) are ordered by [date, toiletname]
        """

        # will predict weight accumulated in the coming 7 days
        today = test_x[self.config['cols']['date']].min() #The first day in the features
        next_days = [today + datetime.timedelta(days=delta) for delta in range(0,self.config['implementation']['prediction_horizon'][0])]
        features = test_x.loc[ [d in next_days for  d in test_x[self.config['cols']['date']] ]]#.drop([self.config['cols']['toiletname'], self.config['cols']['date']], axis=1)
        self.waste_vector = features[[self.config['cols']['toiletname'], self.config['cols']['date']]]

        result_y = []
        for d in next_days:
            #update the results table
            ftr_pred = (features.loc[features[self.config['cols']['date']]==d]).drop([self.config['cols']['toiletname'], self.config['cols']['date']], axis=1)
            result_onedayahead = list(self.trained_model.predict( ftr_pred))
            result_y = result_y + result_onedayahead
            #update the features table
            d_next = d + datetime.timedelta(days=1)
            if (len(self.v_lag) > 0) & (d < max(next_days)): #Last day -> need not shift
                features = self.shift(features, d_next, result_onedayahead)

        self.waste_matrix = self.form_the_waste_matrix(features[[self.config['cols']['toiletname'], self.config['cols']['date']]], result_y, self.config['implementation']['prediction_horizon'][0] )
        self.waste_vector['response'] = result_y #This declares a warning, but should be fine...
        return self.waste_matrix, self.waste_vector, result_y

    def shift(self, features, day, y_new):
        features_shifted = features.copy()
        #print(str(features.shape) + "-" + str(len(y_new)) )
        for lag in reversed(self.v_lag[1:]):
            var_replaced = self.v_response + '_lag' + str(lag)
            var_replace = self.v_response + '_lag' + str(lag - 1)
            features_shifted.loc[features_shifted[self.config['cols']['date']]==day, var_replaced ] = features_shifted.loc[features_shifted[self.config['cols']['date']]==day, var_replace ]
        v_lag_latest = self.v_response + '_lag' + str(1)
        features_shifted.loc[features_shifted[self.config['cols']['date']]==day, v_lag_latest ] = y_new
        return(features_shifted)

    def form_the_waste_matrix(self, indices, y, horizon, merge_y = False):
        """
        Args:
          indices (DataFrame): Includes the date and toilet_id
          y (vector?): a vector of predictions, each index correpsonding to the toilet and date from indices
          horizon (int): how long in the future should the  waste_matrix go?
          merge_y (bool): If True, y is a dataframe with the vector of interest (in the third column, the first two columns being toilet_id and date),
            which needs to be merged into indices based on toilets and dates.
        """
        #Append y to indices
        waste_matrix = indices.copy()
        if merge_y:
            y = y[[self.config['cols']['date'], self.config['cols']['toiletname'], 'response']]
            waste_matrix = pd.merge(waste_matrix, y, on = [self.config['cols']['date'], self.config['cols']['toiletname']])
        else:
            waste_matrix['response'] = y

        #Take the next prediction horizon days, extract them from features.
        today = indices[self.config['cols']['date']].min() #The first day in the features
        #7 (or horizon) days from today
        next_days = [today + datetime.timedelta(days=delta) for delta in range(0,horizon)]

        #Sort the indices and only include the ones within the following days
        waste_matrix.sort_values(by=self.config['cols']['date'], inplace=True)
        waste_matrix = waste_matrix[waste_matrix[self.config['cols']['date']].isin(next_days)]
        #Pivot the waste matrix
        waste_matrix = waste_matrix.pivot(index = self.config['cols']['toiletname'], columns = self.config['cols']['date'],values='response')

        return waste_matrix

    def gen_model(self, train_x, train_y):
        log.info("Training {0} with {1}".format(self.modeltype, self.parameters))
        self.trained_model = self.define_model()
        #Strip the index parameters (e.g., toilet id and day) from the train data
        labels=train_y['response'].fillna(0).values
        #For features, assume they have already been subsetted, use everything except toilet_id, day...
        features = train_x.drop([self.config['cols']['toiletname'], self.config['cols']['date']], axis=1)

        #fit the model...
        self.trained_model.fit(features, labels)
        return self.trained_model

    def define_model(self):
        if self.modeltype == "AR" :
            return statsmodels.tsa.ar_model.AR(max_order=self.parameters['max_order'])
        elif self.modeltype == "RandomForest" :
            return ensemble.RandomForestRegressor(n_estimators=self.parameters['n_estimators'])
            #return ensemble.RandomForestClassifier(
            #    n_estimators=self.parameters['n_estimators'])
        elif self.modeltype == "LinearRegression" :
            return linear_model.LinearRegression()
        elif self.modeltype == "Lasso" :
            return linear_model.Lasso(
            alpha=self.parameters['alpha'])
        elif self.modeltype == "ElasticNet" :
            return linear_model.ElasticNet(
            alpha=self.parameters['alpha'],
            l1_ratio=self.parameters['l1_ratio'])
        elif self.modeltype == "SVR" :
            return SVR(
            C=self.parameters['C'],
            epsilon=self.parameters['epsilon'],
            kernel=self.parameters['kernel'])
        elif self.modeltype == 'SGDClassifier' :
            return linear_model.SGDClassifier(
            loss=self.parameters['loss'],
            penalty=self.parameters['penalty'],
            epsilon=self.parameters['epsilon'],
            l1_ratio=self.parameters['l1_ratio'])
        else:
            raise ConfigError("Unsupported model {0}".format(self.modeltype))

class ScheduleModel(object):
    TOILET_CAPACITY = 100 # 100% full
    MAXIMAL_COLLECTION_INTERVAL = 3 #TODO: Get this into yaml.
    """
    Based on the waste matrix, create the collection schedule. The same format as the waste matrix, but values are 0/1 (skip/collect)
    """
    def __init__(self, config, modeltype='simple', parameters=None, waste_past = None, train_x=None, train_y=None):
        self.config = config
        self.modeltype = modeltype
        self.parameters = parameters  #needs a field "threshold" for the StaticModel
        self.waste_past = waste_past
        self.train_x = train_x
        self.train_y = train_y
        #self.test_x = test_x

    def simple_waste_collector(self, waste_row, remaining_threshold = 0, waste_today=0) :
        """
        An iterator that simulates the simple waste collection process
        """
        fill_threshold = self.TOILET_CAPACITY - remaining_threshold
        total_waste = waste_today
        last_collected = 0
        i_collected = 0
        for new_waste in waste_row:
            collect = 0
            i_collected += 1
            total_waste += new_waste
            if (total_waste > fill_threshold) or ((i_collected - last_collected) >= self.MAXIMAL_COLLECTION_INTERVAL):
                collect = 1
                total_waste = 0
                last_collected = i_collected
            yield collect, total_waste

    def compute_schedule(self, waste_matrix = None, remaining_threshold=0, next_days = None):
        """
        Based on the waste predictions, compute the optimal schedule for the next week.

        simple:
          Per toilet, predict accumulated waste. Then whenever the waste exceeds toilet capacity, collect the toilet.

        Args:
          waste_today (DataFrame):

        Returns:
          collection_schedule (DataFrame): The same format as the waste_matrix
        """
        #Same dimensions as the waste_matrix
        if next_days is None:
             next_days = waste_matrix.columns
        yesterday = next_days.min() + datetime.timedelta(days=-1)
        #A dict of toilet -> waste
        if self.waste_past is not None:
            waste_today = { toilet[self.config['cols']['toiletname']]:toilet['response'] for i_toilet, toilet in  self.waste_past.iterrows() if toilet[self.config['cols']['date']]==yesterday}
        else:
            waste_today = {}

        if self.modeltype=='StaticModel':
            #TODO: Afraid this indexing will not work :-(
            collection_schedule = pd.DataFrame(0,index=self.train_x[self.config['cols']['toiletname']].unique(), columns=pd.DatetimeIndex(next_days))
        else:
            collection_schedule = pd.DataFrame(index=waste_matrix.index, columns=pd.DatetimeIndex(next_days))
        if self.modeltype == 'simple':
            for i_toilet, toilet in waste_matrix.iterrows():
                toilet_accums = [collect for collect, waste in self.simple_waste_collector(toilet,remaining_threshold, waste_today.get(i_toilet,0)) ]
                collection_schedule.loc[i_toilet] = toilet_accums
                #collection_schedule.append(pd.DataFrame(toilet_accums, index = i_toilet), ignore_index=True)
        elif self.modeltype == 'StaticModel':
                group_ID=self.train_y.groupby(self.config['cols']['toiletname'])
                group_mean=group_ID.mean()
                group_std=group_ID.agg(np.std, ddof=0)
                group_low=group_mean.loc[(group_mean['response']<=self.parameters.thresholds['meanlow']) & (group_std['response']<=self.parameters.thresholds['stdlow'])];
                ToiletID_LOW=list(set(group_low.index))
                group_medium=group_mean.loc[(group_mean['response']>self.parameters.thresholds['meanlow']) & (group_mean['response']<=self.parameters.thresholds['meanmed']) & (group_std['response']<=self.parameters.thresholds['stdmed'])];
                ToiletID_MEDIUM=list(set(group_medium.index))
                for i_toilet in self.train_x[self.config['cols']['toiletname']].unique():
                    if i_toilet in ToiletID_LOW:
                        toilet_accums=[1, 0, 0, 1, 0, 0, 1]
                    elif i_toilet in ToiletID_MEDIUM:
                        toilet_accums=[1, 0, 1, 0, 1, 0, 1]
                    else:
                        toilet_accums=[1, 1, 1, 1, 1, 1, 1]
                    collection_schedule.loc[i_toilet] = toilet_accums
        elif self.modeltype == 'AdvancedStaticModel':
            #Not working yet?
            ToiletID_LOW = ToiletID_MEDIUM = ToiletID_HIGH = self.train_y.groupby(self.config['cols']['toiletname']).unique()
            keep_going=True
            i=0
            while (keep_going==True):
                day_start=pd.to_datetime(self.train_y[self.config['cols']['date']].min()+timedelta(days=i*7))
                day_end=day_start+timedelta(days=6)
                print (day_start)
                i=i+1
                if  (day_end>pd.to_datetime(self.train_y[self.config['cols']['date']].max())):
                    keep_going=False
                    break
                    #one week in the traing data
                    ToiletCollectionData_train=tmp.loc[((tmp['Collection_Date']>=day_start) & (tmp['Collection_Date']<=day_end))]
                    # group the collections data by Toilet ID
                    group_ID=ToiletCollectionData_train.groupby(['ToiletID'])
                    #find means for the groups
                    group_mean=group_ID.mean()
                    #find standard deviations for the groups
                    #group_std=group_ID.std()
                    group_std=group_ID.agg(np.std, ddof=0)

                    group_low=group_mean.loc[(group_mean['response']<=self.parameters.thresholds['meanlow']) & (group_std['response']<=self.parameters.thresholds['stdlow'])];
                    ToiletID_LOW=list(set(group_low.index) & set(ToiletID_LOW))

                    group_medium=group_mean.loc[(group_mean['response']>self.parameters.thresholds['meanlow']) & (group_mean['response']<=self.parameters.thresholds['meanmed']) & (group_std['response']<=self.parameters.thresholds['stdmed'])];
                    ToiletID_MEDIUM=list(set(group_medium.index) & set(ToiletID_MEDIUM))

                    for i_toilet in self.train_x[self.config['cols']['toiletname']].unique() :
                        if i_toilet in ToiletID_LOW:
                            toilet_accums=[1, 0, 0, 1, 0, 0, 1]
                        elif i_toilet in ToiletID_MEDIUM:
                            toilet_accums=[1, 0, 1, 0, 1, 0, 1]
                        else:
                            toilet_accums=[1, 1, 1, 1, 1, 1, 1]
                            collection_schedule.loc[i_toilet] = toilet_accums

        else:
            raise ConfigError("Unsupported model {0}".format(self.modeltype))

        collection_matrix_aux = collection_schedule.copy()
        collection_matrix_aux[self.config['cols']['toiletname']] = collection_matrix_aux.index
        collection_vector = pd.melt(collection_matrix_aux,id_vars = self.config['cols']['toiletname'], var_name=self.config['cols']['date'], value_name = 'collect')

        return collection_schedule, collection_vector


def run_models_on_folds(folds, loss_function, db, experiment):
    results = pd.DataFrame({'model id':[], 'model':[], 'fold':[], 'metric':[], 'parameter':[], 'value':[]})#Index by experiment hash
    log = logging.getLogger("Sanergy Collection Optimizer")
    log.debug("Running model {0}".format(experiment.model))
    for i_fold, fold in enumerate(folds):
        #log.debug("Fold {0}: {1}".format(i_fold, fold))
        features_train, labels_train, features_test, labels_test = grab_from_features_and_labels(db, fold, experiment.config)


        # 5. Run the models
        model = FullModel(experiment.config, experiment.model, parameters_waste = experiment.parameters)
        cm, wm, cv, wv = model.run(features_train, labels_train, features_test) #Not interested in the collection schedule, will recompute with different parameters.
        #L2 evaluation of the waste prediction

        loss = loss_function.evaluate_waste(labels_test, wv)
        results_fold = generate_result_row(experiment, i_fold, 'MSE', loss)
        #proportion collected and proportion overflow
        for safety_remainder in range(0, 100, 1):
           #Compute the collection schedule assuing the given safety_remainder
           schedule, cv = model.schedule_model.compute_schedule(wm, safety_remainder)

           true_waste = model.waste_model.form_the_waste_matrix(features_test, labels_test, experiment.config['implementation']['prediction_horizon'][0], merge_y=True)#Compute the actual waste produced based on labels_test
           p_collect = loss_function.compute_p_collect(cv)
           p_overflow, p_overflow_conservative, _, _, _ =  loss_function.compute_p_overflow(schedule, true_waste)
           #print(true_waste.head(10))
           #print(model.waste_model.waste_matrix.head(10))
           #print(cv.head(10))
           #print(schedule.head(10))
           print(p_collect)
           print(p_overflow)
           print(p_overflow_conservative)
           #print("--------")
           res_collect = generate_result_row(experiment, i_fold, 'p_collect', p_collect, parameter = float(safety_remainder))
           res_overflow = generate_result_row(experiment, i_fold, 'p_overflow', p_overflow, parameter = float(safety_remainder))
           res_overflow_conservative = generate_result_row(experiment, i_fold, 'p_overflow_conservative', p_overflow_conservative, parameter = float(safety_remainder))
           results_fold = results_fold.append(res_collect, ignore_index=True)
           results_fold = results_fold.append(res_overflow, ignore_index=True)
           results_fold = results_fold.append(res_overflow_conservative, ignore_index=True)

        """
        TODO:
        7. We have to save the model results and the evaluation in postgres
        Experiment x Fold, long file
        """
        #print(results_fold)
        write_evaluation_into_db(results_fold, db)
        results = results.append(results_fold,ignore_index=True)


    #write_evaluation_into_db(results, append = False)
    return(results)

def generate_result_row(experiment, fold, metric, value, parameter=np.nan):
    """
    Just a wrapper
    """
    #result_row = pd.DataFrame({'model id':[hash(experiment)], 'model':[experiment.model], 'fold':[fold], 'metric':[metric], 'parameter':[parameter], 'value':[value]})#, index = [hash( (experiment.model, fold, metric,parameter) )] )
    result_row = pd.DataFrame({'model':[experiment.model], 'fold':[fold], 'metric':[metric], 'parameter':[parameter], 'value':[value]})#, index = [hash( (experiment.model, fold, metric,parameter) )] )
    return result_row

def write_evaluation_into_db(results, db , append = True, chunksize=1000):
    #if ~append :
    #    db['connection'].execute('DROP TABLE IF EXISTS output."evaluations"')
    results.to_sql(name='evaluations',
    schema="output",
    con=db['connection'],
    chunksize=chunksize,
    if_exists='append')

    return None

def run_best_model_on_all_data(experiment, db, folds):
    """
    TODO: Rethink this. Perhaps we can just draw a pickle file or something, need not evaluate the model from scratch.
    Run the model based on the passed experiment, predict on the test (future) data, and present the results.
    Write everything to the db.

    Args:
    test_features (array): Should include Day and Toilet
    """
    #Create a "master" fold: including all training and testing data.
    master_fold = create_enveloping_fold(folds)
    #Extract all features and labels
    features_all_big,labels_all_big,_,_=grab_from_features_and_labels(db, master_fold)
    features_all,labels_all=format_features_labels(features_all_big,labels_all_big)
    #Create a dataset for future prediction (?)... will need to do this differently... (?)
    features_future_big = create_future(master_fold, features_all_big, experiment.parameters) #TODO: This needs fixing
    features_future=format_features_labels(features_future_big,labels_all_big)[0]

    best_model = FullModel(experiment.model,experiment.parameters)
    #Results are the predicted probabilities that a toilet will overflow? This is probably an array
    yhat = best_model.run(features_all, labels_all, features_future)[0]

    #TODO: Need to transform yhat (a probability?) into 1/0 for collect vs not collect. Probably should happen within the Model class?
    output_schedule = present_schedule(yhat, features_future_big, experiment.config)
    #output_waste = ...

    #Workforce scheduling
    staffing = Staffing(output_schedule, None, None, experiment.config)
    output_roster = staffing.staff()[0]


    # Write the results to postgres
    db['connection'].execute('DROP TABLE IF EXISTS output."predicted_filled"')
    pd.DataFrame(yhat).to_sql(name='predicted_filled',
    schema="output",
    con=db['connection'],
    chunksize=1000)

    db['connection'].execute('DROP TABLE IF EXISTS output."collection_schedule"')
    pd.DataFrame(output_schedule).to_sql(name='collection_schedule',
    schema="output",
    con=db['connection'],
    chunksize=1000)

    #If we created the output roster, save it into the db too
    if output_roster:
        db['connection'].execute('DROP TABLE IF EXISTS output."workforce_schedule"')
        pd.DataFrame(output_roster).to_sql(name='workforce_schedule',
        schema="output",
        con=db['connection'],
        chunksize=1000)


    return(best_model)
