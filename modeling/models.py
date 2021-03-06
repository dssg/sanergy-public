#!/usr/bin/env python
import logging
import pdb
import numpy as np
import pandas as pd
import datetime
import sys
import json
import pickle
import pdb

from sklearn import svm, ensemble, tree, linear_model, neighbors, naive_bayes
from sklearn.svm import SVR
from sklearn.feature_selection import SelectKBest
import statsmodels
from datetime import  timedelta

from sanergy.modeling.dataset import grab_from_features_and_labels, format_features_labels
from sanergy.modeling.Staffing import Staffing

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

    def __init__(self, config, modeltype_waste, modeltype_schedule="simple", parameters_waste=None, parameters_schedule=None,
    toilet_routes=None):
        """
        Args:
        """
        self.parameters_waste=parameters_waste
        self.parameters_schedule=parameters_schedule
        self.modeltype_waste=modeltype_waste
        self.modeltype_schedule=modeltype_schedule
        self.toilet_routes = toilet_routes
        self.config = config


    def run(self, train_x, train_yf, train_yu, test_x, waste_past = None, remaining_threshold=50.0):
        """
        Args:
          waste_past: A past waste matrix. Currently not used?
        """
        today = test_x[self.config['cols']['date']].min() #The first day in the features
        next_days = [today + datetime.timedelta(days=delta) for delta in range(0,self.config['implementation']['prediction_horizon'][0])]

        if  self.modeltype_waste=='StaticModel':
            self.modeltype_schedule='StaticModel'
            self.schedule_model = ScheduleModel(config=self.config, modeltype=self.modeltype_schedule, parameters=self.parameters_schedule, waste_past_feces=None, waste_past_urine=None, train_x= train_x, train_yf=train_yf) #For simpler models, can ignore train_x and train_y?
            collection_matrix, collection_vector = self.schedule_model.compute_schedule(waste_matrix_feces = None, waste_matrix_urine=None, remaining_threshold_feces=0.0, remaining_threshold_urine=0.0, next_days =next_days)
            waste_matrix_feces = waste_matrix_urine = roster = waste_vector_urine = waste_vector_feces = importances = None
            
        else:
            self.feces_model = WasteModel(self.modeltype_waste, self.parameters_waste, self.config, train_x, train_yf, waste_type = 'feces') #Includes gen_model?
            self.urine_model = WasteModel(self.modeltype_waste, self.parameters_waste, self.config, train_x, train_yu, waste_type = 'urine') #Includes gen_model?
            waste_matrix_feces, waste_vector_feces, yf = self.feces_model.predict(test_x)
            waste_matrix_urine, waste_vector_urine, yu = self.urine_model.predict(test_x)
            self.schedule_model = ScheduleModel(self.config, self.modeltype_schedule, self.parameters_schedule, train_yf, train_yu, train_x, train_yf, train_yu) #For simpler models, can ignore train_x and train_y?
            #Use train_y for waste_past
            collection_matrix, collection_vector = self.schedule_model.compute_schedule(waste_matrix_feces, waste_matrix_urine, remaining_threshold, remaining_threshold , next_days)
            importances, coefs = self.get_feature_importances()

            if self.config['staffing']['active']:
                #Compute the staffing schedule for the next week
                self.staffing_model = Staffing(collection_matrix, waste_matrix_feces, waste_matrix_urine, self.toilet_routes, self.config['staffing'], self.config)
                roster, _, _ = self.staffing_model.staff()
            else:
                roster = None


        return collection_matrix, waste_matrix_feces, waste_matrix_urine, roster, collection_vector, waste_vector_feces, waste_vector_urine, importances


    def get_feature_importances(self, modeltype="feces"):
        """
        Get feature importances (from scikit-learn) of trained model.
        Args:
        model: Trained model
        Returns:
        Feature importances, or failing that, None
        """
        if modeltype == "feces":
            model = self.feces_model.trained_model
        else:
            model = self.urine_model.trained_model
        try:
            importances = model.feature_importances_
        except:
            importances = np.zeros(1)
        try:
            coefs = model.coef_
        except:
            coefs = np.zeros(1)
        return importances, coefs




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
    def __init__(self, modeltype, parameters, config, train_x = None, train_y = None, waste_type='feces'):
        """

        Args:
        waste_type: 'feces' or 'urine'
        """
        self.parameters = parameters
        self.modeltype = modeltype
        self.config = config
	self.timestamp = datetime.datetime.now().isoformat()
        if (train_x is not None) and (train_y is not None):
            self.gen_model(train_x, train_y)
        self.v_response = self.config['cols'][waste_type]
        if self.v_response in self.config['Xy']['lagged']:
            self.v_lag = self.config['Xy']['lagged'][self.v_response]['rows']
            #For now, just extrapolate the response variable, keep the other dynamic variables fixed at the first day.
        else:
            self.v_lag = []



    def predict(self, test_x):
        """
        Assume test_x (and test_y) are ordered by [date, toiletname]
        """

        # will predict weight accumulated in the coming 7 days
        today = test_x[self.config['cols']['date']].min() #The first day in the features
        future_days = [today + datetime.timedelta(days=delta) for delta in range(0,self.config['implementation']['prediction_horizon'][0])]
        features = test_x.loc[ [d in future_days for  d in test_x[self.config['cols']['date']] ]]#.drop([self.config['cols']['toiletname'], self.config['cols']['date']], axis=1)
        self.waste_vector = features[[self.config['cols']['toiletname'], self.config['cols']['date']]]

        result_y = []
        for d in future_days:
            #update the results table
            ftr_pred = (features.loc[features[self.config['cols']['date']]==d]).drop([self.config['cols']['toiletname'], self.config['cols']['date']], axis=1)
            result_onedayahead = list(self.trained_model.predict( ftr_pred))
            result_y = result_y + result_onedayahead
            #update the features table
            d_next = d + datetime.timedelta(days=1)
            if (len(self.v_lag) > 0) & (d < max(future_days)): #Last day -> need not shift
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
        future_days = [today + datetime.timedelta(days=delta) for delta in range(0,horizon)]

        #Sort the indices and only include the ones within the following days
        waste_matrix.sort_values(by=self.config['cols']['date'], inplace=True)
        waste_matrix = waste_matrix[waste_matrix[self.config['cols']['date']].isin(future_days)]
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

        #fit the model..
	self.time_started = datetime.datetime.now().isoformat()
        self.feature_names = features.columns
        self.trained_model.fit(features, labels)
	self.time_ended = datetime.datetime.now().isoformat()
        return self.trained_model

    def define_model(self):
        #if self.modeltype == "AR" :
        #    return statsmodels.tsa.ar_model.AR(max_order=self.parameters['max_order'])
        if self.modeltype == "RandomForest" :
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
        #elif self.modeltype == 'StaticModel':
        #   return StaticModel (
        #      parameters=self.parameters
        #     )
        #elif self.modeltype == 'AdvancedStaticModel':
        #   return AdvancedStaticModel (
        #       parameters=self.parameters
        #        )

        # elif self.modeltype == 'SGDRegressor' :
        #     print(self.parameters)
        #     return linear_model.SGDRegressor(
        #     loss=self.parameters['loss'],
        #     penalty=self.parameters['penalty'],
        #     l1_ratio=self.parameters['l1_ratio'])
        else:
            raise ConfigError("Unsupported model {0}".format(self.modeltype))

class ScheduleModel(object):
    TOILET_CAPACITY = 100.0 # 100% full
    MAXIMAL_COLLECTION_INTERVAL = 3 #TODO: Get this into yaml.
    """
    Based on the waste matrix, create the collection schedule. The same format as the waste matrix, but values are 0/1 (skip/collect)
    """
    def __init__(self, config, modeltype='simple', parameters=None, waste_past_feces = None, waste_past_urine=None, train_x=None, train_yf=None, train_yu=None):
        self.config = config
        self.modeltype = modeltype
        self.parameters = parameters  #needs a field "threshold" for the StaticModel
        self.waste_past_feces = waste_past_feces
        self.waste_past_urine = waste_past_urine
        self.train_x = train_x
        self.train_yf = train_yf
        self.train_yu = train_yu
        #self.test_x = test_x

    def simple_waste_collector(self, feces_row, urine_row, remaining_threshold_feces = 0.0, remaining_threshold_urine = 0.0, feces_today=0.0, urine_today=0.0) :
        """
        An iterator that simulates the simple waste collection process
        """
        fill_threshold_feces = self.TOILET_CAPACITY - remaining_threshold_feces
        fill_threshold_urine = self.TOILET_CAPACITY - remaining_threshold_urine
        total_waste_feces = feces_today
        total_waste_urine = urine_today
        last_collected = 0
        i_collected = 0
        for i, new_feces in enumerate(feces_row):
            new_urine = urine_row[i]
            collect = 0
            i_collected += 1
            total_waste_feces += new_feces
            total_waste_urine += new_urine
            if (total_waste_feces > fill_threshold_feces) or (total_waste_urine > fill_threshold_urine) or ((i_collected - last_collected) >= self.MAXIMAL_COLLECTION_INTERVAL):
                collect = 1
                total_waste_feces = 0
                total_waste_urine = 0
                last_collected = i_collected
            yield collect, total_waste_feces, total_waste_urine

    def compute_schedule(self, waste_matrix_feces = None, waste_matrix_urine=None, remaining_threshold_feces=0.0, remaining_threshold_urine=0.0, next_days = None):
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
             next_days = waste_matrix_feces.columns
        yesterday = min(next_days) + datetime.timedelta(days=-1)
        #A dict of toilet -> waste
        if self.waste_past_feces is not None:
            waste_feces_today = { toilet[self.config['cols']['toiletname']]:toilet['response'] for i_toilet, toilet in  self.waste_past_feces.iterrows() if toilet[self.config['cols']['date']]==yesterday}
        else:
            waste_feces_today = {}
        if self.waste_past_urine is not None:
            waste_urine_today = { toilet[self.config['cols']['toiletname']]:toilet['response'] for i_toilet, toilet in  self.waste_past_urine.iterrows() if toilet[self.config['cols']['date']]==yesterday}
        else:
            waste_urine_today = {}

        if (self.modeltype=='StaticModel') or (self.modeltype=='AdvancedStaticModel'):
            print(self.config['cols']['toiletname'])
            #TODO: Afraid this indexing will not work :-(
            #collection_schedule = pd.DataFrame(0,index=self.train_x['Id'].unique(), columns=pd.DatetimeIndex(next_days))
            collection_schedule = pd.DataFrame(0,index=self.train_yf[self.config['cols']['toiletname']].unique(), columns=pd.DatetimeIndex(next_days))
        else:
            collection_schedule = pd.DataFrame(index=waste_matrix_feces.index, columns=pd.DatetimeIndex(next_days))
        if self.modeltype == 'simple':
            for i_toilet, toilet in waste_matrix_feces.iterrows():
                toilet_accums = [collect for collect, feces, urine in self.simple_waste_collector(toilet, waste_matrix_urine.loc[i_toilet], remaining_threshold_feces, remaining_threshold_urine, waste_feces_today.get(i_toilet,0), waste_urine_today.get(i_toilet,0)) ]
                collection_schedule.loc[i_toilet] = toilet_accums
               #collection_schedule.append(pd.DataFrame(toilet_accums, index = i_toilet), ignore_index=True)
        elif self.modeltype == 'StaticModel':
            #pdb.set_trace()
            group_ID=self.train_yf.groupby(self.config['cols']['toiletname'])
            #group_ID=self.train_x.groupby('Id')
            group_mean=group_ID.mean()
            group_std=group_ID.agg(np.std, ddof=0)
            pdb.set_trace()
            group_low=group_mean.loc[(group_mean['response']<=self.parameters['meanlow'][0]) & (group_std['response']<=self.parameters['stdlow'][0])];
            ToiletID_LOW=list(set(group_low.index))
            group_medium=group_mean.loc[(group_mean['response']>self.parameters['meanlow'][0]) & (group_mean['response']<=self.parameters['meanmed'][0]) & (group_std['response']<=self.parameters['stdmed'][0])];
            ToiletID_MEDIUM=list(set(group_medium.index))
            for i_toilet in self.train_yf[self.config['cols']['toiletname']].unique():
                if i_toilet in ToiletID_LOW:
                    toilet_accums=[1, 0, 0, 1, 0, 0, 1]
                elif i_toilet in ToiletID_MEDIUM:
                    toilet_accums=[1, 0, 1, 0, 1, 0, 1]
                else:
                    toilet_accums=[1, 1, 1, 1, 1, 1, 1]
                collection_schedule.loc[i_toilet] = toilet_accums
        elif self.modeltype == 'AdvancedStaticModel':
            #pdb.set_trace()
            ToiletID_LOW = ToiletID_MEDIUM = ToiletID_HIGH = self.train_yf[self.config['cols']['toiletname']].unique()
            keep_going=True
            i=0
            while (keep_going==True):
                day_start=pd.to_datetime(self.train_y[self.config['cols']['date']].min()+timedelta(days=i*7))
                day_end=day_start+timedelta(days=6)
                print (day_start)
                i=i+1
                #one week in the traing data
                train_tmp=self.train_yf.loc[((self.train_y[self.config['cols']['date']]>=day_start) & (self.train_y[self.config['cols']['date']]<=day_end))]
                # group the collections data by Toilet ID
                group_ID=train_tmp.groupby(self.config['cols']['toiletname'])
                #find means for the groups
                group_mean=group_ID.mean()
                #find standard deviations for the groups
                #group_std=group_ID.std()
                group_std=group_ID.agg(np.std, ddof=0)
                group_low=group_mean.loc[(group_mean['response']<=self.parameters['meanlow']) & (group_std['response']<=self.parameters['stdlow'])];
                ToiletID_LOW=list(set(group_low.index) & set(ToiletID_LOW))

                group_medium=group_mean.loc[(group_mean['response']>self.parameters['meanlow']) & (group_mean['response']<=self.parameters['meanmed']) & (group_std['response']<=self.parameters['stdmed'])];
                ToiletID_MEDIUM=list(set(group_medium.index) & set(ToiletID_MEDIUM))

                if  (day_end>pd.to_datetime(self.train_yf[self.config['cols']['date']].max())):
                    keep_going=False
                    break
            for i_toilet in self.train_yf[self.config['cols']['toiletname']].unique():
                if i_toilet in ToiletID_LOW:
                    toilet_accums=[1, 0, 0, 1, 0, 0, 1]
                elif i_toilet in ToiletID_MEDIUM:
                    toilet_accums=[1, 0, 1, 0, 1, 0, 1]
                else:
                    toilet_accums=[1, 1, 1, 1, 1, 1, 1]
                collection_schedule.loc[i_toilet] = toilet_accums
                #pdb.set_trace()

        else:
            raise ConfigError("Unsupported model {0}".format(self.modeltype))
        collection_matrix_aux = collection_schedule.copy()
        collection_matrix_aux[self.config['cols']['toiletname']] = collection_matrix_aux.index
        collection_vector = pd.melt(collection_matrix_aux,id_vars = self.config['cols']['toiletname'], var_name=self.config['cols']['date'], value_name = 'collect')
	self.collection_vector = collection_vector

        return collection_schedule, collection_vector


def run_models_on_folds(folds, loss_function, db, experiment):
    results = pd.DataFrame({'model id':[], 'model':[], 'fold':[], 'metric':[], 'parameter':[], 'value':[]})#Index by experiment hash
    log = logging.getLogger("Sanergy Collection Optimizer")
    log.debug("Running model {0}".format(experiment.model))
    for i_fold, fold in enumerate(folds):
        #log.debug("Fold {0}: {1}".format(i_fold, fold))
        features_train, labels_train_f, labels_train_u, features_test, labels_test_f, labels_test_u,  toilet_routes = grab_from_features_and_labels(db, fold, experiment.config)


        # 5. Run the models
        #pdb.set_trace()
        model = FullModel(experiment.config, experiment.model, parameters_waste = experiment.parameters, parameters_schedule=experiment.config['parameters']['StaticModel']['parameters'], toilet_routes = toilet_routes)
        cm, wmf, wmu, roster, cv, wvf, wvu, fi = model.run(features_train, labels_train_f, labels_train_u, features_test) #Not interested in the collection schedule, will recompute with different parameters.
        #L2 evaluation of the waste prediction
        roster.to_csv("%s\workforce_schedule.csv" %(experiment.config['pickle_store']))

        loss_f = loss_function.evaluate_waste(labels_test_f, wvf)
        loss_u = loss_function.evaluate_waste(labels_test_u, wvu)
        generate_result_row(db, experiment, model.feces_model, i_fold, 'MSE_feces', loss_f)
        generate_result_row(db, experiment, model.urine_model, i_fold, 'MSE_urine', loss_u)
        remainder_range = list(reversed(experiment.config['setup']['collection_remainder_threshold']))
        if len(remainder_range) == 0:
            remainder_range = range(0, 100, 1)
        #proportion collected and proportion overflow
        for safety_remainder in remainder_range:
           #Compute the collection schedule assuing the given safety_remainder
           schedule, cv = model.schedule_model.compute_schedule(wmf, wmu, safety_remainder)

           true_waste_f = model.feces_model.form_the_waste_matrix(features_test, labels_test_f, experiment.config['implementation']['prediction_horizon'][0], merge_y=True)#Compute the actual waste produced based on labels_test
           true_waste_u = model.urine_model.form_the_waste_matrix(features_test, labels_test_u, experiment.config['implementation']['prediction_horizon'][0], merge_y=True)#Compute the actual waste produced based on labels_test
           p_collect = loss_function.compute_p_collect(cv)
           p_overflow, p_overflow_conservative, p_overflow_f, p_overflow_f_conservative, p_overflow_u, p_overflow_u_conservative, _, _, _ =  loss_function.compute_p_overflow(schedule, true_waste_f, true_waste_u)
           #print(true_waste.head(10))
           #print(model.waste_model.waste_matrix.head(10))
           #print(cv.head(10))
           #print(schedule.head(10))
           print(p_collect)
           print(p_overflow)
           print(p_overflow_conservative)
           #print("--------")
           generate_result_row(db, experiment, model.feces_model, i_fold, 'p_collect', p_collect, parameter = float(safety_remainder))
           generate_result_row(db, experiment, model.feces_model, i_fold, 'p_overflow', p_overflow, parameter = float(safety_remainder))
           generate_result_row(db, experiment, model.feces_model, i_fold, 'p_overflow_conservative', p_overflow_conservative, parameter = float(safety_remainder))
           generate_result_row(db, experiment, model.feces_model, i_fold, 'p_overflow_feces', p_overflow_f, parameter = float(safety_remainder))
           generate_result_row(db, experiment, model.urine_model, i_fold, 'p_overflow_urine', p_overflow_u, parameter = float(safety_remainder))
           generate_result_row(db, experiment, model.urine_model, i_fold, 'p_overflow_urine_conservative', p_overflow_u_conservative, parameter = float(safety_remainder))
           generate_result_row(db, experiment, model.feces_model, i_fold, 'p_overflow_feces_conservative', p_overflow_f_conservative, parameter = float(safety_remainder))

           exp_results = pd.DataFrame(model.schedule_model.collection_vector)
	   exp_results["model_id"] = hash(experiment)
           exp_results["waste_type"]="Combined"
	   exp_results["fold_id"]=i_fold
	   exp_results["comment"]="Lauren will reach inbox 0!"  
           exp_results.to_sql(con=db['connection'], name="predictions", schema="output", if_exists="append", index=False)

           #results_fold = results_fold.append([res_collect,res_overflow,res_overflow_conservative, res_overflow_f, res_overflow_f_conservative, res_overflow_u, res_overflow_u_conservative], ignore_index=True)

        """
        7. We have to save the model results and the evaluation in postgres
        Experiment x Fold, long file
        """
        #print(results_fold)
        #write_evaluation_into_db(results_fold, db)
        write_experiment_into_db(experiment, model, db)
        #results = results.append(results_fold,ignore_index=True)

	#write_evaluation_into_db(results, append = False)
    return(results)

def generate_result_row(db, experiment, model, fold, metric, value, parameter=np.nan):
    """
    Just a wrapper
    """
    timestamp = datetime.datetime.now().isoformat()
    exp_results = [{"model_id": hash(experiment),
			"algorithm": experiment.model,
			"hyperparameters": experiment.to_json(),
			"features": "|".join(model.feature_names.tolist()),
			"time_started": model.time_started,
			"time_ended": model.time_ended,
			"batch_id": None,
			"fold_id": fold,
			"comment": "Joe was right."}]  
    pd.DataFrame(exp_results).to_sql(con=db['connection'], name="model", schema="output", if_exists="append", index=False)
    
    exp_results = [{"model_id": hash(experiment),
			"metric": metric,
			"parameter": parameter,
			"fold_id": fold,
			"parameter_value": "Joe was right.",
			"value":value}]  
    pd.DataFrame(exp_results).to_sql(con=db['connection'], name="evaluations", schema="output", if_exists="append", index=False)

    #result_row = pd.DataFrame({'model id':[hash(experiment)], 'model':[experiment.model], 'fold':[fold], 'metric':[metric], 'parameter':[parameter], 'value':[value]})#, index = [hash( (experiment.model, fold, metric,parameter) )] )
    #result_row = pd.DataFrame({'id':[hash(experiment)],'model':[experiment.model], 'model_parameters':[experiment.to_json()], 'fold':[fold], 'metric':[metric], 'parameter':[parameter], 'value':[value]})#, index = [hash( (experiment.model, fold, metric,parameter) )] )
    return None

def write_evaluation_into_db(results, db , append = True, chunksize=1000):
    #if ~append :
    #    db['connection'].execute('DROP TABLE IF EXISTS output."evaluations"')
    results.to_sql(name='evaluations',
    schema="output",
    con=db['connection'],
    chunksize=chunksize,
    if_exists='append')

    return None

def write_experiment_into_db(experiment, model, db , append = True, chunksize=1000):
    timestamp =  model.feces_model.time_started

    #save model to pickle object
    save_model_file = open('%s/feces_model-%s.pkl' %(experiment.config["pickle_store"], timestamp), 'wb')
    pickle.dump(model.feces_model, save_model_file)
    save_model_file.close()
    save_model_file = open('%s/urine_model-%s.pkl' %(experiment.config["pickle_store"], timestamp), 'wb')
    pickle.dump(model.urine_model, save_model_file)
    save_model_file.close()
    save_model_file = open('%s/schedule_model-%s.pkl' %(experiment.config["pickle_store"], timestamp), 'wb')
    pickle.dump(model.schedule_model, save_model_file)
    save_model_file.close()

    #save_model_file = open('./store/staffing_model-%s.pkl' %(timestamp), 'wb')
    #pickle.dump(model.staffing_model, save_model_file)
    #save_model_file.close()

    exp_row = pd.DataFrame({'timestamp':[timestamp], 'id':[hash(experiment)] ,'model':[experiment.model], 'model_parameters':[experiment.to_json()], 'model_config':[json.dumps(experiment.config)],
    'feature_importances':[json.dumps(model.get_feature_importances()[0].tolist())],'feature_names':[json.dumps(model.get_feature_importances()[1].tolist())] })

    exp_row.to_sql(name='experiments',
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
