import unittest
import yaml
import re, pprint
import sqlalchemy
import pandas as pd
import numpy as np
import logging
import sys
from datetime import datetime, date, timedelta
from functools import reduce

from sanergy.premodeling.Experiment import generate_experiments, Experiment
from sanergy.modeling.LossFunction import LossFunction, compare_models_by_loss_functions
from sanergy.modeling.dataset import grab_collections_data, temporal_split, format_features_labels, create_enveloping_fold
import sanergy.input.dbconfig as dbconfig
from sanergy.modeling.models import WasteModel, ScheduleModel, FullModel
from sanergy.modeling.Staffing import Staffing
#from premodeling.Experiment import generate_experiments

class ExperimentTest(unittest.TestCase):
    def setUp(self):
        with open("tests/test.yaml", 'r') as f:
            self.config = yaml.load(f)
    #def test_true(self):
    #    self.assertEqual(True, False)
    #    self.assertEqual(1, 2)
    def test_Yaml(self):
        self.assertEqual(self.config['model'][0],"RandomForest")
        #print(type(self.config['db']))
        self.assertIsInstance(self.config['db'],dict)
        self.assertIsInstance(self.config['parameters'],dict)
        self.assertIsInstance(self.config['parameters']['RandomForest']['n_estimators'],list)

    def test_experiments(self):
         experiments = generate_experiments(self.config)
         #print(experiments[1].parameters)
         self.assertEqual(len(experiments[1].parameters),9)
         self.assertIsInstance(experiments[23].parameters,dict)

    def test_default_yaml(self):
        """
        Test that default.yaml as given is readable and working.
        """
        with open("default.yaml", 'r') as f:
            cfg_default = yaml.load(f)
        experiments = generate_experiments(cfg_default)
        self.assertIsInstance(experiments[0],Experiment)
        self.assertIsInstance(experiments[0].model,str)

    def test_index_by_experiment(self):
        loss_map = {}
        experiments = generate_experiments(self.config)
        exp1 = experiments[1]
        exp2 = experiments[2]
        loss_map[exp1] = [6,5]
        loss_map[exp2] = [4,3]
        exp, loss = compare_models_by_loss_functions(loss_map)
        self.assertEqual(loss,(4.0+3.0)/2)
        self.assertEqual(exp,exp2)
        self.assertNotEqual(exp,exp1)



class datasetTest(unittest.TestCase):
    def setUp(self):
        self.config_cv = {
        'start_date':'2014-01-01',
        'end_date':'2014-05-05',
        'train_on':{'days':0, 'weeks':5},
        'test_on':{'days':0, 'weeks':1}
        }
        d_labels = {'response' : pd.Series([1.18, 1.28], index=[0,1]),
        'Feces_kg_day' : pd.Series([1.18, 1.28], index=[0,1]),
        'ToiletID' : pd.Series(['a08D000000PXgspIAD', 'a08D000000PXgspIAD'], index=[0,1]),
        'Collection_Date': pd.Series([datetime.strptime('2014-10-16','%Y-%m-%d'), datetime.strptime('2014-10-17','%Y-%m-%d')], index=[0,1])
        }
        d_feat = {'Total_Waste_kg_day' : pd.Series([7.13, 7.63], index=[0,1]),
        'ToiletID' : pd.Series(['a08D000000PXgspIAD', 'a08D000000PXgspIAD'], index=[0,1]),
        'Collection_Date': pd.Series([datetime.strptime('2014-10-16','%Y-%m-%d'), datetime.strptime('2014-10-17','%Y-%m-%d')], index=[0,1]),
        'Feces_kg_day_lag1' : pd.Series([0, 1.18], index=[0,1]),
        'Feces_kg_day_lag2' : pd.Series([0, 0], index=[0,1]),
        'Feces_kg_day_lag3' : pd.Series([0, 0], index=[0,1])
        }
        self.features_big = pd.DataFrame(d_feat)
        self.labels_big = pd.DataFrame(d_labels)

    def test_grab_collections_data(self):
         pass
    #     #TODO: Have a test dataset on which we can do a proper unit test.
    #     # Real data are not good for testing because they change as we get new data.
    #     engine = sqlalchemy.create_engine('postgresql+psycopg2://%s:%s@%s:%s' %(dbconfig.config['user'],
    #     dbconfig.config['password'],
    #     dbconfig.config['host'],
    #     dbconfig.config['port']))
    #     try:
    #         conn = engine.connect()
    #     except:
    #         pass
    #
    #     db={'connection':conn,
    #     'table':'toiletcollection',
    #     'database':'premodeling'}
    #     response = {'variable':'Feces_kg_day',
    #     'split':{'and':[('>',3),('<',7)]}}
    #     features = {'Urine_kg_day':{'and':[('<=',10),('>',3)],
    #     'not':('=',5),
    #     'list':['4','7','8','5']}}
    #     unique = {'ToiletID':{'list':['a08D000000i1KgnIAE']},
    #     'Collection_Date':{'and':[('>',"'2012-01-01'"),('<',"'2014-01-01'")]}}
    #     lagged = {'Feces_kg_day':{'function':'lag',
    #     'rows':[1,2,3]},
    #     'FecesContainer_percent':{'function':'lag',
    #     'rows':[1,6,12]}}
    #
    #     y,x = grab_collections_data(db, response, features, unique, lagged)
    #
    #     print('\nThe LABELS (y) dataframe, includes both the RESPONSE variable and the original ("%s")' %(response['variable']))
    #     pprint.pprint(y.keys())
    #     print(y.head())
    #
    #     print('\nThe FEATURES (x) dataframe includes %i variables, %i rows of data (unique identifiers: %s)' %(len(x.keys()), len(x), ','.join(unique.keys()) ))
    #     pprint.pprint(x.keys())
    #     print(x.head())

    def test_temporal_split(self):
        splits=temporal_split(self.config_cv,
        day_of_week=6,
        floating_window=False)
        #Checked by Brian
        self.assertEqual(len(splits),12)

    def test_create_enveloping_fold(self):
        folds=temporal_split(self.config_cv,
        day_of_week=6,
        floating_window=False)
        enveloping_fold = create_enveloping_fold(folds)
        self.assertEqual(enveloping_fold['train_start'], datetime.strptime(self.config_cv['start_date'],'%Y-%m-%d'))
        #print(f1)

    def test_create_future(self):
        enveloping_fold=create_enveloping_fold(temporal_split(self.config_cv,
        day_of_week=6,
        floating_window=False))
        #Take the last day and duplicate it for the relevant number of days
        last_day = enveloping_fold['window_end']
        next_days = [last_day + timedelta(days=i) for i in xrange(1,5)]
        old_features = self.features_big.drop_duplicates(subset='ToiletID')
        l_future_features = []
        for day in  next_days:
            next_day_features = old_features.copy()
            next_day_features["Collection_Date"] = day
            l_future_features.append(next_day_features)
        future_features = pd.concat(l_future_features, ignore_index=True)

        #Now replicate the original features

        #print(next_days)


    def test_format_features_labels(self):
         features, labels = format_features_labels(self.features_big, self.labels_big)
         self.assertIsInstance(features, pd.DataFrame)
         self.assertIsInstance(labels, np.ndarray)


class modelsTest(unittest.TestCase):
    def setUp(self):
        self.horizon = 7
        self.fake_col = np.repeat([1.5],2*self.horizon)
        self.today = datetime(2011,11,11)
        self.unique_dates = [self.today + timedelta(days=delta) for delta in range(0,self.horizon)]
        self.dates = [self.today + timedelta(days=delta) for delta in range(0,self.horizon)] * 2
        self.toilets = ['toilet1'] * self.horizon + ['toilet2'] * self.horizon
        self.config = {
        'Xy':{
        'response':{'variable':'y'},
        'lagged':{}
        },
        'cols':{'toiletname':'ToiletID', 'date':'Collection_Date'},
        'implementation':{'prediction_horizon':[2]}
        }
        self.config2 = {
        'Xy':{
        'response':{'variable':'y'},
        'lagged':{'y':{'rows':[1,2]}}
        },
        'cols':{'toiletname':'ToiletID', 'date':'Collection_Date'},
        'implementation':{'prediction_horizon':[7]}
        }
        self.y = pd.DataFrame.from_dict({'response':range(0,2*self.horizon)})
        self.x = -self.y['response'] + 5.0

        self.z = np.repeat([-1.0,1.0],self.horizon)
        self.df = pd.DataFrame.from_dict({'ToiletID':self.toilets, 'Collection_Date':self.dates, 'w':self.fake_col,
        'x':self.x, 'z' : self.z})
        self.dftest  = pd.DataFrame.from_dict({'ToiletID':['t1','t1','t1','t2','t2','t3','t3'],
         'Collection_Date':[datetime(2012,1,1), datetime(2012,1,2),  datetime(2012,5,2), datetime(2012,1,1), datetime(2012,1,2),datetime(2012,1,1), datetime(2012,1,2)],
          'w':[3,5,8,7,8,9,10],'x':[0,1,1, 2, 3,4,5], 'z' : [-5,3,6,0,0,0,0]})
        self.dftest2  = pd.DataFrame.from_dict({'ToiletID':['t1','t1','t2','t2','t3','t3'],
         'Collection_Date':[datetime(2012,1,1), datetime(2012,1,2),   datetime(2012,1,1), datetime(2012,1,2),datetime(2012,1,1), datetime(2012,1,2)],
          'w':[3,5,7,8,9,10],'x':[0,1,2, 3,4,5], 'z' : [-5,3,6,0,0,0]})
        self.dftest2.sort_values(by=['Collection_Date','ToiletID'],inplace=True)
        self.dftrainx  = pd.DataFrame.from_dict({'ToiletID':['t1','t1','t1','t2','t2','t3','t3'],
         'Collection_Date':[datetime(2012,1,1), datetime(2012,1,2),  datetime(2012,5,2), datetime(2012,1,1), datetime(2012,1,2),datetime(2012,1,1), datetime(2012,1,2)],
          'w':[3,5,8,7,8,9,10],'x':[0,1,1, 2, 3,4,5], 'z' : [-5,3,6,0,0,0,0]})
        self.dftrainy = pd.DataFrame.from_dict({'ToiletID':['t1','t1','t1','t2','t2','t3','t3'],
         'Collection_Date':[datetime(2012,1,1), datetime(2012,1,2),  datetime(2012,5,2), datetime(2012,1,1), datetime(2012,1,2),datetime(2012,1,1), datetime(2012,1,2)],
          'response': [1,1,1, 0,2,1,2]})
        self.wm =WasteModel("LinearRegression",{},self.config)
        self.wm2 =WasteModel("LinearRegression",{},self.config2)
        self.sm =ScheduleModel(self.config, modeltype='StaticModel', parameters={'meanlow':23, 'stdlow':100, 'meanmed':40, 'stdmed':10}, train_x=self.dftrainx, train_y=self.dftrainy)
        self.waste_matrix =  pd.DataFrame.from_items([('t1', [60, 50, 10, 40, 70, 10, 30]), ('t2', [10, 20, 30, 40, 50, 60, 70])],
        orient='index', columns=self.unique_dates)
        self.shift_set = pd.DataFrame.from_dict({'ToiletID':['t1','t2','t1','t2','t1','t2'],
        'Collection_Date':[datetime(2012,1,1),datetime(2012,1,1),datetime(2012,1,2),datetime(2012,1,2),datetime(2012,1,3),datetime(2012,1,3)],
        'y_lag1': range(0,6), 'y_lag2':range(6,12)})


    def test_form_the_waste_matrix(self):
        waste_matrix = self.wm.form_the_waste_matrix(self.df[[0,1]],self.y, self.horizon)
        self.assertIsInstance(waste_matrix, pd.DataFrame)
        self.assertEqual(waste_matrix.loc['toilet1', datetime(2011,11,17)], 6)
        self.assertEqual(waste_matrix.loc['toilet2', datetime(2011,11,15)], 11)
        self.assertEqual(waste_matrix.shape, (2,7))

    def test_WasteModel_run(self):
        self.wm.gen_model(self.df, self.y)
        waste, wv, y = self.wm.predict(self.dftest2)
        self.assertEqual(waste.shape, (3,2))
        self.assertEqual(np.linalg.norm(y  + self.dftest2['x'] - 5.0) < 1.0e-9,True)

    #def test_compute_schedule(self):
    #    schedule, sv = self.sm.compute_schedule(self.waste_matrix, remaining_threshold=0, next_days = None)
    #    self.assertEqual(schedule.loc["t2",datetime(2011,11,13) ], 1 ) #Test that collects after 3 days
    #    self.assertEqual(schedule.loc["t1",datetime(2011,11,12) ], 1 ) #Test that the toilet is collected when full
    #    self.assertEqual(schedule.loc["t2",datetime(2011,11,16) ], 1 ) #Test that the toilet is collected when full
    #    self.assertEqual(schedule.loc["t1",datetime(2011,11,13) ], 0 ) #Test that the toilet is empty after the collection
    #    self.assertEqual(schedule.loc["t2",datetime(2011,11,12) ], 0 ) #Test that the toilet is not collected when not full

    def test_compute_schedule_Static_model(self):
        schedule, sv = self.sm.compute_schedule(waste_matrix = None, remaining_threshold=0, next_days = self.unique_dates)
        #collection_matrix, collection_vector = self.schedule_model.compute_schedule(waste_matrix=None, next_days)

    # def test_Model(self):
    #     model = Model(self.config, "LinearRegression")
    #     cm, sm, cv, sv = model.run(self.df, self.y, self.dftest)
    #     self.assertEqual(cm.shape, (3,2))
    #     self.assertEqual(sm.shape, (3,2))
    #     self.assertEqual(cm.loc['t2',datetime(2012,1,2)], 0)

    def test_shift(self):
        shifted = self.wm2.shift(self.shift_set,datetime(2012,1,2),[42,53])
        self.assertEqual( shifted.loc[(shifted['ToiletID']=="t2") & (shifted['Collection_Date']==datetime(2012,1,2)),'y_lag1'].values, 53)
        self.assertEqual(shifted.loc[(shifted['ToiletID']=="t1") & (shifted['Collection_Date']==datetime(2012,1,2)),'y_lag2'].values,
        self.shift_set.loc[(self.shift_set['ToiletID']=="t1") & (self.shift_set['Collection_Date']==datetime(2012,1,2)),'y_lag1'].values)

class LossFunctionTest(unittest.TestCase):
    def setUp(self):
        self.config = {
            'implementation':{'loss':"L2", 'aggregation_measure':'mean'}
        }
    def test_NewLoss(self):
        lf = LossFunction(self.config)
        self.assertIsInstance(lf,LossFunction)

    def test_L2_loss(self):
        lf = LossFunction(self.config)
        yhat = [1,0,0]
        y = [0,2,0]
        loss = lf.evaluate_waste(yhat,y)
        self.assertEqual(loss, np.sqrt(5)/3)

    def test_01_loss(self):
        lf = LossFunction(self.config)
        yhat = [1,0,0]
        y = [0,1,0]
        loss = lf.evaluate_schedule(yhat,y)
        self.assertEqual(loss, 2.0/3)

    def test_compute_p_overflow(self):
        lf = LossFunction(self.config)
        waste = pd.DataFrame({datetime(2015,1,1):[80,20], datetime(2015,1,2):[30, 30], datetime(2015,1,3):[50, 49], datetime(2015,1,4):[70, 20] ,
        datetime(2015,1,5):[30, 30], datetime(2015,1,6):[30, 30],  datetime(2015,1,7):[30, 30]}, index = ['t1', 't2'])
        schedule = pd.DataFrame({datetime(2015,1,1):[0,0], datetime(2015,1,2):[0,0], datetime(2015,1,3):[1,0], datetime(2015,1,4):[0,1] ,
        datetime(2015,1,5):[0,0], datetime(2015,1,6):[0,0], datetime(2015,1,7):[1,1]}, index = ['t1', 't2'])
        p_overflows, n_overflows, n_days = lf.compute_p_overflow(schedule, waste)
        self.assertEqual(n_days, 14)
        self.assertEqual(n_overflows, 2) #t1 on 15/1/2 and on 15/1/6

class outputTest(unittest.TestCase):
    pass

class StaffingTest(unittest.TestCase):
    def setUp(self):
        self.config = {
        'cols':{'feces':"FecesContainer_percent"}
        }
        self.staffing_parameters = {'N':5, 'W':10.0, 'NR':2,'D':5}
        d_waste = {
        'ToiletID' : pd.Series(['T1', 'T2','T3'], index=[0,1,2]),
        '0' : pd.Series([5,6,11], index=[0,1,2]),
        '1' : pd.Series([5,6,11], index=[0,1,2]),
        '2' : pd.Series([5,6,11], index=[0,1,2]),
        '3' : pd.Series([5,6,11], index=[0,1,2]),
        '4' : pd.Series([5,6,11], index=[0,1,2]),
        '5' : pd.Series([5,6,11], index=[0,1,2]),
        '6' : pd.Series([5,6,11], index=[0,1,2])
        }
        d_schedule = {
        'ToiletID' : pd.Series(['T1', 'T2','T3'], index=[0,1,2]),
        '0' : pd.Series([1,1,0], index=[0,1,2]),
        '1' : pd.Series([1,1,1], index=[0,1,2]),
        '2' : pd.Series([1,0,0], index=[0,1,2]),
        '3' : pd.Series([0,0,0], index=[0,1,2]),
        '4' : pd.Series([0,0,0], index=[0,1,2]),
        '5' : pd.Series([0,0,0], index=[0,1,2]),
        '6' : pd.Series([0,0,0], index=[0,1,2]),
        'Area' : pd.Series(['DSSG','DSSG','DSSG'], index=[0,1,2])
        }
        self.dfw = pd.DataFrame(d_waste)
        self.dfs = pd.DataFrame(d_schedule)
        logging.basicConfig(format="%(asctime)s %(message)s",
        filename="default.log", level=logging.DEBUG)
        self.log = logging.getLogger("Sanergy Collection Optimizer")

        screenlog = logging.StreamHandler(sys.stdout)
        screenlog.setLevel(logging.DEBUG)
        formatter = logging.Formatter("%(asctime)s - %(name)s: %(message)s")
        screenlog.setFormatter(formatter)
        self.log.addHandler(screenlog)

    def test_staff(self):
        staffing = Staffing(self.dfs, self.dfw, self.staffing_parameters,self.config)
        roster, s, vars =staffing.staff()
        collectors_day0 =  reduce(lambda x,y: x+y, [s.getVal(vars[i,'DSSG','0']) for i in range(0,self.staffing_parameters['N'])])
        collectors_day1 =  reduce(lambda x,y: x+y, [s.getVal(vars[i,'DSSG','1']) for i in range(0,self.staffing_parameters['N'])])
        collectors_day2 =  reduce(lambda x,y: x+y, [s.getVal(vars[i,'DSSG','2']) for i in range(0,self.staffing_parameters['N'])])
        collectors_day5 =  reduce(lambda x,y: x+y, [s.getVal(vars[i,'DSSG','5']) for i in range(0,self.staffing_parameters['N'])])
        #Need 2 people on Monday, 3 people on Tuesday, and 1 (-> 2) people on Wednesday. Zero on other days.
        self.assertEqual(collectors_day0, 2)
        self.assertEqual(collectors_day1, 3)
        self.assertEqual(collectors_day2, 2)
        self.assertEqual(collectors_day5, 0)
        self.assertEqual(roster.shape[0], 1)
        self.assertEqual( list(roster.loc['DSSG',['0','1','2']].values), [collectors_day0,collectors_day1,collectors_day2])

    def test_emptyStaffing(self):
        staffing = Staffing(None, None, self.staffing_parameters, self.config)
        output_roster = staffing.staff()[0]
        self.assertEqual(output_roster, None)


if __name__ == '__main__':
    unittest.main()
