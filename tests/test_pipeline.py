import unittest
import yaml
import re, pprint
import sqlalchemy
import pandas as pd
import numpy as np
from datetime import datetime, date, timedelta


from sanergy.premodeling.Experiment import generate_experiments, Experiment
from sanergy.modeling.LossFunction import LossFunction, compare_models_by_loss_functions
from sanergy.modeling.dataset import grab_collections_data, temporal_split, format_features_labels, create_enveloping_fold
import sanergy.input.dbconfig as dbconfig
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

        print(next_days)


    def test_format_features_labels(self):
         features, labels = format_features_labels(self.features_big, self.labels_big)
         self.assertIsInstance(features, pd.DataFrame)
         self.assertIsInstance(labels, np.ndarray)



class LossFunctionTest(unittest.TestCase):
    def setUp(self):
        self.config = {
            'implementation':{'loss':"L2", 'aggregation_measure':'mean'}
        }
    def test_NewLoss(self):
        lf = LossFunction(self.config)
        self.assertIsInstance(lf,LossFunction)

    def test_L2_loss(self):
        lf = LossFunction(self.config, self.config['implementation']['loss'], self.config['implementation']['aggregation_measure'])
        yhat = [1,0,0]
        y = [0,2,0]
        loss = lf.evaluate(yhat,y)
        self.assertEqual(loss, np.sqrt(5)/3)

    def test_01_loss(self):
        lf = LossFunction(self.config, '0-1')
        yhat = [1,0,0]
        y = [0,1,0]
        loss = lf.evaluate(yhat,y)
        self.assertEqual(loss, 2.0/3)

class outputTest(unittest.TestCase):
    pass

if __name__ == '__main__':
    unittest.main()
