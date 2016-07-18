import unittest
import yaml
import re, pprint
import sqlalchemy
from datetime import datetime, date, timedelta


from sanergy.premodeling.Experiment import generate_experiments, Experiment
from sanergy.modeling.LossFunction import LossFunction
from sanergy.modeling.dataset import grab_collections_data, temporal_split
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


class datasetTest(unittest.TestCase):
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
        splits=temporal_split(start_date='2014-01-01',
        end_date='2014-05-05',
        train_on={'days':0, 'weeks':5},
        test_on={'days':0, 'weeks':1},
        day_of_week=6,
        floating_window=False)
        #Checked by Brian
        self.assertEqual(len(splits),12)

class LossFunctionTest(unittest.TestCase):
    def setUp(self):
        self.config = {
            'implementation':{'loss':"L2", 'aggregation_measure':'mean'}
        }
    def test_NewLoss(self):
        lf = LossFunction(self.config)
        self.assertIsInstance(lf,LossFunction)

if __name__ == '__main__':
    unittest.main()
