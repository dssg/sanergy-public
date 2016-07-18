import unittest
import yaml
from sanergy.premodeling.Experiment import generate_experiments, Experiment
from sanergy.modeling.LossFunction import LossFunction
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
