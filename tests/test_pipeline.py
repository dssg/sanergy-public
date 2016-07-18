import unittest
import yaml
from sanergy.premodeling.Experiment import generate_experiments
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

if __name__ == '__main__':
    unittest.main()
