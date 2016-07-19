#The first draft of the enveloping script that runs the pipeline.
#Import external modules
import logging
import yaml
import sys

#Import our modules
from sanergy.modeling.LossFunction import LossFunction, compare_models_by_loss_functions
from sanergy.premodeling.Experiment import generate_experiments
from sanergy.modeling.dataset import grab_collections_data, get_db, temporal_split
from sanergy.modeling.models import run_models_on_folds
from sanergy.modeling.output import run_best_model_on_all_data

#
#import sanergy.preprocessing from datasets

#Import the internal modules

def main(config_file_name="default.yaml"):
  #Set up the Logger
  # define logging
  logging.basicConfig(format="%(asctime)s %(message)s",
  filename="default.log", level=logging.DEBUG)
  log = logging.getLogger("Sanergy Collection Optimizer")

  screenlog = logging.StreamHandler(sys.stdout)
  screenlog.setLevel(logging.DEBUG)
  formatter = logging.Formatter("%(asctime)s - %(name)s: %(message)s")
  screenlog.setFormatter(formatter)
  log.addHandler(screenlog)
  #Load the yaml file.
  log.debug("Testing logger.")
  try:
      with open(config_file_name, 'r') as f:
          config = yaml.load(f)
          log.info("Loaded experiment file")
  except:
      log.exception("Failed to get experiment configuration file!")

  db = get_db(config, log)
  # Capture all of the results from all experiments
  # Save results in a dict of lists: {Exp 1: [cv_loss_per_fold_0, cv_loss_per_fold_1, ...], Exp 2:[...], ...}.
  # Note that the number of the Experiments is fixed per run, but the number of folds may differ by experiment.
  # In the aggregate evaluation, we may need to interpret losses from folds with different windows differently.
  losses_from_experiments = {} #A dict keyed by experiments and valued by a list of cv-losses for that experiment.

  # 1. Generate all experiments
  log.info("Generate experiments from default.yaml...")
  experiments = generate_experiments(config)
  log.info("Generated {0} experiments.".format(len(experiments)))


  """
      loop through the config variables to generate a list of experiments to run
      [{model: "randomForest",
        parameters: {},
        situation_vars: {prediction_window: (then, now)}...}, ...experiment 2... ]
  """
  #3. The function splits should take in the config file, so that we can train every day / seven days / month, etc.
  folds=temporal_split(config['cv']) # this will be a list of date ranges for train and test. Let's imagine that train and test are sets of pairs (start/end date), we pass that list of tuples of models.py and train each of the models on the list tuples.
  log.debug("Generated {} folds.".format(len(folds)))
  """
  [{"train":(start, end),
  "test":(start, end)}, ... Fold 2 ...]
  """

  # Loop through each of the experiments
  # for i_exp, experiment in enumerate(experiments):
  #     log.debug("Running experiment #{0}".format(i_exp))
  #     #Initialize the loss function.
  #     lf = LossFunction(experiment.config, experiment.parameters['loss'], experiment.parameters['aggregation_measure'])
  #
  #     # 2. Create the labels / features data set in Postgres
  #     #TODO: grab_collections_data needs a unittest
  #     #features, responses=grab_collections_data(db, experiment.config['Xy'], log) #this creates df features and labels in the postgres
  #     log.debug("Generated features in the database.")
  #
  #
  #     # 4. Folds are passed to models functions
  #     # 5. Run the models
  #     # 6. Calculate and save the losses
  #     losses_from_experiments[experiment] = run_models_on_folds(folds, lf, db, experiment) #See below the structure. Return a list of losses per fold.
  #     # 8. Evaluate the losses
  #     # Have results_from_experiments ready or load it from the db
  # log.info("Crossvalidated the experiments.")
  # best_experiment, best_loss = compare_models_by_loss_functions(losses_from_experiments)
  best_experiment = experiments[0]
  # 9. Rerun best model on whole dataset
  #TODO: What is test?
  run_best_model_on_all_data(best_experiment, db, folds)
  # Write the results to postgres

if __name__ == '__main__':
    main()
