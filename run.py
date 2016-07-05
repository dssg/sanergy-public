#The first draft of the enveloping script that runs the pipeline.
import logging
import yaml
#Import external modules

import . from datasets

#Import the internal modules

def main(config_file_name):
  #Set up the Logger
  # define logging
  logging.basicConfig(format="%(asctime)s %(message)s",
                 filename="default.log", level=logging.INFO)
    log = logging.getLogger("Police EIS")

    screenlog = logging.StreamHandler(sys.stdout)
    screenlog.setLevel(logging.DEBUG)
    formatter = logging.Formatter("%(asctime)s - %(name)s: %(message)s")
    screenlog.setFormatter(formatter)
    log.addHandler(screenlog)
      #Load the yaml file.
    try:
        with open(config_file_name, 'r') as f:
            config = yaml.load(f)
        log.info("Loaded experiment file")
    except:
        log.exception("Failed to get experiment configuration file!")

  # Capture all of the results from all experiments
  storing_results_from_experiments = [{}]
    # Pushed to postres regularly, rather than stored in memory

  # 1. Generate all experiments [Jan]
  experiments = generate_experiments(config)
  """
      loop through the config variables to generate a list of experiments to run
      [{model: "randomForest",
        parameters: {},
        situation_vars: {prediction_window: (then, now)}...}, ...experiment 2... ]
  """

  # Loop through each of the experiments
  for experiment in experiments:
  
    # Brian will work on the following
    # 2. Create the labels / features data set in Postgres
    grab_from_dataset(experiment.config) #this creates df features and labels in the postgres
    
    # 3. The function splits should take in the config file, so that we can train every day / seven days / month, etc.
    [folds]=splits(experiment.config) # this will be a list of date ranges for train and test. Let's imagine that train and test are sets of pairs (start/end date), we pass that list of tuples of models.py and train each of the models on the list tuples.
    """
        [{"train":(start, end),
          "test":(start, end)}, ... Fold 2 ...]  
    """
    # 4. Folds are passed to models functions
    run_models_on_folds([folds], experiment.config):
      storing_results_from_modeling = [{}]
      for fold in folds
        DF{labels: train, features: train}, DF{labels: test, features: test} = grab_from_features_and_labels(fold)
        
        # 5. Run the models
        models_result = run ( labels.train, features.train, features.test, experiment.model, experiment.parameters)
        
        # 6. From the loss function
        evaluate (models_result, labels.test)

        """
          7. We have to save the model results and the evaluation in postgres
             Experiment x Fold, long file
        """
        
        #return([...list of loss...])
            
# 8. Evaluate the losses
compare_models_by_loss_functions()
  for experiment in experiments:
    # evaluate ([...list of loss...])
  return(...best model...)
  
# 9. Rerun best model on whole dataset
run_best_model_on_all_data(experiment_for_best_model, test, features)


