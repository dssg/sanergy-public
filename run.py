#The first draft of the enveloping script that runs the pipeline.
import logging
import yaml
import LossFunction, AggregationFunction from LossFunction
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
  # Save results in a dict of lists: {Exp 1: [cv_loss_per_fold_0, cv_loss_per_fold_1, ...], Exp 2:[...], ...}.
  # Note that the number of the Experiments is fixed per run, but the number of folds may differ by experiment.
  # In the aggregate evaluation, we may need to interpret losses from folds with different windows differently.
  results_from_experiments = {} #A dict keyed by experiments and valued by a list of cv-losses for that experiment.

    # Pushed to postres regularly, rather than stored in memory

  # 1. Generate all experiments [DONE]
  experiments = generate_experiments(config)
  """
      loop through the config variables to generate a list of experiments to run
      [{model: "randomForest",
        parameters: {},
        situation_vars: {prediction_window: (then, now)}...}, ...experiment 2... ]
  """

  # Loop through each of the experiments
  for experiment in experiments:
     #Initialize the loss function.
    loss = LossFunction(experiment.config)

    # TODO: Brian
    # 2. Create the labels / features data set in Postgres
    grab_from_dataset(experiment.config) #this creates df features and labels in the postgres

    # TODO: Brian
    # 3. The function splits should take in the config file, so that we can train every day / seven days / month, etc.
    [folds]=splits(experiment.config) # this will be a list of date ranges for train and test. Let's imagine that train and test are sets of pairs (start/end date), we pass that list of tuples of models.py and train each of the models on the list tuples.
    """
        [{"train":(start, end),
          "test":(start, end)}, ... Fold 2 ...]
    """
    # TODO: Ivana
    # 4. Folds are passed to models functions
    results_from_experiments[experiment] = run_models_on_folds([folds], experiment.config) #See below the structure. Return a list of losses per fold.
        for i_fold, fold in enumerate(folds):
            DF{labels: train, features: train}, DF{labels: test, features: test} = grab_from_features_and_labels(fold)
            losses = []
            # TODO: Ivana
            # 5. Run the models
            yhat, trained_model = model.gen_model( labels.train, features.train, features.test, experiment.model, experiment.parameters)

            # DONE
            # 6. From the loss function
            losses.append(loss.evaluate(yhat, labels.test))

            """
              TODO: All
              7. We have to save the model results and the evaluation in postgres
                 Experiment x Fold, long file
            """
            #return([...list of loss...])
        return(losses)

    # 8. Evaluate the losses
    # Have results_from_experiments ready or load it from the db
    best_experiment, best_loss_ = compare_models_by_loss_functions(results_from_experiments)

    # 9. Rerun best model on whole dataset
    run_best_model_on_all_data(best_experiment, test)
    # Write the results to postgres
