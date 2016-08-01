
import pandas as pd
import numpy as np

class LossFunction(object):
    """
    A class to contain loss functions. Mainly used to take a Prediction object and new y data
    and return the evaluated loss, for different loss functions.
    """
    def __init__(self, config, type_loss_schedule = "0-1", type_loss_waste="L2", type_agg="mean"):
        """
        Available evaluation types:
        * ['L2']
        * ...?
        """
        self.loss_schedule = type_loss_schedule
        self.loss_waste = type_loss_waste
        self.aggregation = type_agg
        self.config = config

    def extract_vectors(self, x, y):
        """
        Given two dataframes x, y with toiletname, date, and value, take the inner join and return values only
        """
        joint_df = pd.merge(x,y, on = [self.config['cols']['toiletname'],self.config['cols']['date']])
        x_new = joint_df.iloc[:,3].values
        y_new = joint_df.iloc[:,4].values
        return x_new, y_new

    def evaluate_waste(self, yhat, y):
        """
        Given predicted yhat, evaluate it against observed y, using the loss function.

        Args:
            yhat, y (array(float)): The predicted, respectively observed values.

        Returns:
            loos: Evaluated loss as a float.
        """
        if (isinstance(yhat, pd.DataFrame) and isinstance(y, pd.DataFrame)):
            yhat, y = self.extract_vectors(yhat, y)

        if self.loss_waste == "L2":
            evaluated_loss = (1.0/len(yhat))*np.linalg.norm(np.asarray(yhat)-np.asarray(y), ord=2)
        elif self.loss_waste == "L1":
            evaluated_loss = (1.0/len(yhat))*np.linalg.norm(np.asarray(yhat)-np.asarray(y), ord=1)

        return(evaluated_loss)

    def evaluate_schedule(self, yhat, y):
        """
        Given predicted yhat, evaluate it against observed y, using the loss function.

        Args:
            yhat, y (array(0-1)): The predicted, respectively observed values.

        Returns:
            loss: Evaluated loss as a float.
        """
        if (isinstance(yhat, pd.DataFrame) and isinstance(y, pd.DataFrame)):
            yhat, y = self.extract_vectors(yhat, y)

        if self.loss_schedule == '0-1':
            evaluated_loss = np.mean(np.asarray(yhat) != np.asarray(y))
        return(evaluated_loss)

    def aggregate(self, losses):
        """
        Aggregate the array of losses for this experiment.
        """
        if self.aggregation == "mean":
            aggregated_loss = np.mean(losses)

        return(aggregated_loss)



def compare_models_by_loss_functions(results_from_experiments):
    aggregated_losses = {}
    for experiment, losses in results_from_experiments.iteritems():
        loss_function = LossFunction(experiment.config, experiment.parameters['loss'], experiment.parameters['aggregation_measure'])
        aggregated_losses[experiment] = loss_function.aggregate(losses)
    best_experiment = min(aggregated_losses.iterkeys(), key=(lambda key: aggregated_losses[key]))
    best_aggregated_loss = aggregated_losses[best_experiment]
    return best_experiment, best_aggregated_loss



if __name__ == '__main__':
    main()
