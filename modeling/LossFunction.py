
import pandas as pd
import numpy as np

class LossFunction(object):
    """
    A class to contain loss functions. Mainly used to take a Prediction object and new y data
    and return the evaluated loss, for different loss functions.
    """
    def __init__(self, config, type_loss="L2", type_agg="mean"):
        """
        Available evaluation types:
        * ['L2']
        * ...?
        """
        self.loss = type_loss
        self.aggregation = type_agg
        self.config = config

    def evaluate(self, yhat, y):
        """
        Given predicted yhat, evaluate it against observed y, using the loss function.

        Args:
            yhat, y (array(float)): The predicted, respectively observed values.

        Returns:
            loos: Evaluated loss as a float.
        """

        if self.loss == "L2":
            evaluated_loss = (1.0/len(yhat))*np.linalg.norm(np.asarray(yhat)-np.asarray(y), ord=2)
        elif self.loss == "L1":
            evaluated_loss = (1.0/len(yhat))*np.linalg.norm(np.asarray(yhat)-np.asarray(y), ord=1)

        return(evaluated_loss)

    def aggregate(self, losses):
        """
        Aggregate the array of losses for this experiment.
        """
        if self.aggregation == "mean":
            aggregated_loss = np.mean(losses)

        return(aggregated_loss)

    def evaluate_waste_prediction(self, trained_model, new_data, type_waste="feces"):
        """
        !!! Probably invalid currently. We don't currently have the Model.predict() function.
        Evaluate the prediction against the loss function on new data. Apply the function iteratively for purposes such as crossvalidation.

        Args:
            trained_model (model): A prediction object to evaluate, comprises of a trained model applied to data. See the Prediction class for more info.
            new_data (Dataframe): Data to evaluate the prediction on. It should be already subsetted to the toilets/dates as appropriate.

        Returns:
            loss: Evaluated loss function as a (single) float.
        """
        #Assume feces, for now...
        predicted_waste = pd.DataFrame(index=new_data.index, columns = [self.config['cols']['date'], self.config['cols']['date'], "predicted"])
        #Append a row with the waste estimate from the prediction, indexed by the toilet name and date.
        for i, collected in new_data.iterrows(): #collected is a row corresponding to one toilet collection.
            predicted_waste.loc[len(predicted_waste)] = [collected[self.config['cols']['toiletname']],collected[self.config['cols']['date']], trained_model.predict(collected[config['cols']['toiletname']],collected[config['cols']['date']]) ]

        waste = pd.merge(predicted_waste, new_data, on = [self.config['cols']['toiletname'], self.config['cols']['date']])

        #print(waste)
        if self.loss == "L2":
            loss = np.mean((waste["predicted"]-waste[self.config['cols'][type_waste]])**2)

        return(loss)


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
