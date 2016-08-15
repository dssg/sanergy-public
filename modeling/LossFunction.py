
import pandas as pd
import numpy as np
import sklearn.metrics as skm

class LossFunction(object):
    TOILET_CAPACITY= 100
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
        x_new = joint_df['response_x'].as_matrix()
        y_new = joint_df['response_y'].as_matrix()
        return x_new, y_new

    def evaluate_waste(self, yhat, y):
        """
        Given predicted yhat, evaluate it against observed y, using the loss function.

        Args:
            yhat, y (array(float)): The predicted, respectively observed values.

        Returns:
            loss: Evaluated loss as a float.
        """
        if (isinstance(yhat, pd.DataFrame) and isinstance(y, pd.DataFrame)):
            #print(yhat.shape)
            #yhat.to_csv("yhat.csv")
            #y.to_csv("y.csv")
            yhat, y = self.extract_vectors(yhat, y)
            #print(len(yhat))
        if self.loss_waste == "L2":
            evaluated_loss = skm.mean_squared_error(y,yhat)
            #evaluated_loss =(1.0/len(yhat))*np.linalg.norm(yhat - y, ord = 2)

        elif self.loss_waste == "L1":
            #evaluated_loss = (1.0/len(yhat))*np.linalg.norm(np.asarray(yhat)-np.asarray(y), ord=1)
            evaluated_loss = skm.mean_absolute_error(y,yhat)
        else:
            evaluated_loss = skm.mean_squared_error(y,yhat)
            #evaluated_loss =  (1.0/len(yhat))*np.linalg.norm(np.asarray(yhat)-np.asarray(y), ord=2) #L2

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

    def compute_p_collect(self, collection_vector):
        return float(np.mean(collection_vector))

    def simple_waste_inspector(self, schedule_row, feces_row, urine_row) :
        """
        A *function* that checks how often a schedule row leads to an overflow, based on the true waste in waste row
        """
        current_urine = 0.0
        current_feces = 0.0
        n_overflows = n_overflows_urine = n_overflows_feces = 0
        n_overflows_conservative = n_overflows_feces_conservative = n_overflows_urine_conservative = 0
        n_days = 0
        for scheduled, new_feces, new_urine in zip(schedule_row, feces_row, urine_row):
            n_days += 1
            current_feces += new_feces
            current_urine += new_urine
            if current_feces > self.TOILET_CAPACITY:
                n_overflows_feces_conservative += 1
            if current_urine > self.TOILET_CAPACITY:
                n_overflows_urine_conservative += 1
            if (current_feces > self.TOILET_CAPACITY) or (current_urine > self.TOILET_CAPACITY):
                n_overflows_conservative +=1

            if scheduled:
                current_feces = current_urine = 0.0

            if current_feces > self.TOILET_CAPACITY:
                n_overflows_feces += 1
            if current_urine > self.TOILET_CAPACITY:
                n_overflows_urine += 1
            if (current_feces > self.TOILET_CAPACITY) or (current_urine > self.TOILET_CAPACITY):
                n_overflows +=1

        return n_overflows, n_overflows_conservative, n_overflows_feces, n_overflows_feces_conservative, n_overflows_urine, n_overflows_urine_conservative, n_days

    def compute_p_overflow(self, schedule, true_feces, true_urine):
        """
        Given a proposed schedule and a true waste matrix, evaluate how many overflows there would have been if the schedule was followed.
        Args:
          schedule (DataFrame): the proposed schedule, in the format row=toiletname, column=date. Has 1 if the collection is recommended.
          true_waste (DataFrame): The same format as schedule, the entries are actually accumulated waste percentages.
        """
        n_overflows = n_overflows_feces = n_overflows_urine = 0
        n_overflows_conservative = n_overflows_feces_conservative = n_overflows_urine_conservative = 0 #
        n_days = 0
        for i_toilet, toilet_schedule in schedule.iterrows():
            toilet_feces = true_feces.loc[i_toilet]
            toilet_urine = true_urine.loc[i_toilet]
            n_overflows_i, n_overflows_conservative_i, n_overflows_feces_i, n_overflows_feces_conservative_i, n_overflows_urine_i, n_overflows_urine_conservative_i, n_days_i = self.simple_waste_inspector(toilet_schedule, toilet_feces, toilet_urine)
            n_overflows += n_overflows_i
            n_overflows_feces += n_overflows_feces_i
            n_overflows_urine += n_overflows_urine_i
            n_overflows_conservative += n_overflows_conservative_i
            n_overflows_feces_conservative += n_overflows_feces_conservative_i
            n_overflows_urine_conservative += n_overflows_urine_conservative_i
            n_days += n_days_i

        return( (n_overflows / float(n_days)), (n_overflows_conservative / float(n_days)), (n_overflows_feces / float(n_days)), (n_overflows_feces_conservative / float(n_days)), (n_overflows_urine / float(n_days)), (n_overflows_urine_conservative / float(n_days)), n_overflows, n_overflows_conservative, n_days )


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
