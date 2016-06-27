
import pandas as pd
from . import Prediction
from Prediction import COLNAMES

class LossFunction(object):
"""
A class to contain loss functions. Mainly used to take a Prediction object and new y data
 and return the evaluated loss, for different loss functions.
"""
    def __init__(self, type):
        """
        Available evaluation types:
        * L2
        * ...?
        """
        super(LossFunction, self).__init__()
        self.type = type

    def evaluate_waste_prediction(prediction: Prediction, new_data, type_waste="feces"):
        """
        Evaluate the prediction against the loss function on new data.

        Args:
            prediction (Prediction): A prediction object to evaluate
            new_data (Dataframe): Data to evaluate the prediction on. It should be already subsetted to the toilets/dates as appropriate.

        Returns:
            loss: Evaluated loss function as a float.
        """
        #Assume feces, for now...
        predicted_waste = []
        for ic, collected in new_data.iterrows():
            predicted_waste.append([collected[COLNAMES.TOILETNAME],collected[COLNAMES.DATE], prediction.get_toilet_waste_estimate(collected[COLNAMES.TOILETNAME],collected[COLNAMES.DATE]) ] )
        predicted_waste = pd.DataFrame(predicted_waste,columns = [COLNAMES.TOILETNAME, COLNAMES.DATE, "predicted"])
        waste = merge(predicted_waste, new_data, on = [COLNAMES.TOILETNAME, COLNAMES.DATE])

        if type == "L2":
            loss = np.mean((waste["predicted"]-waste[COLNAMES.WASTE[type_waste]])**2)

        return(loss)
