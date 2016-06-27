
import pandas as pd
import numpy as np
from Prediction import Prediction
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

    def evaluate_waste_prediction(self, prediction, new_data, type_waste="feces"):
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


        predicted_waste = pd.DataFrame.from_records(predicted_waste,columns = [COLNAMES.TOILETNAME, COLNAMES.DATE, "predicted"])
        waste = pd.merge(predicted_waste, new_data, on = [COLNAMES.TOILETNAME, COLNAMES.DATE])

        #print(waste)
        if self.type == "L2":
            loss = np.mean((waste["predicted"]-waste[COLNAMES.WASTE[type_waste]])**2)

        return(loss)


def main():
    L2 = LossFunction("L2")
    df_test = pd.DataFrame([["a","Tuesday",13.3], ["a","Wednesday",13.3], ["b","Wednesday",13.1]],columns=[COLNAMES.TOILETNAME, COLNAMES.DATE, COLNAMES.FECES])
    df_test2 = pd.DataFrame([["a","Tuesday",15], ["a","Wednesday",13.2], ["b","Wednesday",13.3]],columns=[COLNAMES.TOILETNAME, COLNAMES.DATE, COLNAMES.FECES])
    pred = Prediction(df_test)
    print(L2.evaluate_waste_prediction(pred,df_test2))



if __name__ == '__main__':
    main()
