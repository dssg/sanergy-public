
import pandas as pd
import numpy as np

class COLNAMES(object):
    #Column names
    TOILETNAME = "ToiletName" #the name of the toilet / toilet identifier
    DATE = "Collection_Date" # date / date identifier
    FECES = "FecesContainer_percent" #How full the container will be
    URINE = "UrineContainer_percent"
    WASTE = {"feces": FECES, "urine": URINE} # Either feces or urine
    FECES_COLLECT = "" #Should the toilet be collected?
    URINE_COLLECT = ""
    COLLECT = {"feces": FECES_COLLECT, "urine": URINE_COLLECT}



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
        self.type = type

    def evaluate_waste_prediction(self, prediction, new_data, type_waste="feces"):
        """
        Evaluate the prediction against the loss function on new data. Apply the function iteratively for purposes such as crossvalidation.

        Args:
            prediction (Prediction): A prediction object to evaluate, comprises of a trained model applied to data. See the Prediction class for more info.
            new_data (Dataframe): Data to evaluate the prediction on. It should be already subsetted to the toilets/dates as appropriate.

        Returns:
            loss: Evaluated loss function as a (single) float.
        """
        #Assume feces, for now...
        predicted_waste = pd.DataFrame(index=new_data.index, columns = [COLNAMES.TOILETNAME, COLNAMES.DATE, "predicted"])
        #Append a row with the waste estimate from the prediction, indexed by the toilet name and date.
        for i, collected in new_data.iterrows(): #collected is a row corresponding to one toilet collection.
            predicted_waste.loc[len(predicted_waste)] = [collected[COLNAMES.TOILETNAME],collected[COLNAMES.DATE], prediction.get_toilet_waste_estimate(collected[COLNAMES.TOILETNAME],collected[COLNAMES.DATE]) ]

        waste = pd.merge(predicted_waste, new_data, on = [COLNAMES.TOILETNAME, COLNAMES.DATE])

        #print(waste)
        if self.type == "L2":
            loss = np.mean((waste["predicted"]-waste[COLNAMES.WASTE[type_waste]])**2)

        return(loss)


def main():
    #Random testing code
    L2 = LossFunction("L2")
    df_hat = pd.DataFrame([["Toilet a","Tuesday",13.3], ["Toilet a","Wednesday",13.3], ["Toilet b","Wednesday",13.1]],columns=[COLNAMES.TOILETNAME, COLNAMES.DATE, COLNAMES.FECES])
    df_test = pd.DataFrame([["Toilet a","Tuesday",15], ["Toilet a","Wednesday",13.2], ["Toilet b","Wednesday",13.3]],columns=[COLNAMES.TOILETNAME, COLNAMES.DATE, COLNAMES.FECES])
    pred = Prediction(df_hat)
    print(L2.evaluate_waste_prediction(pred,df_test))



if __name__ == '__main__':
    main()
