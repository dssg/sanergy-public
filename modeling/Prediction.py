import pandas as pd


class COLNAMES(object):
    #Column names
    TOILETNAME = "ToiletName"
    DATE = "Collection_Date"
    FECES = "FecesContainer_percent" #How full the container will be
    URINE = "UrineContainer_percent"
    WASTE = {"feces": FECES, "urine": URINE}
    FECES_COLLECT = "" #Should the toilet be collected?
    URINE_COLLECT = ""
    COLLECT = {"feces": FECES_COLLECT, "urine": URINE_COLLECT}


class Prediction(object):
    """
    An encapsulating class for toilet waste predictions, so we don't have to worry about what to pass.
    """
    def __init__(self,df):
        """

        Args:
            df: A Dataframe with the columns for the toiletname, date (of prediction),
        """
        self.df = df
        print(df)

    def get_toilet_waste_estimate(self, toilet, day, type_waste="feces"):
        """
        Return an estimate of the toilet waste for the given toilet on the given day

        Args:
            type: "feces" vs "urine"

        """
        waste_estimate = self.df.loc[ (self.df[COLNAMES.TOILETNAME] == toilet) & (self.df[COLNAMES.DATE] == day) ]
        #waste_estimate = self.df[[COLNAMES.TOILETNAME, COLNAMES.DATE, COLNAMES.WASTE[type_waste]]].loc[ self.df[COLNAMES.TOILETNAME] == toilet & self.df[COLNAMES.DATE] == day ]
        return( waste_estimate )



def main():
    df_test = pd.DataFrame([["a","Tuesday",13.3], ["a","Wednesday",13.3], ["b","Wednesday",13.1]],columns=[COLNAMES.TOILETNAME, COLNAMES.DATE, COLNAMES.FECES])
    pred = Prediction(df_test)
    print(pred.get_toilet_waste_estimate("a","Wednesday"))


if __name__ == '__main__':
    main()
