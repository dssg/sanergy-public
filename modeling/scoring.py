import numpy as np
import pandas as pd
from sklearn import metrics

from . import dataset

FULL_PERCENT = 100

def compute_confusion(prediction, test_toilets, test_period):
    """
    Compute the confusion matrix taking numerical predictions and announcing a "success" if the prediction is lower than full and
    the toilet was less than full OR the prediction is more than full and the toilet was more than full.
    We use the days from the test_period, get the predictions for them
    and

    Args:
    prediction - an object of the Prediction class
    test_toilets - a list of toilet ids to test on
    test_period - a list of datetime

    Returns:
    cm_feces, cm_urine - the confusion matrix for the prediction on the test_toilets on the test_period, for feces and urine respectively
    """
    collections = dataset.grab_collections_data(test_toilets, test_period)

    #Take the toilet-periods, attach predictions, attach the true collections
    train_test_df = collections[["ToiletID","Collection_Date",
    "FecesContainer_percent","UrineContainer_percent"]]
    feces_perc_pred = pd.Dataframe({[row["toiletid"].values, row["collection_date"].values:prediction.(toiletid,collection_date,"feces") for collections[["ToiletID","Collection_Date"]].iterrows()}})
    urine_perc_pred = pd.Dataframe({[row["toiletid"].values, row["collection_date"].values:prediction.(toiletid,collection_date,"urine") for collections[["ToiletID","Collection_Date"]].iterrows()}})
    #train_test_df = merge(train_test_df,feces_perc_pred,on=["ToiletID","Collection_Date"])
    #train_test_df = merge(train_test_df,urine_perc_pred,on=["ToiletID","Collection_Date"])


    #Confusion matrix on preds > FULL_PERCENT vs collections.faeces > FULL_PERCENT
    #For feces...
    cm_feces = metrics.confusion_matrix(train_test_df.FecesContainer_percent > FULL_PERCENT, feces_perc_pred > FULL_PERCENT)
    cm_urine = metrics.confusion_matrix(train_test_df.UrineContainer_percent > FULL_PERCENT, urine_perc_pred > FULL_PERCENT)

    return cm_feces, cm_urine


def temporally_crossvalidate(M, data , w=1, Mu = 'mean'):
    """
    Given models M, data = [X_t,y_t] for t=1..T, a loss function L, and a prediction window w, do the following:
    for m in M:
        for t = 1..T-w:
             train m on [X_tau, y_tau]_{tau=1..t}
             use m to predict y_{tau + 1}...y_{tau+w}
             evaluate the predictions using loss L, yielding the evaluated loss l_t
             save l_t in a loss vector.
        Then, evaluate {l_t} using an aggregation measure Mu (function)
    Args
    M (list(Model)) - the list of models to be trained and evaluated through cross
    """
