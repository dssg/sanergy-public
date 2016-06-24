import numpy as np
import pandas as pd
from sklearn import metrics

def compute_confusion(prediction, test_toilets, test_period):
    """
    Compute the confusion matrix taking numerical predictions and announcing a "success" if the prediction is lower than full and
    the toilet was less than full OR the prediction is more than full and the toilet was more than full.

    Args:
    prediction - an object of the Prediction class
    test_toilets - a list of Toilet objects
    test_period - a list of datetime
    """
