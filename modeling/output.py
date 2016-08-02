"""
Scripts related to the final model output.
Needs testing and tuning once the upstream model components are clarified.
"""


# Connect to the database
import psycopg2
from sqlalchemy import create_engine
import datetime as dt
import pandas as pd


import sanergy.input.dbconfig as dbconfig
from sanergy.modeling.dataset import create_enveloping_fold, grab_from_features_and_labels, create_future, format_features_labels

WDAY_SUNDAY = 6
WDAY_MONDAY = 0

def write_evaluation_into_db(results, db , append = True, chunksize=1000):
    if ~append :
        db['connection'].execute('DROP TABLE IF EXISTS output."predicted_filled"')
    results.to_sql(name='evaluations',
    schema="output",
    con=db['connection'],
    chunksize=chunksize)

    return None



def present_schedule(predicted, day_toilet_data, config):
    """
    TODO: Tidy up the output table.

    Present as a dataframe in the form
    Toil    | Mo | Tu | We ...
    KN 13.1 | _  | _  | x ....
    ....

    Args:
        day_toilet_data (Dataframe): Should contain day and toiletname as columns.
        predicted (array): A 0/1 array for collect vs not collect decisions.
    """
    timed_predictions = day_toilet_data
    timed_predictions['predicted'] = predicted
    timed_predictions['wday'] = [day.weekday() for day in day_toilet_data[config['cols']['date']] ]
    timed_predictions = timed_predictions[[config['cols']['toiletname'],'wday','predicted']]
    schedule = timed_predictions.pivot(index = config['cols']['toiletname'], columns = 'wday' ,values='predicted')
    #Reorder the schedule so that the closest days are on the left. For this to work, the prediction window must be less or equal to 7 days
    if config['implementation']['prediction_horizon'][0] == 7:
        wday_ordering = range(config['implementation']['prediction_weekday_start'][0],(WDAY_SUNDAY+1)) + range(WDAY_MONDAY, config['implementation']['prediction_weekday_start'][0])
        schedule = schedule[wday_ordering]
    return(schedule)
