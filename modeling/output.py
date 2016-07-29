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
from sanergy.modeling.models import Model
from sanergy.modeling.dataset import create_enveloping_fold, grab_from_features_and_labels, create_future, format_features_labels

WDAY_SUNDAY = 6
WDAY_MONDAY = 0

def run_best_model_on_all_data(experiment, db, folds):
    """
    Run the model based on the passed experiment, predict on the test (future) data, and present the results.
    Write everything to the db.

    Args:
    test_features (array): Should include Day and Toilet
    """
    #Create a "master" fold: including all training and testing data.
    master_fold = create_enveloping_fold(folds)
    #Extract all features and labels
    features_all_big,labels_all_big,_,_=grab_from_features_and_labels(db, master_fold)
    features_all,labels_all=format_features_labels(features_all_big,labels_all_big)
    #Create a dataset for future prediction (?)... will need to do this differently... (?)
    features_future_big = create_future(master_fold, features_all_big, experiment.parameters) #TODO: This needs fixing
    features_future=format_features_labels(features_future_big,labels_all_big)[0]

    best_model = Model(experiment.model,experiment.parameters)
    #Results are the predicted probabilities that a toilet will overflow? This is probably an array
    yhat = best_model.run(features_all, labels_all, features_future)[0]

    #TODO: Need to transform yhat (a probability?) into 1/0 for collect vs not collect. Probably should happen within the Model class?
    output_schedule = present_schedule(yhat, features_future_big, experiment.config)
    #output_waste = ...

    #Workforce scheduling
    staffing = Staffing(output_schedule, None, None, experiment.config)
    output_roster = staffing.staff()[0]


    # Write the results to postgres
    db['connection'].execute('DROP TABLE IF EXISTS output."predicted_filled"')
    pd.DataFrame(yhat).to_sql(name='predicted_filled',
    schema="output",
    con=db['connection'],
    chunksize=1000)

    db['connection'].execute('DROP TABLE IF EXISTS output."collection_schedule"')
    pd.DataFrame(output_schedule).to_sql(name='collection_schedule',
    schema="output",
    con=db['connection'],
    chunksize=1000)

    #If we created the output roster, save it into the db too
    if output_roster:
        db['connection'].execute('DROP TABLE IF EXISTS output."workforce_schedule"')
        pd.DataFrame(output_roster).to_sql(name='workforce_schedule',
        schema="output",
        con=db['connection'],
        chunksize=1000)


    return(best_model)

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
