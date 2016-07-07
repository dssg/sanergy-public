"""
Scripts related to the final model output.
Needs testing and tuning once the upstream model components are clarified.
"""


# Connect to the database
import dbconfig
import psycopg2
from sqlalchemy import create_engine

import datetime as dt

import . from models

WDAY_SUNDAY = 6
WDAY_MONDAY = 0

def run_best_model_on_all_data(experiment, all_features, all_labels, test_features):
    """
    Run the model based on the passed experiment, predict on the test (future) data, and present the results.
    Write everything to the db.

    Args:
        test_features (array): Should include Day and Toilet
    """
    best_model = Model(experiment["model"])
    #Results are the predicted probabilities that a toilet will overflow? This is probably an array
    yhat = best_model.run(all_features, all_labels, test_features, experiment["model"], experiment["parameters"])[0]
    #TODO: Need to transform yhat (a probability?) into 1/0 for collect vs not collect. Probably should happen within the Model class?
    output_schedule = present_schedule(yhat, test_features, experiment.config)


    # Write the results to postgres
    engine = create_engine('postgresql+psycopg2://%s:%s@%s:%s' %(dbconfig.config['user'],
							dbconfig.config['password'],
							dbconfig.config['host'],
							dbconfig.config['port']))
    try:
    	conn = engine.connect()
    	print('connected to postgres')
    except:
    	log.warning('Failure to connect to postgres')

    conn.execute('DROP TABLE IF EXISTS output."predicted_filled"')
	yhat.to_sql(name='predicted_filled',
			schema="output",
			con=conn,
			chunksize=1000)

    conn.execute('DROP TABLE IF EXISTS output."collection_schedule"')
    output_schedule.to_sql(name='collection_schedule',
			schema="output",
			con=conn,
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
    timed_predictions['wday'] = [day.weekday() for day in config['cols']['date']]
    timed_predictions = timed_predictions[[config['cols']['toiletname'],'wday','predicted']]
    schedule = timed_predictions.pivot(index = config['cols']['toiletname'], columns = 'wday' ,values='predicted')
    #Reorder the schedule so that the closest days are on the left. For this to work, the prediction window must be less or equal to 7 days
    wday_ordering = range(config['implementation']['prediction_weekday_start'],(WDAY_SUNDAY+1)) + range(WDAY_MONDAY, config['implementation']['prediction_weekday_start'])
    schedule = schedule[[config['cols']['toiletname']] + wday_ordering]
    return(schedule)
