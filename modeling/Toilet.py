class Toilet(object):
"""
Objectify the row from the toilet data and related data.
"""
    def __init__(self, toilet_id, df_toiletcollections):
        df_thisToilet = df_toiletcollections.loc[df_toiletcollections["ToiletID"]==toilet_id]
        #Create a map of date -> observation
        #self.waste_observations = df_thisToilet.
        pass
