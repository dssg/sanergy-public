/* Create a history of Start and Last collection dates for each ToiletID
 * This table is used as a look up to check the current density, given a
 * Fake-Today.
 */

DROP TABLE IF EXISTS premodeling.toilethistory;
-- Drop the table if it exists already, simply for efficiency
select "Toilet__c" as "ToiletID",
	   	min("Collection_Date__c") as "StartCollection",
	   	max("Collection_Date__c") as "LastCollection"
		   	into premodeling.toilethistory
			from input."Collection_Data__c"
			group by "ToiletID";
-- group by each toilet and produce the ToiletID, and min/max
--  for each collection date range

/* Create a sham toiletdensity table. There's a circular dependency between
 * toiletcollections and toiletdensity. So as a hack, we create an empty
 * toiletdensity table, create toiletcollections, then create the real
 * toiletdensity table, and then re-create toiletcollections. This is a
 * terrible hack.
 *
 * TODO: VERIFY THAT USING THIS SHAM TABLE DOESN'T MESS UP THE GENERATION OF
 *       THE REAL TABLE
 */
DROP TABLE IF EXISTS premodeling.toiletdensity;
-- similar drop if exists
select "ToiletID",
		ST_Transform(ST_SetSRID(ST_MakePoint(min("Latitude"), min("Longitude")), 4326),21037) as "Point"
-- #4326 is WGS84, but that one is not linear but spherical -> difficult to compute distances
-- #21037 is one of the 4 systems in Kenya and contains Nairobi
	   	into premodeling.toiletdensity
			from input."Collection_Data__c" join input."tblToilet"
			on "Toilet__c"="ToiletID"
		group by "ToiletID";

-- Third, save the sub-sub query to the toilet distances table
DROP TABLE IF EXISTS premodeling.toiletdistances;
select sub.* into premodeling.toiletdistances from
-- Second, create a sub query of the toilet density table, calculating
--  the distances between each toilet
	(select z."ToiletID",
			s."ToiletID"  as "NeighborToiletID",
			ST_Distance(z."Point", s."Point") as "Distance",
			CAST(ST_DWithin(z."Point", s."Point", 5.0) as INT) as "5m",
			CAST(ST_DWithin(z."Point", s."Point", 25.0) as INT) as "25m",
			CAST(ST_DWithin(z."Point", s."Point", 50.0) as INT) as "50m",
			CAST(ST_DWithin(z."Point", s."Point", 100.0) as INT) as "100m" from premodeling.toiletdensity z,
--  First, merge the toilet density table onto itself, where ID != ID
		(select * from premodeling.toiletdensity) s
			where (s."ToiletID" != z."ToiletID")) sub;

DROP TABLE IF EXISTS premodeling.toiletdensity;

-- Create the sham toilet density
select "ToiletID",
-- Second, sum over the toilets that existed during the same time periods
       null::timestamp as "Collection_Date",
       null::text as "functional",
       null::text as "area",
       null::text as "period",
       null::text as "variable",
       0.0::float8 as "observations",
       0.0::float8 as "value"
        into premodeling.toiletdensity
		from premodeling.toiletdistances
-- First, subquery for the toilets and neighbors that existed within the same time periods
			where ("ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" > '2014-01-01')and("LastCollection" < '2016-07-01')))
				and ("NeighborToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" > '2014-01-01')and("LastCollection" < '2016-07-01')))
					group by "ToiletID";
