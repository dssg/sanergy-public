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

/* Create a a ToiletID by ToiletID matrix of distances
 * to be used in conjunction with the premodeling.toilethistory
 * table as a way of understanding density within an area.
 */			
			
DROP TABLE IF EXISTS premodeling.toiletdensity;
-- similar drop if exists
select "ToiletID",
		ST_Transform(ST_SetSRID(ST_MakePoint("Latitude", "Longitude"), 4326),21037) as "Point"
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

/*
 * Another attempt at the density table, this time
 * by day and by toilet statistics for the 50m box. */
DROP TABLE IF EXISTS premodeling.toiletdensity;
create table premodeling.toiletdensity
(
	"ToiletID" text,
	"Collection_Date" timestamp,
	"50m" int,
	"mean50m" float8,
	"std50m" float8
);


-- A function with two loops by day and by toilet
do $$
-- Declare some variables for the FOR loops
DECLARE
	ddate timestamp;
BEGIN
-- Initially loop through the date range
    FOR ddate IN select date from generate_series(
					  '2010-01-01'::date,
					  '2016-05-23'::date,
					  '1 day'::interval) date
    LOOP
-- Return the output
        RAISE NOTICE 'Date: %', ddate;
-- Insert the aggregated values into the density table        
    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","50m","mean50m","std50m")
    	select 	"ToiletID",
		ddate,
		count(*),
		avg("Distance"),
		stddev("Distance")
		from premodeling.toiletdistances
-- Only select the 50m box    			
		where ("50m" != 0) and ("ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
		group by "ToiletID";
    END LOOP;
END $$;
-- Bing, bang, boom



