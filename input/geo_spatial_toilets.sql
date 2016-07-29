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
			ST_Distance(z."Point", s."Point") as "Distance" from premodeling.toiletdensity z,
--  First, merge the toilet density table onto itself, where ID != ID
		(select * from premodeling.toiletdensity) s
			where (s."ToiletID" != z."ToiletID")) sub;

-- Delete the density table just for the hell of it
DROP TABLE IF EXISTS premodeling.toiletdensity;

/**
 * The following count should equal 780,572 = ((884*884)-884)
 * select count(*) from premodeling.toiletdistances
 */
