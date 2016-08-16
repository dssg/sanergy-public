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

-- Delete the density table just for the hell of it
DROP TABLE IF EXISTS premodeling.toiletdensity;

-- Create the toilet density
select "ToiletID",
-- Second, sum over the toilets that existed during the same time periods
	   sum("5m") as "5m",
	   sum("25m") as "25m",
	   sum("50m") as "50m",
	   sum("100m") as "100m"
		into premodeling.toiletdensity
		from premodeling.toiletdistances
-- First, subquery for the toilets and neighbors that existed within the same time periods
			where ("ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" > {d '2014-01-01'})and("LastCollection" < {d '2016-07-01'})))
				and ("NeighborToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" > {d '2014-01-01'})and("LastCollection" < {d '2016-07-01'})))
					group by "ToiletID";
/**
 * The following count should equal 780,572 = ((884*884)-884)
 * select count(*) from premodeling.toiletdistances
 */

select count(*), sum("5m"), avg("5m"), stddev("5m") from premodeling.toiletdensity where "5m"=0;
select count(*), sum("5m"), avg("5m"), stddev("5m") from premodeling.toiletdensity where "5m">0;
-- is there something nearby and does it effect usage
-- did the usage change when another toilet was added
-- how compact the area is
-- how far away from the collection center is the toilet


/*
 * Another attempt at the density table, this time
 * by day and by toilet statistics for the 50m box. */
DROP TABLE IF EXISTS premodeling.toiletdensity;
create table premodeling.toiletdensity
(
	"ToiletID" text,
	"Collection_Date" timestamp,
	"FUNCTIONAL" text,
	"AREA" text,
	"PERIOD" text,
	"VARIABLE" text,
	"OBSERVATIONS" float8,
	"VALUE" float8
);

-- A function with two loops by day and by toilet
do $$
-- Declare some variables for the FOR loops
DECLARE
	ddate timestamp;
	toilet text;
	variable text;
	functional text;
	area text;

	period varchar;
	distance varchar;
begin	
-- distances := string_to_array('100m,50m,25m,5m',',');
-- Initially loop through the date range
    FOR ddate IN select date from generate_series(
					  '2011-01-01'::date,
					  '2016-05-23'::date,
					  '1 day'::interval) date
    LOOP
-- Return the output
-- Insert the aggregated values into the density table
		FOREACH period in array string_to_array('1 days,7 days',',')
		LOOP
			FOREACH distance IN array string_to_array('50m,5m',',')
			LOOP
				RAISE NOTICE format('Dist: %s, %s, %s', ddate, period, distance);
				if distance = '100m' then			
				-- Number of observations
				
					variable := 'distance';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
						    avg(td."Distance")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."100m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
							
					variable := 'Feces_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Feces_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."100m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Feces_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Feces_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."100m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Urine_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."100m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Urine_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."100m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
							
				elseif distance = '50m' then

					variable := 'distance';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
						    avg(td."Distance")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."50m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
							
					variable := 'Feces_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Feces_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."50m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Feces_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Feces_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."50m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Urine_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."50m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Urine_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."50m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
				
				elseif distance = '25m' then

					variable := 'distance';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
						    avg(td."Distance")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."25m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
							
					variable := 'Feces_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Feces_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."25m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Feces_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Feces_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."25m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Urine_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."25m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Urine_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."25m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
								
				else

					variable := 'distance';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
						    avg(td."Distance")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."5m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
							
					variable := 'Feces_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Feces_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."5m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Feces_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Feces_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."5m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'average';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			avg(tc."Urine_kg_day")
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."5m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";

					variable := 'Urine_kg_day';
					functional := 'collections';
					area := format('%s', distance);
					
			    	insert into premodeling.toiletdensity ("ToiletID","Collection_Date","VARIABLE","FUNCTIONAL","AREA","PERIOD","OBSERVATIONS","VALUE")
			    	select 	td."ToiletID",
			    			ddate as "Collection_Date",
			    			variable,
			    			functional,
			    			area,
			    			period,
			    			count(td.*),
			    			count(tc."Urine_kg_day"<>0)
						from premodeling.toiletdistances td, premodeling.toiletcollection tc
							where (td."5m" != 0)
								and (td."ToiletID" in (select "ToiletID" from premodeling.toilethistory where ("StartCollection" < ddate)and("LastCollection">=ddate)))
								and (tc."Collection_Date">=ddate - (period)::interval)
								and (tc."Collection_Date"<ddate)
								and (td."ToiletID"=tc."ToiletID")
							group by td."ToiletID";
				
				end if;
			end LOOP;
		END LOOP;
	END LOOP;
END $$;
-- Bing, bang, boom
