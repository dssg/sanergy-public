drop table if exists input.schedule_routes;

CREATE TABLE input.schedule_routes (
	"File" VARCHAR(14) NOT NULL, 
	"FLT name" VARCHAR(8) NOT NULL, 
	"FLT Location" VARCHAR(7), 
	"Responsible WC" VARCHAR(16), 
	"Crew lead" VARCHAR(15), 
	"Phone number" VARCHAR(12), 
	"Field Officer" VARCHAR(17), 
	"Franchise" VARCHAR(26), 
	"Route" VARCHAR(10), 
	"Sub-route number" VARCHAR(8), 
	"Route_sub-route" VARCHAR(17), 
	"Open?" VARCHAR(6), 
	"WTS toilet?" VARCHAR(5), 
	"Extra Containers" VARCHAR(5), 
	"Collection Time" VARCHAR(8), 
	"Type of  Collection" VARCHAR(47), 
	"Mon" VARCHAR(47), 
	"Tue" VARCHAR(47), 
	"Wed" VARCHAR(47), 
	"Thur" VARCHAR(47), 
	"Fri" VARCHAR(47), 
	"Sat" VARCHAR(47), 
	"Sun" VARCHAR(47), 
	_unnamed VARCHAR(32)
);

\copy input.schedule_routes from 'Sanergy/data/Route_Master_All_Dates.csv' with csv header;
