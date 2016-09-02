drop table if exists input.Collection_Schedule_Wheelbarrow;
drop table if exists input.Collection_Schedule_Truck;
drop table if exists input.Collection_Schedule_Tuktuk;
drop table if exists input.Collection_Schedule_School;
drop table if exists input.new_geo_data;
drop table if exists input.toilet_cases;

CREATE TABLE input.toilet_cases (
        "Case Owner" VARCHAR(22) NOT NULL, 
        "Account Name" VARCHAR(43), 
        "Toilet" VARCHAR(8) NOT NULL, 
        "Subject" VARCHAR(99), 
        "Date/Time Opened" TEXT NOT NULL, 
        "Age (Hours)" FLOAT NOT NULL, 
        "Open" INTEGER NOT NULL, 
        "Closed" INTEGER NOT NULL, 
        "Area" VARCHAR(13) NOT NULL
);

CREATE TABLE input.new_geo_data (
        "Date" DATE NOT NULL, 
        "Location ID" VARCHAR(18) NOT NULL, 
        "Location Name" VARCHAR(6) NOT NULL, 
        "GPS_LATITUDE__C" FLOAT NOT NULL, 
        "GPS_LONGITUDE__C" FLOAT NOT NULL
);


CREATE TABLE input.Collection_Schedule_Wheelbarrow (
	flt_name TEXT,
	"flt-location" TEXT,
	responsible_wc TEXT,
	crew_lead TEXT,
	phone_number BIGINT,
	field_officer TEXT,
	franchise_type TEXT,
	route_name TEXT,
	"sub-route_number" TEXT,
	"route_sub-route" TEXT,
	"open?" TEXT,
	"wts_toilet?" BOOLEAN,
	extra_containers INTEGER,
	collection_time TEXT,
	type_of__collection TEXT,
	mon TEXT,
	tue TEXT,
	wed TEXT,
	thur TEXT,
	fri TEXT,
	sat TEXT,
	sun TEXT
);


CREATE TABLE input.Collection_Schedule_Truck (
	flt_name TEXT,
	"flt-location" TEXT,
	responsible_wc TEXT,
	crew_lead TEXT,
	phone_number BIGINT,
	field_officer TEXT,
	franchise_type TEXT,
	route_name TEXT,
	"sub-route_number" INTEGER,
	"route_sub-route" TEXT,
	open TEXT,
	"wts?" BOOLEAN,
	truck_optimisation INTEGER,
	"extra_container?" TEXT,
	collection_time TEXT,
	periodic_collection_collection TEXT,
	mon TEXT,
	tue TEXT,
	wed TEXT,
	thur TEXT,
	fri TEXT,
	sat TEXT,
	sun TEXT,
	comments TEXT
);

CREATE TABLE input.Collection_Schedule_Tuktuk (
	flt_name TEXT,
	"flt-location" TEXT,
	responsible_wc TEXT,
	crew_lead TEXT,
	phone_number BIGINT,
	field_officer TEXT,
	franchise_type TEXT,
	route_name TEXT,
	"sub-route_number" INTEGER,
	"route_sub-route" TEXT,
	"open?" TEXT,
	"wts?" BOOLEAN,
	extra_containers INTEGER,
	collection_time TEXT,
	periodic_collection_collection TEXT,
	mon TEXT,
	tue TEXT,
	wed TEXT,
	thur TEXT,
	fri TEXT,
	sat TEXT,
	sun TEXT
);

CREATE TABLE input.Collection_Schedule_School (
	toilet_name VARCHAR(8), 
	location_name VARCHAR(7), 
	launch_date VARCHAR(10), 
	franchise_type VARCHAR(26), 
	flo_name VARCHAR(69), 
	area VARCHAR(13), 
	sub_area VARCHAR(18), 
	operational_status VARCHAR(6), 
	field_officer VARCHAR(17), 
	senior_field_officer VARCHAR(16), 
	"ipa_toilet?" VARCHAR(32), 
	"wts_toilet?" VARCHAR(10), 
	sub_route VARCHAR(12), 
	days_since_location_opened VARCHAR(5), 
	percentage_of_days_open VARCHAR(5), 
	price VARCHAR(4), 
	if_plot_number_of_houses_in_plot VARCHAR(4), 
	if_hybrid_plot_number_of_hh_around_plot VARCHAR(4), 
	if_school_number_of_non_flt_toilets VARCHAR(4), 
	if_school_total_student_popl VARCHAR(4), 
	"if_school,_pop_with_access_to_flt" VARCHAR(4), 
	if_non_commercial_potential_user_population VARCHAR(4), 
	"if_non_commercial_%%_of_potential_pop_using_flt" VARCHAR(7), 
	"who_owns_the_toilet?" VARCHAR(20), 
	"who_operates_or_manages_the_flt_on_a_daily_basis?" VARCHAR(24) 
);

\copy input.toilet_cases from 'Sanergy/data/AllCasesAreaToiletID.csv' with csv header;
\copy input.Collection_Schedule_Wheelbarrow from 'Sanergy/data/wheelCut.csv' with csv header;
\copy input.Collection_Schedule_Truck from 'Sanergy/data/truckCut.csv' with csv header;
\copy input.Collection_Schedule_Tuktuk from 'Sanergy/data/tukCut.csv' with csv header;
\copy input.Collection_Schedule_School from 'Sanergy/data/school_by_route.csv' with csv header;
\copy input.new_geo_data from 'Sanergy/data/new_geo_data.csv' with csv header;
