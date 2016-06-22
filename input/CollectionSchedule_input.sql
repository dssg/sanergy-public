drop table if exists input.Collection_Schedule_Wheelbarrow;
drop table if exists input.Collection_Schedule_Truck;
drop table if exists input.Collection_Schedule_Tuktuk;


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

\copy input.Collection_Schedule_Wheelbarrow from 'data/input/wheelCut.csv' with csv header;
\copy input.Collection_Schedule_Truck from 'data/input/truckCut.csv' with csv header;
\copy input.Collection_Schedule_Tuktuk from 'data/input/tukCut.csv' with csv header;
