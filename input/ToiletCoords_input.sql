drop table if exists input.toilet_coordinates;

CREATE TABLE input.toilet_coordinates (
	locationid TEXT,
	location TEXT,
	area TEXT,
	subarea TEXT,
	sflatitude FLOAT,
	sflongitude FLOAT,
	garminlatitude FLOAT,
	garminlongitude FLOAT
);

\copy input.toilet_coordinates from 'Sanergy/data/ToiletCoords.csv' with csv header;

-- Add geometry
ALTER TABLE input.toilet_coordinates ADD COLUMN geom geometry(POINT,4326);
UPDATE input.toilet_coordinates SET geom = ST_SetSRID(ST_MakePoint(sflongitude, sflatitude),4326)
