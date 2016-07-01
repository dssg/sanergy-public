drop table if exists input.toilet_coordinates;

CREATE TABLE input.toilet_coordinates (
	locationid TEXT NOT NULL,
location TEXT NOT NULL,
area TEXT NOT NULL,
subarea TEXT NOT NULL,
sflatitude FLOAT NOT NULL,
sflongitude FLOAT NOT NULL,
garminlatitude FLOAT NOT NULL,
garminlongitude FLOAT NOT NULL

);

\copy input.toilet_coordinates from 'data/input/ToiletCoords.csv' with csv header;

-- Add geometry
ALTER TABLE input.toilet_coordinates ADD COLUMN geom geometry(POINT,4326);
UPDATE input.toilet_coordinates SET geom = ST_MakePoint(sflongitude, sflatitude);
