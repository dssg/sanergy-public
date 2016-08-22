drop table if exists output.model;
drop table if exists output.predictions;
drop table if exists output.evaluations;

CREATE TABLE output.model (
        "model_id" VARCHAR(50),
        "algorithm" VARCHAR(50),
        "hyperparameters" TEXT,
        "features" TEXT,
        "time_started" timestamp,
        "time_ended" timestamp,
        "batch_id" VARCHAR(50),
        "fold_id" INTEGER,
        "comment" TEXT
);

CREATE TABLE output.predictions (
        "model_id" VARCHAR(50),
        "fold_id" INTEGER,
        "ToiletID" VARCHAR(50),
	"Collection_Date" timestamp,
	"collect" smallint,
	"waste_type" VARCHAR(50),
	"comment" TEXT
);

CREATE TABLE output.evaluations (
        "model_id" VARCHAR(50),
	"fold_id" INTEGER,
	"metric" VARCHAR(50),
	"parameter" TEXT,
	"parameter_value" NUMERIC,
	"value" NUMERIC,
        "comment" TEXT
);

