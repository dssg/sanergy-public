drop table if exists output.model;
drop table if exists output.predictions;
drop table if exists output.evaluations;

CREATE TABLE output.model (
        "model_id" VARCHAR(50) NOT NULL,
        "algorithm" VARCHAR(50),
        "hyperparameters" TEXT NOT NULL,
        "features" TEXT NOT NULL,
        "time_started" timestamp NOT NULL,
        "time_ended" timestamp NOT NULL,
        "batch_id" VARCHAR(50) NOT NULL,
        "fold_id" INTEGER NOT NULL,
        "comment" TEXT NOT NULL
);

CREATE TABLE output.predictions (
        "model_id" VARCHAR(50) NOT NULL,
        "fold_id" INTEGER NOT NULL,
        "ToiletID" VARCHAR(50) NOT NULL,
	"Collection_Date" timestamp NOT NULL,
	"predicted" smallint NOT NULL,
	"observed" smallint NOT NULL,
	"comment" TEXT NOT NULL
);

CREATE TABLE output.evaluations (
        "model_id" VARCHAR(50) NOT NULL,
	"fold_id" INTEGER NOT NULL,
	"metric" VARCHAR(50) NOT NULL,
	"parameter" TEXT NOT NULL,
	"parameter_value" NUMERIC NOT NULL,
	"value" NUMERIC NOT NULL,
        "comment" TEXT NOT NULL
);

