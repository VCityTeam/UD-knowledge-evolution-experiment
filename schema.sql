-- Description: This file contains the schema of the database that will be used to store the experiments results.
DROP TABLE IF EXISTS run;
DROP TABLE IF EXISTS experiment;

-- Experiment: Stores the parameters of the experiment, and the start and end timestamp of the experiment.
-- Create experiment table
CREATE TABLE experiment (
    -- this is the id of the experiment
    id SERIAL PRIMARY KEY,
    -- this is the parameters of the experiment
    parameters jsonb,
    -- this is the start timestamp of the experiment
    start_date timestamp,
    -- this is the end timestamp of the experiment
    end_date timestamp
);

-- Stores experimental results, and their associated parameters to analyze the data.
-- Create run table
CREATE TABLE run (
    -- this is the id of the run
    id SERIAL PRIMARY KEY,
    -- this is the experiment id
    experiment_id INTEGER REFERENCES experiment(id),
    -- this is the parameters of the run
    parameters jsonb,
    -- this is the metrics of the run
    metrics jsonb
);
