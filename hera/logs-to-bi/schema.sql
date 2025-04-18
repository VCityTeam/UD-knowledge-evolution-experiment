CREATE TABLE execution_logs (
    version integer,
    product integer,
    step integer,
    component text,
    duration_ms integer,
    time TIMESTAMP,
    query text,
    nb_try integer,
    component_name text GENERATED ALWAYS AS (
        REGEXP_REPLACE(
               component, 
               '-[0-9]+(-[0-9]+)*-service$',
               ''
       )
    ),
    PRIMARY KEY (version, product, step, component_name, query, nb_try)
)