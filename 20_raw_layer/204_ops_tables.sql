USE DATABASE COVID_DEMO;
USE SCHEMA OPS;

CREATE OR REPLACE TABLE OPS.LOAD_AUDIT (
    run_id        STRING,
    step          STRING,
    table_name    STRING,
    started_at    TIMESTAMP_NTZ,
    ended_at      TIMESTAMP_NTZ,
    rows_affected NUMBER
);
