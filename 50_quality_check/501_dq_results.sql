USE DATABASE COVID_DEMO;
USE SCHEMA OPS;

CREATE OR REPLACE TABLE OPS.DQ_RESULTS (
    dq_run_id     STRING,
    dq_ts         TIMESTAMP_NTZ,
    check_name    STRING,
    status        STRING,
    failed_rows   NUMBER,
);
