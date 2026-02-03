USE DATABASE COVID_DEMO;
USE SCHEMA MART;

CREATE OR REPLACE TABLE MART.FACT_COVID_DAILY (
    covid_location_key   STRING,
    city_name            STRING,
    country_name         STRING,
    report_date          DATE,
    confirmed            NUMBER,
    deaths               NUMBER,
    recovered            NUMBER,
    active               NUMBER,
    row_hash             STRING,
    last_update_ts       TIMESTAMP_NTZ,
    source_file          STRING,
    created_at           TIMESTAMP_NTZ,
    updated_at           TIMESTAMP_NTZ
);
