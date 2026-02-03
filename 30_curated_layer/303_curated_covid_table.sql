USE DATABASE COVID_DEMO;
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE CURATED.COVID_DAILY_CLEAN (
    covid_location_key   STRING,
    fips                 STRING,
    admin2               STRING,
    province_state       STRING,
    country_region       STRING,
    last_update_ts       TIMESTAMP_NTZ,
    report_date          DATE,
    lat                  FLOAT,
    long_                FLOAT,
    confirmed            NUMBER,
    deaths               NUMBER,
    recovered            NUMBER,
    active               NUMBER,
    row_hash             STRING,
    source_file          STRING,
    ingested_at          TIMESTAMP_NTZ
);
