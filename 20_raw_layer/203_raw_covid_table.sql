USE DATABASE COVID_DEMO;
USE SCHEMA RAW;

-- Adjust columns if your COVID CSV has different order/fields.
CREATE OR REPLACE TABLE RAW.COVID_DAILY_RAW (
    fips           STRING,
    admin2         STRING,
    province_state STRING,
    country_region STRING,
    last_update    STRING,
    lat            STRING,
    long_          STRING,
    confirmed      STRING,
    deaths         STRING,
    recovered      STRING,
    active         STRING,
    source_file    STRING,
    source_row     NUMBER,
    ingested_at    TIMESTAMP_NTZ
);
