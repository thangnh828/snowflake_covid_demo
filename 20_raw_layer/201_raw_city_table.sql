USE DATABASE COVID_DEMO;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW.CITY_RAW (
    name         STRING,
    country      STRING,
    subcountry   STRING,
    geonameid    STRING,
    source_file  STRING,
    source_row   NUMBER,
    ingested_at  TIMESTAMP_NTZ
);

