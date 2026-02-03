USE DATABASE COVID_DEMO;
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE CURATED.CITY_CLEAN (
    city_key     STRING,
    city_name    STRING,
    country_name STRING,
    subcountry   STRING,
    geonameid    STRING,
    row_hash     STRING,
    source_file  STRING,
    ingested_at  TIMESTAMP_NTZ
);
