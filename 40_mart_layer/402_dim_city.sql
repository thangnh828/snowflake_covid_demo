USE DATABASE COVID_DEMO;
USE SCHEMA MART;

CREATE OR REPLACE TABLE MART.DIM_CITY (
    city_id      STRING,
    city_name    STRING,
    subcountry   STRING,
    country_id   STRING,
    geonameid    STRING,
    row_hash     STRING,
    created_at   TIMESTAMP_NTZ,
    updated_at   TIMESTAMP_NTZ
);
