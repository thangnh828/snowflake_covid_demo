USE DATABASE COVID_DEMO;
USE SCHEMA MART;

CREATE OR REPLACE TABLE MART.DIM_COUNTRY (
    country_id   STRING,
    country_name STRING,
    alpha_2      STRING,
    alpha_3      STRING,
    country_code STRING,
    region       STRING,
    sub_region   STRING,
    row_hash     STRING,
    created_at   TIMESTAMP_NTZ,
    updated_at   TIMESTAMP_NTZ
);
