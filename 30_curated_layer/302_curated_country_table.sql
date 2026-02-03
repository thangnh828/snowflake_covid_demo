USE DATABASE COVID_DEMO;
USE SCHEMA CURATED;

CREATE OR REPLACE TABLE CURATED.COUNTRY_CLEAN (
    country_key  STRING,
    country_name STRING,
    alpha_2      STRING,
    alpha_3      STRING,
    country_code STRING,
    iso_3166_2   STRING,
    region       STRING,
    sub_region   STRING,
    row_hash     STRING,
    source_file  STRING,
    ingested_at  TIMESTAMP_NTZ
);
