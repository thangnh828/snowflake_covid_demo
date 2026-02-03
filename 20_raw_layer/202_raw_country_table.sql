USE DATABASE COVID_DEMO;
USE SCHEMA RAW;

CREATE OR REPLACE TABLE RAW.COUNTRY_RAW (
    name                     STRING,
    alpha_2                  STRING,
    alpha_3                  STRING,
    country_code             STRING,
    iso_3166_2               STRING,
    region                   STRING,
    sub_region               STRING,
    intermediate_region      STRING,
    region_code              STRING,
    sub_region_code          STRING,
    intermediate_region_code STRING,
    source_file              STRING,
    source_row               NUMBER,
    ingested_at              TIMESTAMP_NTZ
);
