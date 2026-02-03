# Snowflake-only COVID ingestion project (City + Country datasets included)

## Data folders in stage
Use stages with prefixes:
- @STG_COVID_FILES
- @STG_CITY_FILES
- @STG_COUNTRY_FILES

## Layers
- BRONZE (RAW): append-only, keep source columns + file metadata
- SILVER (CURATED): parsed/typed/standardized
- GOLD (MART): DIM_COUNTRY, DIM_CITY, FACT_COVID_DAILY, dashboard views

