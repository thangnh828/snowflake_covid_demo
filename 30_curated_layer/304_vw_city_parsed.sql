USE DATABASE COVID_DEMO;
USE SCHEMA CURATED;

CREATE OR REPLACE VIEW CURATED.VW_CITY_PARSED AS
WITH base AS (
    SELECT
        SHA2(
            COALESCE(UPPER(TRIM(name)), '') || '|' || COALESCE(UPPER(TRIM(country)), '') || '|' || COALESCE(UPPER(TRIM(subcountry)), ''),
            256
        ) AS city_key,
        TRIM(name) AS city_name,
        TRIM(country) AS country_name,
        TRIM(subcountry) AS subcountry,
        TRIM(geonameid) AS geonameid,
        SHA2(
            COALESCE(TRIM(name), '') || '|' ||
            COALESCE(TRIM(country), '') || '|' ||
            COALESCE(TRIM(subcountry), '') || '|' ||
            COALESCE(TRIM(geonameid), ''),
            256
        ) AS row_hash,
        source_file,
        ingested_at
    FROM COVID_DEMO.RAW.CITY_RAW
    WHERE name IS NOT NULL
    AND TRIM(name) <> ''
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY city_key
            ORDER BY ingested_at DESC, source_file DESC, row_hash DESC
        ) AS rn
    FROM base
)
SELECT
    city_key,
    city_name,
    country_name,
    subcountry,
    geonameid,
    row_hash,
    source_file,
    ingested_at
FROM dedup
WHERE rn = 1;
