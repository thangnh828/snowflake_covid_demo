USE DATABASE COVID_DEMO;
USE SCHEMA CURATED;

CREATE OR REPLACE VIEW CURATED.VW_COUNTRY_PARSED AS
WITH base AS (
    SELECT
        -- business key: prefer alpha_2 if present, otherwise fall back to name
        SHA2(
            COALESCE(TRIM(alpha_2), TRIM(name), ''),
            256
        ) AS country_key,

        TRIM(name) AS country_name,
        TRIM(alpha_2) AS alpha_2,
        TRIM(alpha_3) AS alpha_3,
        TRIM(country_code) AS country_code,
        TRIM(iso_3166_2) AS iso_3166_2,
        TRIM(region) AS region,
        TRIM(sub_region) AS sub_region,
        TRIM(intermediate_region) AS intermediate_region,
        TRIM(region_code) AS region_code,
        TRIM(sub_region_code) AS sub_region_code,
        SHA2(
            COALESCE(TRIM(name), '') || '|' ||
            COALESCE(TRIM(alpha_2), '') || '|' ||
            COALESCE(TRIM(alpha_3), '') || '|' ||
            COALESCE(TRIM(country_code), '') || '|' ||
            COALESCE(TRIM(iso_3166_2), '') || '|' ||
            COALESCE(TRIM(region), '') || '|' ||
            COALESCE(TRIM(sub_region), '') || '|' ||
            COALESCE(TRIM(intermediate_region), '') || '|' ||
            COALESCE(TRIM(region_code), '') || '|' ||
            COALESCE(TRIM(sub_region_code), ''),
            256
        ) AS row_hash,

        source_file,
        ingested_at
    FROM COVID_DEMO.RAW.COUNTRY_RAW
    WHERE name IS NOT NULL
      AND TRIM(name) <> ''
),
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY country_key
            ORDER BY ingested_at DESC, source_file DESC, row_hash DESC
        ) AS rn
    FROM base
)
SELECT
    country_key,
    country_name,
    alpha_2,
    alpha_3,
    country_code,
    iso_3166_2,
    region,
    sub_region,
    row_hash,
    source_file,
    ingested_at
FROM dedup
WHERE rn = 1;
