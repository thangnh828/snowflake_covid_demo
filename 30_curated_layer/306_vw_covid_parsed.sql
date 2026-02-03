-- Set working database and schema
USE DATABASE COVID_DEMO;
USE SCHEMA CURATED;

-- ============================================================
-- View: VW_COVID_PARSED
-- Purpose:
--   - Parse and standardize raw COVID-19 data
--   - Generate deterministic keys and hashes
--   - Deduplicate records and keep the latest version per location
--   - Expose a clean, analytics-ready dataset
-- ============================================================

CREATE OR REPLACE VIEW CURATED.VW_COVID_PARSED AS
WITH base AS (
    SELECT
        -- Deterministic key representing a COVID reporting location
        -- Used for deduplication and merge logic downstream
        SHA2(
            COALESCE(TRIM(fips), '') || '|' ||
            COALESCE(UPPER(TRIM(admin2)), '') || '|' ||
            COALESCE(UPPER(TRIM(province_state)), '') || '|' ||
            COALESCE(UPPER(TRIM(country_region)), '') || '|' ||
            COALESCE(last_update, ''),
            256
        ) AS covid_location_key,
        fips,
        admin2,
        province_state,
        country_region,
        last_update,
        lat,
        long_,
        confirmed,
        deaths,
        recovered,
        active,
        -- Robust timestamp parsing to handle multiple source formats
        COALESCE(
            TRY_TO_TIMESTAMP_NTZ(last_update),
            TRY_TO_TIMESTAMP_NTZ(last_update, 'MM/DD/YYYY HH24:MI'),
            TRY_TO_TIMESTAMP_NTZ(last_update, 'YYYY-MM-DD HH24:MI:SS')
        ) AS last_update_ts,
        TO_DATE(
            COALESCE(
                TRY_TO_TIMESTAMP_NTZ(last_update),
                TRY_TO_TIMESTAMP_NTZ(last_update, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_TIMESTAMP_NTZ(last_update, 'YYYY-MM-DD HH24:MI:SS')
            )
        ) AS report_date,
        -- Row-level hash used to detect data changes
        SHA2(
            COALESCE(TRIM(fips), '') || '|' ||
            COALESCE(TRIM(admin2), '') || '|' ||
            COALESCE(TRIM(province_state), '') || '|' ||
            COALESCE(TRIM(country_region), '') || '|' ||
            COALESCE(last_update, '') || '|' ||
            COALESCE(TO_VARCHAR(lat), '') || '|' ||
            COALESCE(TO_VARCHAR(long_), '') || '|' ||
            COALESCE(TO_VARCHAR(confirmed), '') || '|' ||
            COALESCE(TO_VARCHAR(deaths), '') || '|' ||
            COALESCE(TO_VARCHAR(recovered), '') || '|' ||
            COALESCE(TO_VARCHAR(active), ''),
            256
        ) AS row_hash,

        -- Lineage and audit metadata
        source_file,
        ingested_at

    FROM COVID_DEMO.RAW.COVID_DAILY_RAW

    -- Basic data quality filter
    WHERE country_region IS NOT NULL
      AND TRIM(country_region) <> ''
),
-- Deduplication CTE:
dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY covid_location_key
            ORDER BY ingested_at DESC, source_file DESC, row_hash DESC
        ) AS rn
    FROM base
)
-- Return one current record per COVID reporting location
SELECT
    covid_location_key,
    fips,
    admin2,
    province_state,
    country_region,
    last_update_ts,
    report_date,
    lat,
    long_,
    confirmed,
    deaths,
    recovered,
    active,
    row_hash,
    source_file,
    ingested_at
FROM dedup
WHERE rn = 1;
