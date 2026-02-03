USE DATABASE COVID_DEMO;
USE SCHEMA OPS;

CREATE OR REPLACE PROCEDURE OPS.SP_BUILD_GOLD()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_run_id STRING DEFAULT UUID_STRING();
    v_start_ts TIMESTAMP_NTZ;
    v_end_ts TIMESTAMP_NTZ;
    v_rows NUMBER DEFAULT 0;
BEGIN
    -- Log start
    v_start_ts := CURRENT_TIMESTAMP();
    -- DIM_COUNTRY from curated country dataset (authoritative)
    MERGE INTO COVID_DEMO.MART.DIM_COUNTRY t
    USING (
        SELECT DISTINCT
        country_key AS country_id,
        country_name,
        alpha_2,
        alpha_3,
        country_code,
        region,
        sub_region,
        row_hash
        FROM COVID_DEMO.CURATED.COUNTRY_CLEAN
        WHERE DATE(ingested_at) = CURRENT_DATE()
    ) s
    ON t.country_id = s.country_id
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN
        UPDATE SET
        country_name = s.country_name,
        alpha_2 = s.alpha_2,
        alpha_3 = s.alpha_3,
        country_code = s.country_code,
        region = s.region,
        sub_region = s.sub_region,
        row_hash = s.row_hash,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (country_id, country_name, alpha_2, alpha_3, country_code, region, sub_region, row_hash, created_at, updated_at)
        VALUES (s.country_id, s.country_name, s.alpha_2, s.alpha_3, s.country_code, s.region, s.sub_region, s.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
    v_rows := SQLROWCOUNT;
    -- Log end
    v_end_ts := CURRENT_TIMESTAMP();

    INSERT INTO OPS.LOAD_AUDIT(run_id, step, table_name, started_at, ended_at, rows_affected)
    VALUES (:v_run_id, 'BUILD_GOLD', 'DIM_COUNTRY', :v_start_ts, :v_end_ts, :v_rows);

    -- Log start
    v_start_ts := CURRENT_TIMESTAMP();
    -- DIM_CITY from curated city dataset (authoritative)
    MERGE INTO COVID_DEMO.MART.DIM_CITY t
    USING (
        SELECT DISTINCT
        city_key AS city_id,
        city_name,
        subcountry,
        SHA2(UPPER(TRIM(country_name)), 256) AS country_id,
        geonameid,
        row_hash
        FROM COVID_DEMO.CURATED.CITY_CLEAN
        WHERE DATE(ingested_at) = CURRENT_DATE()
    ) s
    ON t.city_id = s.city_id
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN
        UPDATE SET
        city_name = s.city_name,
        subcountry = s.subcountry,
        country_id = s.country_id,
        geonameid = s.geonameid,
        row_hash = s.row_hash,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (city_id, city_name, subcountry, country_id, geonameid, row_hash, created_at, updated_at)
        VALUES (s.city_id, s.city_name, s.subcountry, s.country_id, s.geonameid, s.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
    v_rows := SQLROWCOUNT;
    -- Log end
    v_end_ts := CURRENT_TIMESTAMP();

    INSERT INTO OPS.LOAD_AUDIT(run_id, step, table_name, started_at, ended_at, rows_affected)
    VALUES (:v_run_id, 'BUILD_GOLD', 'DIM_CITY', :v_start_ts, :v_end_ts, :v_rows);

    -- Log start
    v_start_ts := CURRENT_TIMESTAMP();
    -- FACT from curated covid dataset (dedup latest per location/date)
    MERGE INTO COVID_DEMO.MART.FACT_COVID_DAILY t
    USING (
        SELECT
        covid_location_key,
        report_date,
        confirmed,
        province_state as city_name,
        country_region as country_name,
        deaths,
        recovered,
        active,
        last_update_ts,
        source_file,
        row_hash
        FROM COVID_DEMO.CURATED.COVID_DAILY_CLEAN
        WHERE DATE(ingested_at) = CURRENT_DATE()
    ) s
    ON t.covid_location_key = s.covid_location_key
    WHEN MATCHED AND t.row_hash <> s.row_hash THEN
        UPDATE SET
        confirmed = s.confirmed,
        deaths = s.deaths,
        recovered = s.recovered,
        active = s.active,
        last_update_ts = s.last_update_ts,
        source_file = s.source_file,
        updated_at = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (covid_location_key, report_date, city_name, country_name, confirmed, deaths, recovered, active, last_update_ts, source_file, row_hash, created_at, updated_at)
        VALUES (s.covid_location_key, s.report_date, s.city_name, s.country_name, s.confirmed, s.deaths, s.recovered, s.active, s.last_update_ts, s.source_file, s.row_hash, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP());
    v_rows := SQLROWCOUNT;
    -- Log end
    v_end_ts := CURRENT_TIMESTAMP();

    INSERT INTO OPS.LOAD_AUDIT(run_id, step, table_name, started_at, ended_at, rows_affected)
    VALUES (:v_run_id, 'BUILD_GOLD', 'FACT_COVID_DAILY', :v_start_ts, :v_end_ts, :v_rows);

    RETURN OBJECT_CONSTRUCT('run_id', :v_run_id, 'status', 'COMPLETED');
END;
$$;
