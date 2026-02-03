USE DATABASE COVID_DEMO;
USE SCHEMA OPS;

CREATE OR REPLACE PROCEDURE OPS.SP_BUILD_SILVER()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    -- Unique identifier for the current execution run
    v_run_id STRING DEFAULT UUID_STRING();
    -- Timestamp marking the start time of the execution
    v_start_ts TIMESTAMP_NTZ;
    -- Timestamp marking the end time of the execution
    v_end_ts TIMESTAMP_NTZ;
    -- Number of rows processed during the execution
    v_rows NUMBER DEFAULT 0;
BEGIN
    -- Log start
    v_start_ts := CURRENT_TIMESTAMP();
    -- CITY_CLEAN: insert only new row_hash
    INSERT INTO COVID_DEMO.CURATED.CITY_CLEAN
    SELECT *
    FROM COVID_DEMO.CURATED.VW_CITY_PARSED v
    WHERE NOT EXISTS (
        SELECT 1 FROM COVID_DEMO.CURATED.CITY_CLEAN c WHERE c.row_hash = v.row_hash
    );
    v_rows := SQLROWCOUNT;
    -- Log end
    v_end_ts := CURRENT_TIMESTAMP();

    INSERT INTO OPS.LOAD_AUDIT(run_id, step, table_name, started_at, ended_at, rows_affected)
    VALUES (:v_run_id, 'BUILD_SILVER', 'CITY_CLEAN', :v_start_ts, :v_end_ts, :v_rows);

    -- Log start
    v_start_ts := CURRENT_TIMESTAMP();
    -- COUNTRY_CLEAN
    INSERT INTO COVID_DEMO.CURATED.COUNTRY_CLEAN
    SELECT *
    FROM COVID_DEMO.CURATED.VW_COUNTRY_PARSED v
    WHERE NOT EXISTS (
        SELECT 1 FROM COVID_DEMO.CURATED.COUNTRY_CLEAN c WHERE c.row_hash = v.row_hash
    );
    v_rows := SQLROWCOUNT;
    -- Log end
    v_end_ts := CURRENT_TIMESTAMP();

    INSERT INTO OPS.LOAD_AUDIT(run_id, step, table_name, started_at, ended_at, rows_affected)
    VALUES (:v_run_id, 'BUILD_SILVER', 'COUNTRY_CLEAN', :v_start_ts, :v_end_ts, :v_rows);

    -- Log start
    v_start_ts := CURRENT_TIMESTAMP();
    -- COVID_DAILY_CLEAN
    INSERT INTO COVID_DEMO.CURATED.COVID_DAILY_CLEAN
    SELECT *
    FROM COVID_DEMO.CURATED.VW_COVID_PARSED v
    WHERE v.report_date IS NOT NULL
        AND NOT EXISTS (
        SELECT 1 FROM COVID_DEMO.CURATED.COVID_DAILY_CLEAN c WHERE c.row_hash = v.row_hash
        );
    v_rows := SQLROWCOUNT;
    -- Log end
    v_end_ts := CURRENT_TIMESTAMP();

    INSERT INTO OPS.LOAD_AUDIT(run_id, step, table_name, started_at, ended_at, rows_affected)
    VALUES (:v_run_id, 'BUILD_SILVER', 'COVID_DAILY_CLEAN', :v_start_ts, :v_end_ts, :v_rows);

    RETURN OBJECT_CONSTRUCT('run_id', :v_run_id, 'status', 'COMPLETED');
END;
$$;
