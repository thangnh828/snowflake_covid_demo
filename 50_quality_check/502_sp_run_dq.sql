USE DATABASE COVID_DEMO;
USE SCHEMA OPS;

CREATE OR REPLACE PROCEDURE OPS.SP_RUN_DQ()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
    v_id STRING DEFAULT UUID_STRING();
    v_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_failed NUMBER;
BEGIN
    -- Fact date not null
    SELECT COUNT(*) INTO :v_failed
    FROM COVID_DEMO.CURATED.COVID_DAILY_CLEAN
    WHERE report_date IS NULL;

    INSERT INTO OPS.DQ_RESULTS VALUES (:v_id, :v_ts, 'curated_report_date_not_null',
        IFF(:v_failed=0,'PASS','FAIL'), :v_failed, NULL);

    -- No negative values
    SELECT COUNT(*) INTO :v_failed
    FROM COVID_DEMO.CURATED.COVID_DAILY_CLEAN
    WHERE COALESCE(confirmed,0) < 0 OR COALESCE(deaths,0) < 0 OR COALESCE(recovered,0) < 0 OR COALESCE(active,0) < 0;

    INSERT INTO OPS.DQ_RESULTS VALUES (:v_id, :v_ts, 'curated_no_negative_values',
        IFF(:v_failed=0,'PASS','FAIL'), :v_failed, NULL);

    RETURN OBJECT_CONSTRUCT('dq_run_id', :v_id, 'dq_ts', :v_ts);
END;
$$;
