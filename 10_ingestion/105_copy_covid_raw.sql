USE DATABASE COVID_DEMO;
USE SCHEMA RAW;

COPY INTO RAW.COVID_DAILY_RAW
FROM (
    SELECT
        $1  AS fips,
        $2  AS admin2,
        $3  AS province_state,
        $4  AS country_region,
        $5  AS last_update,
        $6  AS lat,
        $7  AS long_,
        $8  AS confirmed,
        $9  AS deaths,
        $10 AS recovered,
        $11 AS active,
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS source_row,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM @RAW.STG_COVID_FILES/
)
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_DEFAULT)
ON_ERROR = 'CONTINUE';
