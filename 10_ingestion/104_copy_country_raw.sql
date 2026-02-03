USE DATABASE COVID_DEMO;
USE SCHEMA RAW;

COPY INTO RAW.COUNTRY_RAW
FROM (
    SELECT
        $1  AS name,
        $2  AS alpha_2,
        $3  AS alpha_3,
        $4  AS country_code,
        $5  AS iso_3166_2,
        $6  AS region,
        $7  AS sub_region,
        $8  AS intermediate_region,
        $9  AS region_code,
        $10 AS sub_region_code,
        $11 AS intermediate_region_code,
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS source_row,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM @RAW.STG_COUNTRY_FILES/
)
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_DEFAULT)
ON_ERROR = 'CONTINUE';
