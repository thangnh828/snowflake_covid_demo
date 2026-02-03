USE DATABASE COVID_DEMO;
USE SCHEMA RAW;

COPY INTO RAW.CITY_RAW
FROM (
    SELECT
        $1 AS name,
        $2 AS country,
        $3 AS subcountry,
        $4 AS geonameid,
        METADATA$FILENAME AS source_file,
        METADATA$FILE_ROW_NUMBER AS source_row,
        CURRENT_TIMESTAMP() AS ingested_at
    FROM @STG_CITY_FILES/
)
FILE_FORMAT = (FORMAT_NAME = RAW.FF_CSV_DEFAULT)
ON_ERROR = 'CONTINUE';

