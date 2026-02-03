USE DATABASE COVID_DEMO;

USE SCHEMA MART;

-- Global daily summary
CREATE
OR REPLACE VIEW MART.VW_GOLD_GLOBAL_DAILY AS
SELECT
    report_date,
    SUM(confirmed) AS confirmed,
    SUM(deaths) AS deaths,
    SUM(recovered) AS recovered,
    SUM(active) AS active
FROM
    MART.FACT_COVID_DAILY
GROUP BY
    report_date;

-- Latest day per country
CREATE
OR REPLACE VIEW MART.VW_LATEST_BY_CITY AS WITH latest_date AS (
    SELECT
        MAX(report_date) AS report_date
    FROM
        MART.FACT_COVID_DAILY
)
SELECT
    f.city_name,
    f.report_date,
    SUM(f.confirmed) AS confirmed,
    SUM(f.deaths) AS deaths,
    SUM(f.recovered) AS recovered,
    SUM(f.active) AS active
FROM
    MART.FACT_COVID_DAILY f
    JOIN latest_date ld ON ld.report_date = f.report_date
GROUP BY
    f.city_name,
    f.report_date;

-- Latest day per country
CREATE
OR REPLACE VIEW MART.VW_LATEST_BY_COUNTRY AS WITH latest_date AS (
    SELECT
        MAX(report_date) AS report_date
    FROM
        MART.FACT_COVID_DAILY
)
SELECT
    f.country_name,
    f.report_date,
    SUM(f.confirmed) AS confirmed,
    SUM(f.deaths) AS deaths,
    SUM(f.recovered) AS recovered,
    SUM(f.active) AS active
FROM
    MART.FACT_COVID_DAILY f
    JOIN latest_date ld ON ld.report_date = f.report_date
GROUP BY
    f.country_name,
    f.report_date;

-- Daily new cases per country
CREATE
OR REPLACE VIEW MART.VW_DAILY_NEW_CASES_BY_COUNTRY AS WITH sum_confirmed AS (
    SELECT
        country_name,
        report_date,
        SUM(confirmed) AS confirmed
    FROM
        MART.FACT_COVID_DAILY
    GROUP BY
        country_name,
        report_date
)
SELECT
    country_name,
    report_date,
    confirmed,
    confirmed - LAG(confirmed) OVER (
        PARTITION BY country_name
        ORDER BY
            report_date
    ) AS new_confirmed
FROM
    sum_confirmed;



-- Latest day per region
CREATE
OR REPLACE VIEW MART.VW_LATEST_BY_REGION AS WITH latest_date AS (
    SELECT
        MAX(report_date) AS report_date
    FROM
        MART.FACT_COVID_DAILY
)
SELECT
    c.region,
    f.report_date,
    SUM(f.confirmed) AS confirmed,
    SUM(f.deaths) AS deaths,
    SUM(f.recovered) AS recovered,
    SUM(f.active) AS active
FROM
    MART.FACT_COVID_DAILY f
    JOIN MART.DIM_COUNTRY c ON c.country_name = f.country_name
    JOIN latest_date ld ON ld.report_date = f.report_date
GROUP BY
    c.region,
    f.report_date;