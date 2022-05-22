{{ config(materialized = 'table') }}

SELECT 
Country_Other,
AVG(DataUpdateLatencyReal) AS AverageDataUpdateLatency
FROM {{ source('staging', 'covid_crawl_events') }}
WHERE TIMESTAMP_DIFF(last_updated_date, CURRENT_TIMESTAMP(), MINUTE) <= 1440
AND Country_Other NOT IN (SELECT * FROM {{ ref('country_exclusion') }} )
GROUP BY Country_Other
ORDER BY AVG(DataUpdateLatencyReal) DESC