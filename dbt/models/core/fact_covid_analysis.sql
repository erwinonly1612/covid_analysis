{{ config(materialized = 'table') }}

SELECT 
Country_Other,
ROUND(AVG(DataUpdateLatencyReal),0) AS AverageDataUpdateLatency
FROM {{ source('covid_crawl_stg', 'covid_crawl_events') }}
WHERE TIMESTAMP_DIFF(last_updated_date, CURRENT_TIMESTAMP(), MINUTE) <= 1440
AND Country_Other NOT IN {{ ref('country_exclusion') }}
GROUP BY Country_Other
ORDER BY AVG(DataUpdateLatencyReal) DESC

