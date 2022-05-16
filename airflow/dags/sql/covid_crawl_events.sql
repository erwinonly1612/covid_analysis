CREATE OR REPLACE TABLE {{ BIGQUERY_DATASET }}.{{ COVID_CRAWL_EVENTS_TABLE }}
PARTITION BY DATE(last_updated_date)
CLUSTER BY Country_Other, last_updated_date AS
SELECT 
    COALESCE(Country_Other, 'NA') AS Country_Other,
    COALESCE(TotalCases, 0) AS TotalCases,
    COALESCE(NewCases, 0) AS NewCases,
    COALESCE(TotalDeaths, 0) AS TotalDeaths,
    COALESCE(NewDeaths, 0) AS NewDeaths,
    COALESCE(TotalRecovered, 0) AS TotalRecovered,
    COALESCE(NewRecovered, 0) AS NewRecovered,
    COALESCE(ActiveCases, 0) AS ActiveCases,
    COALESCE(NewCases, 0)  + COALESCE(NewDeaths, 0) + COALESCE(NewRecovered, 0) AS NewUpdatedCases,
    LAG(COALESCE(NewCases, 0)  + COALESCE(NewDeaths, 0) + COALESCE(NewRecovered, 0)) OVER (PARTITION BY Country_Other ORDER BY last_updated_date) AS PreviousNewUpdatedCases,
    
    COALESCE(NewCases, 0)  + COALESCE(NewDeaths, 0) + COALESCE(NewRecovered, 0) -
    LAG(COALESCE(NewCases, 0)  + COALESCE(NewDeaths, 0) + COALESCE(NewRecovered, 0)) OVER (PARTITION BY Country_Other ORDER BY last_updated_date) AS ChangesInNewCases,
    
    last_updated_date,
    LAG(last_updated_date) OVER (PARTITION BY Country_Other ORDER BY last_updated_date) AS PreviousUpdatedDate,

    TIMESTAMP_DIFF(last_updated_date, LAG(last_updated_date) OVER (PARTITION BY Country_Other ORDER BY last_updated_date), MINUTE )  AS DataUpdateLatency,
    
    (CASE 
        WHEN COALESCE(NewCases, 0)  + COALESCE(NewDeaths, 0) + COALESCE(NewRecovered, 0) -
        LAG(COALESCE(NewCases, 0)  + COALESCE(NewDeaths, 0) + COALESCE(NewRecovered, 0)) 
        OVER (PARTITION BY Country_Other ORDER BY last_updated_date) = 0 THEN NULL
    ELSE TIMESTAMP_DIFF(last_updated_date, LAG(last_updated_date) OVER (PARTITION BY Country_Other ORDER BY last_updated_date), MINUTE )  
    END) AS DataUpdateLatencyReal



FROM {{ BIGQUERY_DATASET }}.{{ COVID_CRAWL_EXTERNAL_TABLE}}
WHERE Country_Other IS NOT NULL
AND Country_Other <> 'Total:'
