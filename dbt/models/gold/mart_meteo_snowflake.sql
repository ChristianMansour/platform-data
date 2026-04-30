{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    date_meteo,
    tmax as temperature_max,
    tmin as temperature_min,
    precipitation,
    CASE 
        WHEN precipitation > 0 THEN TRUE 
        ELSE FALSE 
    END as is_rainy
from {{ ref('meteo_quotidien_snowflake') }}
