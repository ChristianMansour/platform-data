{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    TRY_TO_DATE(date) as date_meteo,
    TRY_TO_DOUBLE(temperature_max) as tmax,
    TRY_TO_DOUBLE(temperature_min) as tmin,
    TRY_TO_DOUBLE(precipitation) as precipitation
from PLATFORM_DB.BRONZE.METEO_QUOTIDIEN
