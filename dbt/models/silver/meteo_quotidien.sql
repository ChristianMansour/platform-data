{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    date::date as date_meteo,
    coalesce(temperature_max::float, 0) as tmax,
    coalesce(temperature_min::float, 0) as tmin,
    coalesce(precipitation::float, 0) as precipitation
from {{ source('bronze', 'meteo_quotidien') }}
