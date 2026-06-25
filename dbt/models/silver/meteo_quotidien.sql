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
    coalesce(precipitation::float, 0) as precipitation,
    case
        when coalesce(precipitation::float, 0) > 0 then true
        else false
    end as a_plu
from {{ source('bronze', 'meteo_quotidien') }}
