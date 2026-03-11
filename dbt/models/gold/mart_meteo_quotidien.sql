{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    date_meteo,
    avg(tmax) as tmax_moyen,
    avg(tmin) as tmin_moyen,
    sum(precipitation) as precipitation_totale
from {{ ref('meteo_quotidien') }}
group by date_meteo
order by date_meteo
