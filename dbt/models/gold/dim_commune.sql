{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select distinct
    code_commune as commune_id,
    code_commune,
    nom_commune
from {{ ref('dvf_mutations') }}
where code_commune is not null
order by code_commune
