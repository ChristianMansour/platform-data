{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    code_commune,
    nom_commune,
    count(*) as nb_mutations,
    avg(valeur_fonciere / nullif(surface_reelle_bati, 0)) as prix_m2_moyen
from {{ ref('dvf_mutations_snowflake') }}
where surface_reelle_bati > 0
group by code_commune, nom_commune
