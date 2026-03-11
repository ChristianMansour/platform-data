{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    date_mutation::date as date_mutation,
    nature_mutation,
    valeur_fonciere::numeric(12,2) as valeur_fonciere,
    code_commune,
    nom_commune,
    type_local,
    coalesce(surface_reelle_bati::numeric(10,2), 0) as surface_reelle_bati,
    coalesce(nombre_pieces_principales::integer, 0) as nombre_pieces_principales
from {{ source('bronze', 'dvf_mutations') }}
where valeur_fonciere is not null
  and valeur_fonciere::numeric > 0
