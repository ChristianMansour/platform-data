{{
  config(
    materialized='table',
    schema='gold'
  )
}}

select
    date_mutation,
    valeur_fonciere,
    surface_reelle_bati,
    nombre_pieces_principales,
    code_commune,
    type_local,
    nature_mutation
from {{ ref('dvf_mutations') }}
