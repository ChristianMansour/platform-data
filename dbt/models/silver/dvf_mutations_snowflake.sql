{{
  config(
    materialized='table',
    schema='silver'
  )
}}

select
    TRY_TO_DATE(date_mutation) as date_mutation,
    nature_mutation,
    TRY_TO_NUMBER(valeur_fonciere, 12, 2) as valeur_fonciere,
    code_commune,
    nom_commune,
    type_local,
    coalesce(TRY_TO_NUMBER(surface_reelle_bati, 10, 2), 0) as surface_reelle_bati,
    coalesce(TRY_TO_NUMBER(nombre_pieces_principales), 0) as nombre_pieces_principales
from PLATFORM_DB.BRONZE.DVF_MUTATIONS
where valeur_fonciere is not null
  and TRY_TO_NUMBER(valeur_fonciere) > 0
