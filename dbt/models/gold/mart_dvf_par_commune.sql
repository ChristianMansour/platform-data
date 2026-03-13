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
    round(avg(valeur_fonciere)::numeric, 2) as valeur_moyenne,
    round(avg(surface_reelle_bati)::numeric, 2) as surface_moyenne,
    round(
        avg(
            case 
                when surface_reelle_bati > 0 
                then valeur_fonciere / surface_reelle_bati 
                else null 
            end
        )::numeric, 2
    ) as prix_m2_moyen,
    min(date_mutation) as premiere_mutation,
    max(date_mutation) as derniere_mutation
from {{ ref('dvf_mutations') }}
where code_commune is not null
  and nom_commune is not null
group by code_commune, nom_commune
having count(*) >= 10
order by nb_mutations desc
