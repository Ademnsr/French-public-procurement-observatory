--Compare le montant initial (modification_id = 0) au montant de la derniere version connue
with initial as (
    select uid, montant as montant_initial, date_notification as date_initiale
    from {{ ref('stg_fact_contrat') }}
    where modification_id = 0 and montant_fiable
),

derniere_version as (
    select uid, montant as montant_final, modification_id as derniere_modification
    from (
        select
            uid,
            montant,
            modification_id,
            row_number() over (partition by uid order by modification_id desc) as rang
        from {{ ref('stg_fact_contrat') }}
        where montant_fiable
    ) t
    where rang = 1
)

select
    i.uid,
    i.date_initiale,
    i.montant_initial,
    d.montant_final,
    d.derniere_modification as nb_modifications,
    d.montant_final - i.montant_initial as ecart_montant,
    case
        when i.montant_initial > 0 then (d.montant_final - i.montant_initial) / i.montant_initial
        else null
    end as ecart_pct
from initial i
join derniere_version d on i.uid = d.uid
