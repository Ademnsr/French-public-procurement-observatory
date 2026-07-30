--Montant et nombre de contrats par couple acheteur/fournisseur, par mois de notification
select
    date_trunc('month', date_notification) as mois,
    acheteur_id,
    acheteur_nom,
    titulaire_id,
    titulaire_nom,
    count(*) as nb_contrats,
    sum(montant) as montant_total
from {{ ref('stg_fact_contrat') }}
where date_notification is not null and montant_fiable
group by 1, 2, 3, 4, 5
