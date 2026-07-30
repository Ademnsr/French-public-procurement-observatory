--Indice de concentration HHI par acheteur : somme des parts de marche au carre (0-10000)
--Plus l'indice est eleve, plus l'acheteur depend d'un petit nombre de fournisseurs
with contrats_valides as (
    select acheteur_id, acheteur_nom, titulaire_id, montant
    from {{ ref('stg_fact_contrat') }}
    where montant is not null and montant > 0 and titulaire_id is not null and montant_fiable
),

par_acheteur_fournisseur as (
    select
        acheteur_id,
        acheteur_nom,
        titulaire_id,
        sum(montant) as montant_fournisseur
    from contrats_valides
    group by acheteur_id, acheteur_nom, titulaire_id
),

montant_total_acheteur as (
    select acheteur_id, sum(montant_fournisseur) as montant_total
    from par_acheteur_fournisseur
    group by acheteur_id
),

parts as (
    select
        p.acheteur_id,
        p.acheteur_nom,
        p.titulaire_id,
        p.montant_fournisseur,
        t.montant_total,
        (p.montant_fournisseur / t.montant_total) * 100 as part_pct
    from par_acheteur_fournisseur p
    join montant_total_acheteur t on p.acheteur_id = t.acheteur_id
)

select
    acheteur_id,
    max(acheteur_nom) as acheteur_nom,
    count(distinct titulaire_id) as nb_fournisseurs,
    sum(montant_fournisseur) as montant_total,
    round(sum(power(part_pct, 2)), 4) as hhi,
    max(part_pct) as part_top_fournisseur_pct
from parts
group by acheteur_id
