--Renommage et securite : row_number() garantit une ligne max par (uid, modification_id)
--meme si aucun doublon reel n'existe actuellement sur les 2 262 196 lignes
--bind=false : une vue Redshift classique ne peut pas lire une table externe Spectrum,
--il faut une "late-binding view" (WITH NO SCHEMA BINDING)
{{ config(bind=False) }}

with source as (
    select * from {{ source('gold', 'fact_contrat') }}
),

dedup as (
    select
        *,
        row_number() over (
            partition by uid, modification_id
            order by ingestion_timestamp desc
        ) as ligne
    from source
)

select
    uid,
    id as marche_id,
    nature,
    acheteur_id,
    acheteur_nom,
    acheteur_denomination_sirene,
    acheteur_commune_code,
    acheteur_commune_nom,
    acheteur_departement_code,
    acheteur_departement_nom,
    acheteur_region_code,
    acheteur_region_nom,
    acheteur_categorie,
    acheteur_activite_code,
    acheteur_categorie_juridique,
    acheteur_rapproche_sirene,
    titulaire_id,
    titulaire_nom,
    titulaire_denomination_sirene,
    titulaire_commune_code,
    titulaire_commune_nom,
    titulaire_departement_code,
    titulaire_departement_nom,
    titulaire_region_code,
    titulaire_region_nom,
    titulaire_categorie,
    titulaire_activite_code,
    titulaire_categorie_juridique,
    titulaire_rapproche_sirene,
    objet,
    montant,
    montant_rationalise,
    montant_anomalie,
    type as type_marche,
    codecpv as code_cpv,
    procedure,
    datenotification as date_notification,
    datepublicationdonnees as date_publication,
    dureemois as duree_mois,
    offresrecues as offres_recues,
    modification_id,
    est_modification,
    ingestion_timestamp,
    extract_date
from dedup
where ligne = 1
