--L'indice HHI est une somme de parts au carre (0-100), il doit rester entre 0 et 10000
select acheteur_id, hhi
from {{ ref('mart_buyer_concentration') }}
where hhi < 0 or hhi > 10000
