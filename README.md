# Observatoire des marchés publics français

Pipeline de données qui rapproche les marchés publics français (DECP) du répertoire des entreprises (Sirene), pour mesurer la concentration des dépenses publiques par acheteur.

La question de départ : quels acheteurs publics dépendent d'un très petit nombre de fournisseurs ? Le projet ne cherche pas à détecter de la fraude, les données seules ne le permettent pas. Il produit des indicateurs de concentration et signale des anomalies techniques.

## Le pipeline

Les fichiers sources sont téléchargés depuis data.gouv.fr par une fonction Lambda et déposés dans un bucket S3, couche bronze. Deux jobs AWS Glue les nettoient et les écrivent en Parquet dans la couche silver, un troisième joint le DECP à Sirene et produit la couche gold. Un crawler catalogue le résultat, Redshift l'interroge via Spectrum, et dbt construit les tables métier par dessus. QuickSight se branche sur ces tables pour la restitution. EventBridge planifie l'ingestion, un workflow Glue enchaîne les jobs silver puis gold.

## Stack technique

| Couche | Outil |
|---|---|
| Ingestion | AWS Lambda, Python 3.12 |
| Data lake | Amazon S3 (Parquet partitionné) |
| Traitement distribué | AWS Glue 4.0, PySpark |
| Catalogue | AWS Glue Data Catalog |
| Entrepôt | Amazon Redshift Serverless + Spectrum |
| Transformation | dbt (dbt-redshift) |
| Restitution | Amazon QuickSight |
| Orchestration | EventBridge, Glue Workflow |
| Infrastructure | CloudFormation |

## Sources et volumétrie

Le DECP consolidé au format tabulaire, publié sur data.gouv.fr et mis à jour quotidiennement, contient les marchés attribués avec leurs acheteurs, titulaires, montants et modifications. La base Sirene de l'Insee apporte l'activité NAF, la commune et la catégorie juridique de chaque entreprise.

| Fichier | Taille | Lignes |
|---|---|---|
| decp.csv | 2,47 Go | 3 286 474 |
| unite_legale.parquet | 0,70 Go | 29,8 millions |
| etablissement.parquet | 2,19 Go | 43,7 millions |

Le DECP est republié quotidiennement, Sirene environ une fois par mois. Chaque exécution repart du même instantané : elle relit les 3,3 millions de lignes du DECP, puis effectue deux jointures par SIRET, celle de l'acheteur et celle du titulaire, contre une dimension Sirene de 43,7 millions d'établissements.

Ce volume tient sur une machine unique. Une fois les dix colonnes utiles sélectionnées, la table silver des établissements pèse 0,80 Go en Parquet, largement à la portée de DuckDB ou de Polars. Spark n'est pas ici une nécessité imposée par le volume, c'est un choix d'architecture : un traitement serverless intégré à S3, au Glue Data Catalog et à l'orchestration AWS, sans cluster permanent à administrer. Les jobs tournent sur 2 à 4 workers G.1X, autour de 0,20 $ par job sur l'exécution de référence, hors stockage S3, crawler, Lambda, Redshift et QuickSight.

À raison d'une exécution par jour, le pipeline relirait environ 17 Go et 23 millions de lignes de DECP sur une semaine. Ce sont des volumes cumulés au fil des exécutions, pas la taille d'un jeu de données chargé en une fois : chaque passage écrase le précédent.

## Ce que fait chaque étape

L'ingestion passe par une seule fonction Lambda qui gère les trois sources, choisies par un paramètre dans l'event. Les fichiers font jusqu'à 2,5 Go, donc le téléchargement se fait par morceaux de 64 Mo avec des requêtes HTTP Range, envoyés au fur et à mesure dans un upload multipart S3. Rien n'est écrit sur disque, la taille et le hash sha256 sont calculés au passage. Le transfert des 2,47 Go du DECP prend moins de 3 minutes.

Le nettoyage se fait dans deux jobs Glue. Côté DECP : typage des montants et des dates, validation du format SIRET sur 14 chiffres, déduplication, et mise en quarantaine des lignes invalides dans un dossier séparé. Sur les 3 286 474 lignes du fichier, 2 262 196 passent les contrôles, 347 162 partent en quarantaine et 677 116 sont écartées comme doublons sur le couple `(uid, modification_id)`. Côté Sirene : sélection des colonnes utiles seulement, et masquage des données d'identité quand le statut de diffusion vaut `P`, ce qui correspond aux personnes ayant fait opposition, une obligation RGPD.

Un troisième job joint ensuite le DECP nettoyé à Sirene sur le SIRET de l'acheteur et celui du titulaire. Les dimensions Sirene sont réduites à quelques colonnes puis diffusées en broadcast pour éviter un shuffle massif. Résultat : 2 262 196 lignes de contrats enrichis, écrites en Parquet partitionné par région.

Un crawler Glue catalogue ce résultat, et Redshift l'interroge via Spectrum, sans copie physique des données. Le gold est déjà propre et partitionné, le recopier dans des tables natives n'apporterait rien.

dbt construit enfin trois tables métier : la concentration par acheteur avec l'indice HHI, les agrégats mensuels par couple acheteur et fournisseur, et la comparaison entre montant initial et montant après modifications. Dix tests couvrent l'unicité, la non-nullité et les bornes de l'indice HHI.

## Quelques problèmes rencontrés

Les codes région se sont retrouvés corrompus. En relisant une source déjà partitionnée par `acheteur_region_code`, Spark devine tout seul le type des colonnes de partition. Il a transformé `01` en `1`, `02` en `2`, et ainsi de suite, ce qui casse les codes officiels. Corrigé avec `spark.sql.sources.partitionColumnTypeInference.enabled = false`.

La première écriture partitionnée du silver a produit 798 petits fichiers Parquet pour 2,5 Go de données, parce que chaque partition Spark écrit dans chaque partition de sortie. Un `repartition()` sur la colonne de partition avant l'écriture ramène ça à 25 fichiers, un par région.

Additionner les montants bruts donne des totaux de plusieurs milliards par collectivité. Deux causes : des valeurs sentinelles à 99 999 999 999,99 déjà signalées par la source, et des plafonds d'accords-cadres enregistrés comme s'il s'agissait de dépenses réelles, jusqu'à 12 milliards sur une seule ligne. Une colonne `montant_fiable` calculée en staging écarte les deux cas des agrégats.

Côté permissions, une policy IAM large ne suffit pas quand Lake Formation est actif sur le compte : il intercepte l'accès au Glue Data Catalog et exige ses propres autorisations, en plus de l'IAM.

La connexion QuickSight vers Redshift a demandé un détour. Le workgroup est accessible publiquement, donc son nom d'hôte résout vers une adresse publique. Les interfaces réseau créées par QuickSight dans le VPC n'ont qu'une adresse privée et aucune passerelle NAT, la réponse ne pouvait donc jamais revenir. Il faut passer par l'endpoint privé du VPC Endpoint associé au workgroup.

## Organisation du code

Le dossier `lambda` contient la fonction d'ingestion des trois sources. Les trois jobs Spark sont dans `spark`, un par étape (silver DECP, silver Sirene, gold). Le projet dbt est dans `dbt`, avec une vue de staging et trois marts. Le template CloudFormation qui décrit la Lambda, les jobs Glue, le crawler, le workflow et les règles de planification est dans `cloudformation`.

## Reproduire le projet

Prérequis : un compte AWS, un bucket S3, Python 3.12.

```bash
git clone https://github.com/Ademnsr/French-public-procurement-observatory.git
cd French-public-procurement-observatory
cp .env.example .env
# renseigner le bucket S3 et les identifiants Redshift
set -a; source .env; set +a

python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Deux fichiers de configuration partent d'un modèle versionné, à copier puis remplir :

| Modèle | À copier vers | Utilisé par |
|---|---|---|
| `.env.example` | `.env` | les commandes aws ci-dessous, et dbt via `env_var()` |
| `dbt/profiles.yml.example` | `~/.dbt/profiles.yml` | la connexion dbt vers Redshift |

Le template CloudFormation référence le code depuis S3, il faut donc l'y déposer d'abord :

```bash
cd lambda && zip -j ingestion_bronze.zip ingestion_bronze.py && cd ..
aws s3 cp lambda/ingestion_bronze.zip s3://$S3_BUCKET/scripts/ --region eu-west-3
aws s3 cp spark/ s3://$S3_BUCKET/scripts/ --recursive --exclude "*" --include "*.py" --region eu-west-3
```

Le déploiement crée ensuite la Lambda, les trois jobs Glue, le crawler, le workflow et les règles de planification. Les jobs Glue lisent des partitions datées dans le bronze, le stack prend donc ces dates en paramètres : celle du jour convient si l'ingestion suit le déploiement.

```bash
aws cloudformation deploy \
  --template-file cloudformation/template.yaml \
  --stack-name observatoire-marches \
  --capabilities CAPABILITY_IAM \
  --region eu-west-3 \
  --parameter-overrides BucketName=$S3_BUCKET \
      DecpExtractDate=$(date +%F) SireneSnapshotDate=$(date +%F)
```

Les règles EventBridge sont créées désactivées, pour ne pas déclencher un pipeline facturé sans action explicite. Le premier passage se lance donc à la main : l'ingestion des trois sources, le workflow Glue, puis le crawler une fois le workflow terminé.

```bash
aws lambda invoke --function-name ingestion_bronze \
  --payload '{"source": "decp"}' \
  --cli-binary-format raw-in-base64-out --cli-read-timeout 0 \
  --region eu-west-3 /tmp/ingestion.json
# rejouer avec sirene_unite_legale puis sirene_etablissement

aws glue start-workflow-run --name workflow_observatoire --region eu-west-3
aws glue start-crawler --name crawler_gold --region eu-west-3
```

Redshift reste manuel, ces étapes se prêtent mal à l'infrastructure as code : créer un workgroup Serverless, puis dans l'éditeur de requêtes le schéma externe qui expose le catalogue Glue à Spectrum, avec un rôle IAM qui peut lire le catalogue et le bucket :

```sql
create external schema gold
from data catalog
database 'observatoire_marches'
iam_role 'arn:aws:iam::<compte>:role/<role-spectrum>'
region 'eu-west-3';
```

Si Lake Formation est actif sur le compte, ce rôle doit aussi recevoir DESCRIBE sur la base et SELECT sur la table dans Lake Formation même, l'IAM seul ne suffit pas.

Une fois les données en place :

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
cd dbt
dbt run
dbt test
```

Le profil ne contient aucun secret, la connexion vient des variables d'environnement du `.env` exportées plus haut.

## Limites

Le projet tourne sur un seul instantané. Le traitement incrémental et l'historisation des snapshots Sirene mensuels ne sont pas implémentés.

La planification s'arrête à l'ingestion. Les règles EventBridge n'appellent que la Lambda, le workflow Glue reste en on-demand, et les dates de partition que lisent les jobs sont fixées au déploiement. Une exécution quotidienne de bout en bout demanderait de passer ces dates dynamiquement et d'enchaîner le workflow derrière l'ingestion.

Le seuil de 500 millions d'euros qui filtre les montants aberrants est arbitraire. Distinguer proprement un plafond d'accord-cadre d'une dépense réelle demanderait de croiser d'autres champs de la source.

Le dashboard QuickSight se limite à quelques graphiques sur la table de concentration. La partie intéressante du projet est en amont.
