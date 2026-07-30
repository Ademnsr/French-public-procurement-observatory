# Observatoire des marchés publics français

[English](#english) / [Français](#français)

<a id="english"></a>
## English

A data pipeline that matches French public procurement contracts (DECP) against the national business register (Sirene), to measure how concentrated public spending is per buyer.

The starting question: which public buyers depend on a very small number of suppliers? This is not a fraud detection project, the data alone cannot support that kind of claim. It produces concentration indicators and flags technical anomalies.

### The pipeline

Source files are downloaded from data.gouv.fr by a Lambda function and dropped into an S3 bucket, bronze layer. Two AWS Glue jobs clean them and write Parquet into the silver layer, a third one joins DECP with Sirene and produces the gold layer. A crawler catalogues the result, Redshift queries it through Spectrum, and dbt builds the business tables on top. QuickSight connects to those tables for reporting. EventBridge schedules ingestion, a Glue workflow chains the silver jobs then the gold one.

### Stack

| Layer | Tool |
|---|---|
| Ingestion | AWS Lambda, Python 3.12 |
| Data lake | Amazon S3 (partitioned Parquet) |
| Distributed processing | AWS Glue 4.0, PySpark |
| Catalogue | AWS Glue Data Catalog |
| Warehouse | Amazon Redshift Serverless with Spectrum |
| Transformation | dbt (dbt-redshift) |
| Reporting | Amazon QuickSight |
| Orchestration | EventBridge, Glue Workflow |
| Infrastructure | CloudFormation |

### Sources and volume

The consolidated DECP dataset published on data.gouv.fr, updated daily, holds awarded contracts with their buyers, suppliers, amounts and amendments. Insee's Sirene database adds the NAF activity code, the municipality and the legal category of each company.

| File | Size | Rows |
|---|---|---|
| decp.csv | 2.47 GB | 3,286,474 |
| unite_legale.parquet | 0.70 GB | 29.8 million |
| etablissement.parquet | 2.19 GB | 43.7 million |

DECP is republished daily, Sirene about once a month. Every run starts from the same snapshot: it reads the 3.3 million DECP rows again, then performs two SIRET joins, one on the buyer and one on the supplier, against a Sirene dimension of 43.7 million establishments.

That volume fits on a single machine. Once the ten useful columns are selected, the silver establishments table weighs 0.80 GB in Parquet, well within reach of DuckDB or Polars. Spark is not a necessity imposed by the volume here, it is an architecture choice: serverless processing wired into S3, the Glue Data Catalog and AWS orchestration, with no permanent cluster to administer. The jobs run on 2 to 4 G.1X workers, around $0.20 per job on the reference run, excluding S3 storage, the crawler, Lambda, Redshift and QuickSight.

At one run a day, the pipeline would read around 17 GB and 23 million DECP rows over a week. Those are volumes accumulated across runs, not the size of a single dataset loaded at once: each pass overwrites the previous one.

### What each step does

Ingestion goes through a single Lambda function that handles all three sources, selected by a parameter in the event. Files are up to 2.5 GB, so the download runs in 64 MB chunks using HTTP Range requests, streamed straight into an S3 multipart upload. Nothing touches the disk, and size and sha256 hash are computed along the way. Transferring the 2.47 GB DECP file takes under 3 minutes.

Cleaning happens in two Glue jobs. On the DECP side: typing amounts and dates, validating the 14 digit SIRET format, deduplication, and quarantining invalid rows in a separate folder. Out of the 3,286,474 rows in the file, 2,262,196 pass the checks, 347,162 go to quarantine and 677,116 are dropped as duplicates on the `(uid, modification_id)` pair. On the Sirene side: keeping only the useful columns, and masking identity fields when the diffusion status is `P`, which marks people who opted out, a GDPR requirement.

A third job then joins the cleaned DECP with Sirene on both the buyer's and the supplier's SIRET. The Sirene dimensions are cut down to a few columns and broadcast to avoid a massive shuffle. The result is 2,262,196 enriched contract rows, written as Parquet partitioned by region.

A Glue crawler catalogues that output, and Redshift queries it through Spectrum, with no physical copy. The gold layer is already clean and partitioned, copying it into native tables would add nothing.

dbt finally builds three business tables: concentration per buyer with the HHI index, monthly aggregates per buyer and supplier pair, and the comparison between the initial amount and the amount after amendments. Ten tests cover uniqueness, non nullity and the bounds of the HHI index.

### A few problems along the way

Region codes ended up corrupted. When reading back a source already partitioned by `acheteur_region_code`, Spark infers the type of partition columns on its own. It turned `01` into `1`, `02` into `2`, and so on, which breaks the official codes. Fixed with `spark.sql.sources.partitionColumnTypeInference.enabled = false`.

The first partitioned write of the silver layer produced 798 small Parquet files for 2.5 GB of data, because every Spark partition writes into every output partition. A `repartition()` on the partition column before writing brings that down to 25 files, one per region.

Summing raw amounts gives totals in the billions per local authority. Two causes: sentinel values at 99,999,999,999.99 already flagged by the source, and framework agreement ceilings recorded as if they were actual spending, up to 12 billion on a single row. A `montant_fiable` column computed in staging keeps both cases out of the aggregates.

On permissions, a broad IAM policy is not enough when Lake Formation is active on the account: it intercepts access to the Glue Data Catalog and demands its own grants, on top of IAM.

Connecting QuickSight to Redshift needed a detour. The workgroup is publicly accessible, so its hostname resolves to a public address. The network interfaces QuickSight creates inside the VPC only have private addresses and there is no NAT gateway, so the response could never come back. The fix is to connect through the private endpoint of the VPC Endpoint attached to the workgroup.

### Code layout

The `lambda` folder holds the ingestion function for all three sources. The three Spark jobs are in `spark`, one per step (silver DECP, silver Sirene, gold). The dbt project is in `dbt`, with one staging view and three marts. The CloudFormation template describing the Lambda, the Glue jobs, the crawler, the workflow and the schedule rules is in `cloudformation`.

### Running it

Requirements: an AWS account, an S3 bucket, Python 3.12.

```bash
git clone https://github.com/Ademnsr/French-public-procurement-observatory.git
cd French-public-procurement-observatory
cp .env.example .env
# fill in the S3 bucket and the Redshift credentials
set -a; source .env; set +a

python3.12 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Two configuration files start from a versioned template, to copy then fill in:

| Template | Copy to | Used by |
|---|---|---|
| `.env.example` | `.env` | the aws commands below, and dbt through `env_var()` |
| `dbt/profiles.yml.example` | `~/.dbt/profiles.yml` | the dbt connection to Redshift |

The CloudFormation template references code from S3, so it has to be uploaded first:

```bash
cd lambda && zip -j ingestion_bronze.zip ingestion_bronze.py && cd ..
aws s3 cp lambda/ingestion_bronze.zip s3://$S3_BUCKET/scripts/ --region eu-west-3
aws s3 cp spark/ s3://$S3_BUCKET/scripts/ --recursive --exclude "*" --include "*.py" --region eu-west-3
```

Deploying then creates the Lambda, the three Glue jobs, the crawler, the workflow and the schedule rules. The Glue jobs read dated partitions in the bronze layer, so the stack takes those dates as parameters: today's date works if ingestion follows right after.

```bash
aws cloudformation deploy \
  --template-file cloudformation/template.yaml \
  --stack-name observatoire-marches \
  --capabilities CAPABILITY_IAM \
  --region eu-west-3 \
  --parameter-overrides BucketName=$S3_BUCKET \
      DecpExtractDate=$(date +%F) SireneSnapshotDate=$(date +%F)
```

The EventBridge rules are created disabled, so a billed pipeline never starts running without an explicit decision. The first run is therefore launched by hand: ingest the three sources, start the Glue workflow, then the crawler once the workflow is done.

```bash
aws lambda invoke --function-name ingestion_bronze \
  --payload '{"source": "decp"}' \
  --cli-binary-format raw-in-base64-out --cli-read-timeout 0 \
  --region eu-west-3 /tmp/ingestion.json
# run it again with sirene_unite_legale then sirene_etablissement

aws glue start-workflow-run --name workflow_observatoire --region eu-west-3
aws glue start-crawler --name crawler_gold --region eu-west-3
```

Redshift stays manual, those steps do not lend themselves to infrastructure as code: create a Serverless workgroup, then in the query editor the external schema that exposes the Glue catalogue to Spectrum, with an IAM role that can read the catalogue and the bucket:

```sql
create external schema gold
from data catalog
database 'observatoire_marches'
iam_role 'arn:aws:iam::<account>:role/<spectrum-role>'
region 'eu-west-3';
```

If Lake Formation is active on the account, that role also needs DESCRIBE on the database and SELECT on the table, granted in Lake Formation itself, IAM alone is not enough.

Once the data is in place:

```bash
cp dbt/profiles.yml.example ~/.dbt/profiles.yml
cd dbt
dbt run
dbt test
```

The profile holds no secret, the connection comes from the `.env` environment variables exported earlier.

### Limits

The project runs on a single snapshot. Incremental processing and historising the monthly Sirene snapshots are not implemented.

Scheduling stops at ingestion. The EventBridge rules only invoke the Lambda, the Glue workflow stays on demand, and the partition dates the jobs read are set at deploy time. A daily end to end run would mean passing those dates dynamically and chaining the workflow behind ingestion.

The 500 million euro threshold that filters out aberrant amounts is arbitrary. Properly telling a framework agreement ceiling apart from real spending would mean cross checking other fields from the source.

The QuickSight dashboard is limited to a few charts on the concentration table. The interesting part of this project sits upstream.

<a id="français"></a>
## Français

Pipeline de données qui rapproche les marchés publics français (DECP) du répertoire des entreprises (Sirene), pour mesurer la concentration des dépenses publiques par acheteur.

La question de départ : quels acheteurs publics dépendent d'un très petit nombre de fournisseurs ? Le projet ne cherche pas à détecter de la fraude, les données seules ne le permettent pas. Il produit des indicateurs de concentration et signale des anomalies techniques.

### Le pipeline

Les fichiers sources sont téléchargés depuis data.gouv.fr par une fonction Lambda et déposés dans un bucket S3, couche bronze. Deux jobs AWS Glue les nettoient et les écrivent en Parquet dans la couche silver, un troisième joint le DECP à Sirene et produit la couche gold. Un crawler catalogue le résultat, Redshift l'interroge via Spectrum, et dbt construit les tables métier par dessus. QuickSight se branche sur ces tables pour la restitution. EventBridge planifie l'ingestion, un workflow Glue enchaîne les jobs silver puis gold.

### Stack technique

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

### Sources et volumétrie

Le DECP consolidé au format tabulaire, publié sur data.gouv.fr et mis à jour quotidiennement, contient les marchés attribués avec leurs acheteurs, titulaires, montants et modifications. La base Sirene de l'Insee apporte l'activité NAF, la commune et la catégorie juridique de chaque entreprise.

| Fichier | Taille | Lignes |
|---|---|---|
| decp.csv | 2,47 Go | 3 286 474 |
| unite_legale.parquet | 0,70 Go | 29,8 millions |
| etablissement.parquet | 2,19 Go | 43,7 millions |

Le DECP est republié quotidiennement, Sirene environ une fois par mois. Chaque exécution repart du même instantané : elle relit les 3,3 millions de lignes du DECP, puis effectue deux jointures par SIRET, celle de l'acheteur et celle du titulaire, contre une dimension Sirene de 43,7 millions d'établissements.

Ce volume tient sur une machine unique. Une fois les dix colonnes utiles sélectionnées, la table silver des établissements pèse 0,80 Go en Parquet, largement à la portée de DuckDB ou de Polars. Spark n'est pas ici une nécessité imposée par le volume, c'est un choix d'architecture : un traitement serverless intégré à S3, au Glue Data Catalog et à l'orchestration AWS, sans cluster permanent à administrer. Les jobs tournent sur 2 à 4 workers G.1X, autour de 0,20 $ par job sur l'exécution de référence, hors stockage S3, crawler, Lambda, Redshift et QuickSight.

À raison d'une exécution par jour, le pipeline relirait environ 17 Go et 23 millions de lignes de DECP sur une semaine. Ce sont des volumes cumulés au fil des exécutions, pas la taille d'un jeu de données chargé en une fois : chaque passage écrase le précédent.

### Ce que fait chaque étape

L'ingestion passe par une seule fonction Lambda qui gère les trois sources, choisies par un paramètre dans l'event. Les fichiers font jusqu'à 2,5 Go, donc le téléchargement se fait par morceaux de 64 Mo avec des requêtes HTTP Range, envoyés au fur et à mesure dans un upload multipart S3. Rien n'est écrit sur disque, la taille et le hash sha256 sont calculés au passage. Le transfert des 2,47 Go du DECP prend moins de 3 minutes.

Le nettoyage se fait dans deux jobs Glue. Côté DECP : typage des montants et des dates, validation du format SIRET sur 14 chiffres, déduplication, et mise en quarantaine des lignes invalides dans un dossier séparé. Sur les 3 286 474 lignes du fichier, 2 262 196 passent les contrôles, 347 162 partent en quarantaine et 677 116 sont écartées comme doublons sur le couple `(uid, modification_id)`. Côté Sirene : sélection des colonnes utiles seulement, et masquage des données d'identité quand le statut de diffusion vaut `P`, ce qui correspond aux personnes ayant fait opposition, une obligation RGPD.

Un troisième job joint ensuite le DECP nettoyé à Sirene sur le SIRET de l'acheteur et celui du titulaire. Les dimensions Sirene sont réduites à quelques colonnes puis diffusées en broadcast pour éviter un shuffle massif. Résultat : 2 262 196 lignes de contrats enrichis, écrites en Parquet partitionné par région.

Un crawler Glue catalogue ce résultat, et Redshift l'interroge via Spectrum, sans copie physique des données. Le gold est déjà propre et partitionné, le recopier dans des tables natives n'apporterait rien.

dbt construit enfin trois tables métier : la concentration par acheteur avec l'indice HHI, les agrégats mensuels par couple acheteur et fournisseur, et la comparaison entre montant initial et montant après modifications. Dix tests couvrent l'unicité, la non-nullité et les bornes de l'indice HHI.

### Quelques problèmes rencontrés

Les codes région se sont retrouvés corrompus. En relisant une source déjà partitionnée par `acheteur_region_code`, Spark devine tout seul le type des colonnes de partition. Il a transformé `01` en `1`, `02` en `2`, et ainsi de suite, ce qui casse les codes officiels. Corrigé avec `spark.sql.sources.partitionColumnTypeInference.enabled = false`.

La première écriture partitionnée du silver a produit 798 petits fichiers Parquet pour 2,5 Go de données, parce que chaque partition Spark écrit dans chaque partition de sortie. Un `repartition()` sur la colonne de partition avant l'écriture ramène ça à 25 fichiers, un par région.

Additionner les montants bruts donne des totaux de plusieurs milliards par collectivité. Deux causes : des valeurs sentinelles à 99 999 999 999,99 déjà signalées par la source, et des plafonds d'accords-cadres enregistrés comme s'il s'agissait de dépenses réelles, jusqu'à 12 milliards sur une seule ligne. Une colonne `montant_fiable` calculée en staging écarte les deux cas des agrégats.

Côté permissions, une policy IAM large ne suffit pas quand Lake Formation est actif sur le compte : il intercepte l'accès au Glue Data Catalog et exige ses propres autorisations, en plus de l'IAM.

La connexion QuickSight vers Redshift a demandé un détour. Le workgroup est accessible publiquement, donc son nom d'hôte résout vers une adresse publique. Les interfaces réseau créées par QuickSight dans le VPC n'ont qu'une adresse privée et aucune passerelle NAT, la réponse ne pouvait donc jamais revenir. Il faut passer par l'endpoint privé du VPC Endpoint associé au workgroup.

### Organisation du code

Le dossier `lambda` contient la fonction d'ingestion des trois sources. Les trois jobs Spark sont dans `spark`, un par étape (silver DECP, silver Sirene, gold). Le projet dbt est dans `dbt`, avec une vue de staging et trois marts. Le template CloudFormation qui décrit la Lambda, les jobs Glue, le crawler, le workflow et les règles de planification est dans `cloudformation`.

### Reproduire le projet

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

### Limites

Le projet tourne sur un seul instantané. Le traitement incrémental et l'historisation des snapshots Sirene mensuels ne sont pas implémentés.

La planification s'arrête à l'ingestion. Les règles EventBridge n'appellent que la Lambda, le workflow Glue reste en on-demand, et les dates de partition que lisent les jobs sont fixées au déploiement. Une exécution quotidienne de bout en bout demanderait de passer ces dates dynamiquement et d'enchaîner le workflow derrière l'ingestion.

Le seuil de 500 millions d'euros qui filtre les montants aberrants est arbitraire. Distinguer proprement un plafond d'accord-cadre d'une dépense réelle demanderait de croiser d'autres champs de la source.

Le dashboard QuickSight se limite à quelques graphiques sur la table de concentration. La partie intéressante du projet est en amont.
