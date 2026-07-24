#Nettoyage Sirene (bronze -> silver), colonnes utiles uniquement,
#masquage des donnees personnelles quand statutDiffusion = 'P' (opposition RGPD)
#bucket et snapshot_date arrivent en arguments du job Glue
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["bucket", "snapshot_date"])
bucket = args["bucket"]
snapshot_date = args["snapshot_date"]

spark = SparkSession.builder.appName("silver_sirene").getOrCreate()

#Unites legales
ul = spark.read.parquet(f"s3://{bucket}/bronze/sirene/snapshot_date={snapshot_date}/unite_legale.parquet")

ul = ul.select(
    "siren",
    "statutDiffusionUniteLegale",
    "etatAdministratifUniteLegale",
    "categorieJuridiqueUniteLegale",
    "activitePrincipaleUniteLegale",
    "categorieEntreprise",
    "trancheEffectifsUniteLegale",
    "denominationUniteLegale",
    "nomUniteLegale",
    "prenom1UniteLegale",
)

diffusion_partielle = F.col("statutDiffusionUniteLegale") == "P"
ul = (
    ul.withColumn("nomUniteLegale", F.when(diffusion_partielle, None).otherwise(F.col("nomUniteLegale")))
    .withColumn("prenom1UniteLegale", F.when(diffusion_partielle, None).otherwise(F.col("prenom1UniteLegale")))
    .withColumn("snapshot_date", F.lit(snapshot_date))
)

ul.coalesce(8).write.mode("overwrite").parquet(f"s3://{bucket}/silver/sirene_unite_legale")

#Etablissements
etab = spark.read.parquet(f"s3://{bucket}/bronze/sirene/snapshot_date={snapshot_date}/etablissement.parquet")

etab = etab.select(
    "siren",
    "siret",
    "statutDiffusionEtablissement",
    "etatAdministratifEtablissement",
    "etablissementSiege",
    "activitePrincipaleEtablissement",
    "codePostalEtablissement",
    "codeCommuneEtablissement",
    "libelleCommuneEtablissement",
    "enseigne1Etablissement",
)

diffusion_partielle_etab = F.col("statutDiffusionEtablissement") == "P"
etab = etab.withColumn(
    "libelleCommuneEtablissement",
    F.when(diffusion_partielle_etab, None).otherwise(F.col("libelleCommuneEtablissement")),
).withColumn("snapshot_date", F.lit(snapshot_date))

etab.coalesce(8).write.mode("overwrite").parquet(f"s3://{bucket}/silver/sirene_etablissement")

print(f"Unites legales : {ul.count()}")
print(f"Etablissements : {etab.count()}")
