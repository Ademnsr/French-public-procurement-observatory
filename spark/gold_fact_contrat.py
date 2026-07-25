#Jointure DECP <-> Sirene sur SIRET/SIREN (silver -> gold)
#Un contrat rejoint aux infos acheteur et titulaire (activite, commune, denomination)
#bucket passe en argument du job Glue
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["bucket"])
bucket = args["bucket"]

spark = SparkSession.builder.appName("gold_fact_contrat").getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
#sans ca Spark devine le type des colonnes de partition et supprime les zeros de tete (01 -> 1)
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")

decp = spark.read.parquet(f"s3://{bucket}/silver/decp")
etab = spark.read.parquet(f"s3://{bucket}/silver/sirene_etablissement")
ul = spark.read.parquet(f"s3://{bucket}/silver/sirene_unite_legale")

#Petites dimensions (quelques colonnes, unites legales/etablissements) : broadcast pour eviter le shuffle
etab_leger = F.broadcast(
    etab.select(
        "siret",
        "siren",
        F.col("activitePrincipaleEtablissement").alias("activite_code"),
        F.col("codeCommuneEtablissement").alias("commune_code"),
        F.col("libelleCommuneEtablissement").alias("commune_nom"),
    )
)

ul_leger = F.broadcast(
    ul.select(
        "siren",
        F.col("denominationUniteLegale").alias("denomination"),
        F.col("categorieJuridiqueUniteLegale").alias("categorie_juridique"),
    )
)

acheteur_info = etab_leger.join(ul_leger, "siren").select(
    F.col("siret").alias("acheteur_id"),
    F.col("activite_code").alias("acheteur_activite_code"),
    F.col("commune_code").alias("acheteur_commune_code_sirene"),
    F.col("commune_nom").alias("acheteur_commune_nom_sirene"),
    F.col("denomination").alias("acheteur_denomination_sirene"),
    F.col("categorie_juridique").alias("acheteur_categorie_juridique"),
)

titulaire_info = etab_leger.join(ul_leger, "siren").select(
    F.col("siret").alias("titulaire_id"),
    F.col("activite_code").alias("titulaire_activite_code_sirene"),
    F.col("commune_code").alias("titulaire_commune_code_sirene"),
    F.col("commune_nom").alias("titulaire_commune_nom_sirene"),
    F.col("denomination").alias("titulaire_denomination_sirene"),
    F.col("categorie_juridique").alias("titulaire_categorie_juridique"),
)

fact_contrat = (
    decp.join(acheteur_info, "acheteur_id", "left")
    .join(titulaire_info, "titulaire_id", "left")
    .withColumn("acheteur_rapproche_sirene", F.col("acheteur_denomination_sirene").isNotNull())
    .withColumn("titulaire_rapproche_sirene", F.col("titulaire_denomination_sirene").isNotNull())
)

fact_contrat.repartition("acheteur_region_code").write.mode("overwrite").partitionBy(
    "acheteur_region_code"
).parquet(f"s3://{bucket}/gold/fact_contrat")

total = fact_contrat.count()
rapproches = fact_contrat.filter(
    F.col("acheteur_rapproche_sirene") & F.col("titulaire_rapproche_sirene")
).count()
print(f"Contrats total : {total}")
print(f"Contrats rapproches (acheteur + titulaire) : {rapproches}")
print(f"Taux de rapprochement : {rapproches / total:.2%}")
