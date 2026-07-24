#Nettoyage et typage du DECP (bronze -> silver), lignes invalides en quarantaine
#bucket et extract_date arrivent en arguments du job Glue
import sys

from awsglue.utils import getResolvedOptions
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["bucket", "extract_date"])
bucket = args["bucket"]
extract_date = args["extract_date"]

bronze_path = f"s3://{bucket}/bronze/decp/extract_date={extract_date}/decp.csv"
silver_path = f"s3://{bucket}/silver/decp"
quarantine_path = f"s3://{bucket}/silver/decp_quarantine"

spark = SparkSession.builder.appName("silver_decp").getOrCreate()
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")

df = spark.read.option("header", True).csv(bronze_path)

df = (
    df.withColumn("montant", F.col("montant").cast("double"))
    .withColumn("montant_rationalise", F.col("montant_rationalise").cast("double"))
    .withColumn("dureeMois", F.col("dureeMois").cast("int"))
    .withColumn("offresRecues", F.col("offresRecues").cast("int"))
    .withColumn("modification_id", F.col("modification_id").cast("int"))
    .withColumn("dateNotification", F.to_date("dateNotification"))
    .withColumn("datePublicationDonnees", F.to_date("datePublicationDonnees"))
    .withColumn("est_modification", F.col("modification_id") > 0)
    .withColumn("ingestion_timestamp", F.current_timestamp())
    .withColumn("source_file", F.lit(bronze_path))
    .withColumn("extract_date", F.lit(extract_date))
)

siret_valide = r"^\d{14}$"
df = df.withColumn(
    "siret_valide",
    F.col("acheteur_id").rlike(siret_valide) & F.col("titulaire_id").rlike(siret_valide),
)

valides = df.filter(
    F.col("siret_valide")
    & F.col("montant").isNotNull()
    & (F.col("montant") > 0)
    & F.col("dateNotification").isNotNull()
).dropDuplicates(["uid", "modification_id"])

invalides = df.join(valides.select("uid", "modification_id"), ["uid", "modification_id"], "left_anti")

valides.repartition("acheteur_region_code").write.mode("overwrite").partitionBy(
    "acheteur_region_code"
).parquet(silver_path)
invalides.coalesce(4).write.mode("overwrite").parquet(quarantine_path)

print(f"Lignes valides : {valides.count()}")
print(f"Lignes en quarantaine : {invalides.count()}")
