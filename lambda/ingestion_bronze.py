#Ingestion Bronze : telechargement des sources vers S3 par morceaux, execute dans Lambda
#Une seule fonction pour les trois sources (DECP + les deux fichiers Sirene),
#choisie par le champ "source" de l'event. URL stables data.gouv.fr.
import hashlib
import os
import urllib.request
from datetime import datetime, timezone

import boto3

sources = {
    "decp": "https://www.data.gouv.fr/fr/datasets/r/22847056-61df-452d-837d-8b8ceadbfc52",
    "sirene_unite_legale": "https://www.data.gouv.fr/fr/datasets/r/350182c9-148a-46e0-8389-76c2ec1374a3",
    "sirene_etablissement": "https://www.data.gouv.fr/fr/datasets/r/a29c1297-1f92-4e2a-8f6b-8c902ce96c5f",
}

taille_morceau = 64 * 1024 * 1024


def get_file_size(url):
    #sans identity le serveur repond en gzip et la taille est fausse
    req = urllib.request.Request(url, method="HEAD", headers={"Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return int(response.headers["Content-Length"])


def download_chunk(url, start, end):
    headers = {"Range": f"bytes={start}-{end}", "Accept-Encoding": "identity"}
    req = urllib.request.Request(url, headers=headers)
    with urllib.request.urlopen(req, timeout=120) as response:
        return response.read()


def transfer_source(url, s3_key, bucket_name):
    file_size = get_file_size(url)
    nb_morceaux = (file_size // taille_morceau) + 1
    print(f"{s3_key} : {file_size / 1e9:.2f} Go, {nb_morceaux} morceaux")

    s3_client = boto3.client("s3")
    multipart = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
    upload_id = multipart["UploadId"]

    sha256 = hashlib.sha256()
    parts = []

    for i in range(nb_morceaux):
        start = i * taille_morceau
        end = min(start + taille_morceau, file_size) - 1
        chunk = download_chunk(url, start, end)
        sha256.update(chunk)

        part = s3_client.upload_part(Bucket=bucket_name, Key=s3_key, UploadId=upload_id, PartNumber=i + 1, Body=chunk)
        parts.append({"PartNumber": i + 1, "ETag": part["ETag"]})
        print(f"{s3_key} : morceau {i + 1}/{nb_morceaux} envoye")

    s3_client.complete_multipart_upload(Bucket=bucket_name, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts})
    return {"taille_go": round(file_size / 1e9, 2), "sha256": sha256.hexdigest()}


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)
    date_str = now.strftime("%Y-%m-%d")
    bucket_name = os.environ["S3_BUCKET"]

    source = event["source"]
    url = sources[source]

    if source == "decp":
        s3_key = f"bronze/decp/extract_date={date_str}/decp.csv"
    else:
        nom_fichier = source.replace("sirene_", "")
        s3_key = f"bronze/sirene/snapshot_date={date_str}/{nom_fichier}.parquet"

    resultat = transfer_source(url, s3_key, bucket_name)
    resultat["s3_key"] = s3_key
    return resultat
