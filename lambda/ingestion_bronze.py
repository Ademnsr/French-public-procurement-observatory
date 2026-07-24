#Telechargement du DECP consolide vers S3 par morceaux, execute dans Lambda
import hashlib
import os
import urllib.request
from datetime import datetime, timezone

import boto3

decp_url = "https://www.data.gouv.fr/fr/datasets/r/22847056-61df-452d-837d-8b8ceadbfc52"
taille_morceau = 64 * 1024 * 1024


def get_file_size():
    #sans identity le serveur repond en gzip et la taille est fausse
    req = urllib.request.Request(decp_url, method="HEAD", headers={"Accept-Encoding": "identity"})
    with urllib.request.urlopen(req, timeout=30) as response:
        return int(response.headers["Content-Length"])


def download_chunk(start, end):
    headers = {"Range": f"bytes={start}-{end}", "Accept-Encoding": "identity"}
    req = urllib.request.Request(decp_url, headers=headers)
    with urllib.request.urlopen(req, timeout=120) as response:
        return response.read()


def lambda_handler(event, context):
    now = datetime.now(timezone.utc)

    bucket_name = os.environ["S3_BUCKET"]
    s3_key = f"bronze/decp/extract_date={now.strftime('%Y-%m-%d')}/decp.csv"

    file_size = get_file_size()
    nb_morceaux = (file_size // taille_morceau) + 1
    print(f"Taille du fichier : {file_size / 1e9:.2f} Go, {nb_morceaux} morceaux")

    s3_client = boto3.client("s3")
    multipart = s3_client.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
    upload_id = multipart["UploadId"]

    sha256 = hashlib.sha256()
    parts = []

    for i in range(nb_morceaux):
        start = i * taille_morceau
        end = min(start + taille_morceau, file_size) - 1
        chunk = download_chunk(start, end)
        sha256.update(chunk)

        part = s3_client.upload_part(
            Bucket=bucket_name, Key=s3_key, UploadId=upload_id, PartNumber=i + 1, Body=chunk
        )
        parts.append({"PartNumber": i + 1, "ETag": part["ETag"]})
        print(f"Morceau {i + 1}/{nb_morceaux} envoye")

    s3_client.complete_multipart_upload(
        Bucket=bucket_name, Key=s3_key, UploadId=upload_id, MultipartUpload={"Parts": parts}
    )
    return {"s3_key": s3_key, "taille_go": round(file_size / 1e9, 2), "sha256": sha256.hexdigest()}
