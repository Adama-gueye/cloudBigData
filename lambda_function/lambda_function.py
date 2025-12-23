import json
import urllib.parse
import boto3
import os
import tempfile
from google.cloud import storage
from google.oauth2 import service_account


s3 = boto3.client("s3")
## Configuration pour récupérer le SA key coté google mis dans Secret Manager
secrets = boto3.client("secretsmanager")
## l'arn du secret
secret_arn = "arn:aws:secretsmanager:eu-central-1:050451380085:secret:gcp/service-account/gcs-transfer-xrqv9Q"
## le project ID coté GCP
GCP_PROJECT = "isi-group-m2dsia"
## Mon bucket GCP Dans lequel je souhaiterais mettre mon fichier
GCS_BUCKET = "m2dsia-gueye-adama"

# Petite Function pour récupérer le secret afin d'avoir les auth pour pouvoir envoyer le fichier dans GCP
def get_gcp_credentials():
    print("Getting credentials from Secrets Manager")
    response = secrets.get_secret_value(SecretId=secret_arn)
    return json.loads(response["SecretString"])

print('Loading function')

s3 = boto3.client('s3')


def lambda_handler_v2(event, context):

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    print("BUCKET: " + bucket)
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("KEY: " + key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

def lambda_handler(event, context):
    # S3 event
    # Get the object from the event and show its content type
    s3_bucket = event['Records'][0]['s3']['bucket']['name']
    print("BUCKET: " + s3_bucket)
    s3_key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print("KEY: " + s3_key)

    # Télécharger S3
    tmp = tempfile.NamedTemporaryFile()
    s3.download_file(s3_bucket, s3_key, tmp.name)
    print("Downloaded file from S3")

    # GCP auth
    credentials_info = get_gcp_credentials()
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    print("Got GCP credentials")

    gcs_client = storage.Client(
        project=GCP_PROJECT,
        credentials=credentials
    )
    print("Created GCS client")
    gcs_bucket = gcs_client.bucket(GCS_BUCKET)
    blob = gcs_bucket.blob(s3_key)
    print("Created GCS blob")
    blob.upload_from_filename(tmp.name)
    print("Uploaded file to GCS Thanks Guy it was funny")

    return {
        "status": "OK",
        "s3": f"{s3_bucket}/{s3_key}",
        "gcs": f"{GCS_BUCKET}/{s3_key}"
    }