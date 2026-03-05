import os
import boto3

from dotenv import load_dotenv, find_dotenv

# load environment variables from .env file
load_dotenv(find_dotenv())
def env(key, default=None):
    var = os.getenv(key, default)
    if var is None:
        raise RuntimeError(f"Missing env var: {key}")
    return var

# env vars
AWS_ACCESS_KEY_ID = env("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = env("AWS_SECRET_ACCESS_KEY")
AWS_REGION = env("AWS_REGION")
NOAA_TOKEN = env("NOAA_TOKEN")

# aws vars
S3_BUCKET = "vinci-676206945006"
s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION
)