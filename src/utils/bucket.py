import json
import boto3
from botocore.exceptions import ClientError

def create_bucket_if_not_exists(bucket_endpoint, access_key, secret_key, bucket_name):
    s3 = boto3.client(
        "s3",
        endpoint_url=bucket_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"☑️ Bucket '{bucket_name}' found.")
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            print(f"☑️ Bucket '{bucket_name}' not found. Creating bucket...")
            s3.create_bucket(Bucket=bucket_name)
            print(f"✅ Bucket '{bucket_name}' succesfully created!")
        else:
            raise e

def file_to_bucket(bucket_endpoint, access_key, secret_key, bucket_name, path, data):

    create_bucket_if_not_exists(bucket_endpoint, access_key, secret_key, bucket_name)

    s3 = boto3.client(
        "s3",
        endpoint_url=bucket_endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key
    )

    # Se o arquivo já for bytes (ex: DuckDB, Parquet), manda direto
    if isinstance(data, bytes):
        body = data
    else:
        # Caso contrário, assume que é JSON serializável
        body = json.dumps(data, indent=2).encode("utf-8")

    s3.put_object(
        Bucket=bucket_name,
        Key=path,
        Body=body
    )

    print(f"✅ File saved in s3://{bucket_name}/{path}")

def df_to_bucket(df, path: str, partition_by: str=None, mode: str="append"):
    if partition_by:
        df.write \
            .mode(mode) \
            .partitionBy(partition_by) \
            .parquet(path)
    else:
        df.write \
            .mode(mode) \
            .parquet(path)