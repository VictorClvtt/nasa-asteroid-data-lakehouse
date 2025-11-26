from dotenv import load_dotenv
from pathlib import Path
import os

def load_env_vars(env_base_path=Path(__file__).parent.parent.parent.parent):
    env_path = env_base_path / ".env"
    load_dotenv(env_path)

    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

    return access_key, secret_key, bucket_name, bucket_endpoint