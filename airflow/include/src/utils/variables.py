from dotenv import load_dotenv
from pathlib import Path
import os

def load_env_vars(env_base_path=Path(__file__).parent.parent.parent):
    env_path = env_base_path / ".env"
    load_dotenv(env_path)

    api_key = os.getenv("NASA_API_KEY")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

    return api_key, access_key, secret_key, bucket_name, bucket_endpoint
