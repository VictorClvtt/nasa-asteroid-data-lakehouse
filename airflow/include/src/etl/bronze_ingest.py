#%%
import os
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
import requests

from .utils.bucket import file_to_bucket

def bronze_ingest():
    # Carregando e salvando variaveis do .env
    env_path = Path(__file__).parent.parent.parent / ".env"

    load_dotenv(env_path)

    api_key = os.getenv("NASA_API_KEY")
    access_key = os.getenv("AWS_ACCESS_KEY_ID")
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    bucket_name = os.getenv("BUCKET_NAME")
    bucket_endpoint = os.getenv("BUCKET_ENDPOINT")

    #%%
    url = "https://api.nasa.gov/neo/rest/v1/feed"
    today_str = datetime.today().strftime("%Y-%m-%d")
    params = {
        "start_date": today_str,
        "end_date": today_str,
        "api_key": api_key
    }

    response = requests.get(url, params=params)
    data = response.json()

    # %%
    file_to_bucket(
        bucket_endpoint=bucket_endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket_name=bucket_name, 
        path=f"bronze/JSON/nasa-asteroid-data-{today_str}.json",
        data=data
    )