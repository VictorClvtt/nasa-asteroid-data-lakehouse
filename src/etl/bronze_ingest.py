#%%
from datetime import datetime
import requests

import os, sys

current_dir = os.path.dirname(os.path.abspath(__file__))
project_root = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(project_root)

from src.utils.bucket import file_to_bucket
from src.utils.variables import load_env_vars

# Carregando e salvando variaveis do .env
api_key, access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()

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