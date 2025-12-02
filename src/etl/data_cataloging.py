# %%
import duckdb
from pathlib import Path

from utils.variables import load_env_vars

# -----------------------
# CONFIGURAÇÃO DO DUCKDB
# -----------------------
catalog_path = "catalog.duckdb"
con = duckdb.connect(catalog_path)

con.execute("INSTALL httpfs;")
con.execute("LOAD httpfs;")
con.execute("INSTALL delta;")
con.execute("LOAD delta;")

# %%
# -----------------------
# CONFIGURAÇÃO DO MINIO
# -----------------------
access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars()

bucket_endpoint = bucket_endpoint.replace("http://", "").replace("https://", "").rstrip("/")

con.execute(f"""
    SET s3_endpoint='{bucket_endpoint}';
    SET s3_region='us-east-1';
    SET s3_access_key_id='{access_key}';
    SET s3_secret_access_key='{secret_key}';
    SET s3_use_ssl=false;
    SET s3_url_style='path';
""")

# %%
# -----------------------
# LISTA DE TABELAS GOLD
# -----------------------
gold_tables = {
    "dim_approach_date": f"s3://{bucket_name}/gold/dim_approach_date/*.parquet",
    "dim_asteroid": f"s3://{bucket_name}/gold/dim_asteroid/*.parquet",
    "dim_orbiting_body": f"s3://{bucket_name}/gold/dim_orbiting_body/*.parquet",
    "fact_asteroid_approach": f"s3://{bucket_name}/gold/fact_asteroid_approach/*.parquet"
}

# %%
# -----------------------
# FUNÇÃO DE CATALOGAÇÃO
# -----------------------
def catalog_table(table_name, path):
    print(f"Catalogando {table_name}...")

    con.execute(
        f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM read_parquet(?)",
        [path]
    )

    print(f"Tabela {table_name} atualizada.")

# -----------------------
# EXECUÇÃO
# -----------------------
for name, path in gold_tables.items():
    catalog_table(name, path)

print("\nCatálogo GOLD atualizado com sucesso!")