import streamlit as st
import duckdb
from pathlib import Path
from include.src.utils.variables import load_env_vars

st.set_page_config(page_title="Catálogo GOLD - DuckDB + MinIO", layout="wide")

@st.cache_resource
def init_connection():
    # Conexão DuckDB em modo in-memory (não grava catálogo)
    con = duckdb.connect(database=":memory:")

    # Extensões necessárias
    con.execute("INSTALL httpfs;")
    con.execute("LOAD httpfs;")
    con.execute("INSTALL delta;")
    con.execute("LOAD delta;")

    # Vars de ambiente
    _, access_key, secret_key, bucket_name, bucket_endpoint = load_env_vars(env_base_path=Path(__file__).parent)

    bucket_endpoint = bucket_endpoint.replace("http://", "").replace("https://", "").rstrip("/")

    # Configuração do MinIO para DuckDB
    con.execute(f"SET s3_endpoint='{bucket_endpoint}';")
    con.execute("SET s3_region='us-east-1';")
    con.execute(f"SET s3_access_key_id='{access_key}';")
    con.execute(f"SET s3_secret_access_key='{secret_key}';")
    con.execute("SET s3_use_ssl=false;")
    con.execute("SET s3_url_style='path';")

    return con, bucket_name


def load_table(con, path):
    return con.execute(
        "SELECT * FROM read_parquet(?)",
        [path]
    ).df()


st.title("📀 Catálogo GOLD")

# Inicializa conexão e pega bucket name
con, bucket_name = init_connection()

# Tabelas GOLD
gold_tables = {
    "dim_approach_date": f"s3://{bucket_name}/gold/dim_approach_date/*.parquet",
    "dim_asteroid": f"s3://{bucket_name}/gold/dim_asteroid/*.parquet",
    "dim_orbiting_body": f"s3://{bucket_name}/gold/dim_orbiting_body/*.parquet",
    "fact_asteroid_approach": f"s3://{bucket_name}/gold/fact_asteroid_approach/*.parquet"
}

tabs = st.tabs(gold_tables.keys())

for tab, (table_name, path) in zip(tabs, gold_tables.items()):
    with tab:
        st.subheader(f"📄 {table_name}")
        st.caption(f"Fonte: `{path}`")

        try:
            df = load_table(con, path)
            st.dataframe(df, use_container_width=True)
            st.success(f"{len(df):,} registros carregados.")
        except Exception as e:
            st.error(f"Erro ao carregar {table_name}: {e}")
