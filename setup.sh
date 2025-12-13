#!/usr/bin/env bash
set -euo pipefail

if [[ -d "astro_project" ]]; then
    cd astro_project
    astro-cli dev kill
    cd ..
    sudo rm -rf astro_project
fi

# ---------- Leitura de argumentos ----------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --bucket-name)
        BUCKET_NAME="$2"
        shift 2
        ;;
        --nasa-api-key)
        NASA_API_KEY="$2"
        shift 2
        ;;
        *)
        echo "Parâmetro desconhecido: $1"
        exit 1
        ;;
    esac
done

# ---------- Validação dos parâmetros ----------
: "${BUCKET_NAME:?--bucket-name é obrigatório}"
: "${NASA_API_KEY:?--nasa-api-key é obrigatório}"

# ---------- Setup do diretório do projeto ----------
mkdir -p astro_project
cd astro_project

# ---------- Inicializa o projeto com astro-cli ----------
astro-cli dev init

# ---------- Copia os arquivos de configuração ----------
cp -ra ../airflow/* .

# ---------- Renomeia o arquivo .env.example para .env ----------
mv include/.env.example include/.env

# ---------- Substitui as variáveis dentro do .env ----------
ENV_FILE="include/.env"

# Utiliza sed para substituir os valores no arquivo .env
sed -i \
    -e "s|^BUCKET_NAME=.*|BUCKET_NAME=$BUCKET_NAME|" \
    -e "s|^NASA_API_KEY=.*|NASA_API_KEY=$NASA_API_KEY|" \
    "$ENV_FILE"

# ---------- Inicia o projeto com astro-cli ----------
astro-cli dev start

# ---------- Cria conexão com o container Spark ----------
docker exec \
$(docker ps --format '{{.Names}}' | grep webserver | head -n 1) \
airflow connections add my_spark_conn \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077