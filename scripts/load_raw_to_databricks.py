"""
Load Nubank raw CSVs into Databricks (Unity Catalog).

Duas opções de uso:

    OPÇÃO 1 — Via databricks-sql-connector (local, fora do Databricks):
        pip install databricks-sql-connector
        python load_raw_to_databricks.py

    OPÇÃO 2 — Via notebook no Databricks (PySpark):
        Upload os CSVs pro DBFS ou Volume e rode o bloco no final do script.

Dependências (Opção 1):
    pip install databricks-sql-connector
"""

import os
import csv
from pathlib import Path

# =====================================================
# CONFIGURAÇÃO — ajuste aqui ou use variáveis de ambiente
# =====================================================
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "https://adb-xxxx.azuredatabricks.net")  # ou dbc-xxxx.cloud.databricks.com
DATABRICKS_HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "/sql/1.0/warehouses/xxxx")     # SQL Warehouse HTTP path
DATABRICKS_TOKEN = os.getenv("DATABRICKS_TOKEN", "dapi_xxxx")                            # Personal Access Token

CATALOG = os.getenv("DBX_CATALOG", "nubank_analytics")  # Unity Catalog
SCHEMA = os.getenv("DBX_SCHEMA", "raw")
CSV_DIR = os.getenv("CSV_DIR", ".")

# =====================================================
# MAPEAMENTO: CSV -> tabela -> colunas (tudo STRING, dbt tipa depois)
# =====================================================
TABLES = {
    "raw_customers": {
        "file": "raw_customers.csv",
        "columns": [
            "id", "nome", "sobrenome", "documento_tipo", "documento_numero",
            "email", "telefone", "endereco", "cidade", "estado", "pais",
            "moeda_preferida", "data_cadastro", "status",
        ],
    },
    "raw_products": {
        "file": "raw_products.csv",
        "columns": [
            "codigo_produto", "nome_produto", "categoria", "pais", "data_lancamento",
        ],
    },
    "raw_accounts": {
        "file": "raw_accounts.csv",
        "columns": [
            "id_conta", "id_cliente", "tipo_produto", "agencia", "numero_conta",
            "digito", "clabe", "data_abertura", "data_fechamento", "status", "pais",
        ],
    },
    "raw_pix_keys": {
        "file": "raw_pix_keys.csv",
        "columns": [
            "id", "id_conta", "id_cliente", "tipo_chave", "valor_chave",
            "data_registro", "ativa",
        ],
    },
    "raw_transactions": {
        "file": "raw_transactions.csv",
        "columns": [
            "id_transacao", "id_conta", "tipo", "direcao", "valor", "moeda",
            "contraparte", "descricao", "data_solicitacao", "data_conclusao", "status",
        ],
    },
    "raw_loans": {
        "file": "raw_loans.csv",
        "columns": [
            "id_emprestimo", "id_cliente", "valor_principal", "taxa_juros",
            "prazo_meses", "valor_parcela", "data_contratacao", "status", "pais",
        ],
    },
    "raw_loan_events": {
        "file": "raw_loan_events.csv",
        "columns": [
            "id_evento", "id_emprestimo", "tipo_evento", "valor", "data_evento", "observacao",
        ],
    },
    "raw_insurance": {
        "file": "raw_insurance.csv",
        "columns": [
            "id_apolice", "id_cliente", "tipo_seguro", "valor_cobertura",
            "valor_premio", "frequencia_premio", "qtd_beneficiarios",
            "data_inicio", "data_fim", "status", "pais",
        ],
    },
    "raw_insurance_events": {
        "file": "raw_insurance_events.csv",
        "columns": [
            "id_evento", "id_apolice", "tipo_evento", "valor", "data_evento", "observacao",
        ],
    },
}


def escape_value(val: str) -> str:
    """Escape aspas simples para SQL."""
    if val is None or val == "":
        return "NULL"
    return "'" + val.replace("'", "''") + "'"


# =============================================================================
# OPÇÃO 1: Via databricks-sql-connector (roda local na sua máquina)
# =============================================================================
def load_via_connector():
    from databricks import sql as dbsql

    conn = dbsql.connect(
        server_hostname=DATABRICKS_HOST.replace("https://", ""),
        http_path=DATABRICKS_HTTP_PATH,
        access_token=DATABRICKS_TOKEN,
    )
    cursor = conn.cursor()

    # Criar catalog e schema
    cursor.execute(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    cursor.execute(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
    print(f"✅ Catalog '{CATALOG}' e schema '{SCHEMA}' criados/verificados")

    for table_name, config in TABLES.items():
        filepath = os.path.join(CSV_DIR, config["file"])

        if not os.path.exists(filepath):
            print(f"⚠️  {config['file']} não encontrado em {CSV_DIR}, pulando...")
            continue

        fqn = f"{CATALOG}.{SCHEMA}.{table_name}"

        # Drop + Create (tudo STRING)
        cursor.execute(f"DROP TABLE IF EXISTS {fqn}")
        col_defs = ", ".join(f"`{col}` STRING" for col in config["columns"])
        cursor.execute(f"CREATE TABLE {fqn} ({col_defs})")

        # Ler CSV e inserir em batches
        with open(filepath, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            batch = []
            batch_size = 100

            for row in reader:
                values = ", ".join(
                    escape_value(row.get(col, "")) for col in config["columns"]
                )
                batch.append(f"({values})")

                if len(batch) >= batch_size:
                    cols = ", ".join(f"`{c}`" for c in config["columns"])
                    insert_sql = f"INSERT INTO {fqn} ({cols}) VALUES {', '.join(batch)}"
                    cursor.execute(insert_sql)
                    batch = []

            # Flush remaining
            if batch:
                cols = ", ".join(f"`{c}`" for c in config["columns"])
                insert_sql = f"INSERT INTO {fqn} ({cols}) VALUES {', '.join(batch)}"
                cursor.execute(insert_sql)

        cursor.execute(f"SELECT COUNT(*) FROM {fqn}")
        count = cursor.fetchone()[0]
        print(f"✅ {fqn}: {count} rows")

    cursor.close()
    conn.close()
    print("\n🎉 Todas as tabelas carregadas!")


# =============================================================================
# OPÇÃO 2: Via notebook Databricks (PySpark) — mais rápido para volumes maiores
#
# 1) Faça upload dos CSVs para um Volume:
#      /Volumes/<catalog>/<schema>/raw_csvs/
#
#    Ou via CLI:
#      databricks fs cp ./raw_customers.csv dbfs:/FileStore/nubank_raw/raw_customers.csv
#
# 2) Cole o código abaixo em uma célula do notebook e rode.
# =============================================================================
NOTEBOOK_CODE = """
# ============================================================
# COLE ISTO EM UM NOTEBOOK DATABRICKS
# ============================================================

CATALOG = "nubank_analytics"
SCHEMA = "raw"

# Ajuste o path pra onde você fez upload dos CSVs:
# Opção A — Unity Catalog Volume:
CSV_BASE_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/raw_csvs"
# Opção B — DBFS (legacy):
# CSV_BASE_PATH = "/dbfs/FileStore/nubank_raw"

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

tables = [
    "raw_customers",
    "raw_products",
    "raw_accounts",
    "raw_pix_keys",
    "raw_transactions",
    "raw_loans",
    "raw_loan_events",
    "raw_insurance",
    "raw_insurance_events",
]

for table in tables:
    csv_path = f"{CSV_BASE_PATH}/{table}.csv"

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")  # tudo string, dbt tipa
        .option("multiLine", "true")
        .option("escape", '"')
        .csv(csv_path)
    )

    fqn = f"{CATALOG}.{SCHEMA}.{table}"
    df.write.mode("overwrite").saveAsTable(fqn)

    count = spark.table(fqn).count()
    print(f"✅ {fqn}: {count} rows")

print("\\n🎉 Todas as tabelas carregadas!")
"""


# =============================================================================
# OPÇÃO 3: Gerar profiles.yml para dbt-databricks
# =============================================================================
def print_dbt_profile():
    profile = f"""
# profiles.yml — cole em ~/.dbt/profiles.yml
nubank_analytics:
  target: dev
  outputs:
    dev:
      type: databricks
      catalog: {CATALOG}
      schema: dev_{{{{ env_var('DBT_USER', 'seu_nome') }}}}  # cada dev tem seu schema
      host: {DATABRICKS_HOST.replace('https://', '')}
      http_path: {DATABRICKS_HTTP_PATH}
      token: {DATABRICKS_TOKEN}
      threads: 4

    prod:
      type: databricks
      catalog: {CATALOG}
      schema: marts
      host: {DATABRICKS_HOST.replace('https://', '')}
      http_path: {DATABRICKS_HTTP_PATH}
      token: {DATABRICKS_TOKEN}
      threads: 8
"""
    print(profile)


# =============================================================================
# MAIN
# =============================================================================
if __name__ == "__main__":
    import sys

    if "--notebook" in sys.argv:
        print("📓 Código para notebook Databricks:\n")
        print(NOTEBOOK_CODE)
    elif "--profile" in sys.argv:
        print("📝 profiles.yml para dbt-databricks:\n")
        print_dbt_profile()
    else:
        print("🚀 Carregando via databricks-sql-connector...\n")
        load_via_connector()
        print("\n📝 Dica: rode com --profile pra gerar o profiles.yml do dbt")
        print("📓 Dica: rode com --notebook pra gerar código PySpark pro notebook")
