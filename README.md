# 🏦 Fintech Analytics — dbt + Databricks

End-to-end data warehouse built with **dbt** on **Databricks**, transforming raw transactional data into a production-grade dimensional model inspired by a multi-country fintech operation.

## 🎯 What this project demonstrates

This isn't a toy dataset with 3 tables. It's a realistic data engineering project that covers:

- **Raw → Dimensional** transformation pipeline across 9 source tables → 19 mart tables
- **Multi-country support** (Brazil, Mexico, Colombia) with country-specific identifiers, currencies, and payment rails
- **LGPD/Privacy compliance** — PII hashed at the staging layer, validated by custom tests
- **SCD Type 2** address history via dbt snapshots
- **Incremental models** for high-volume financial transactions (merge strategy)
- **Custom macros** for transaction type normalization, direction resolution, and date parsing
- **Full test coverage** — generic tests on every PK/FK, singular tests for business rules and LGPD
- **CI/CD** with GitHub Actions running slim CI (`state:modified+`)
- **Documentation** browsable via `dbt docs`

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Databricks (Unity Catalog)                                     │
│                                                                 │
│  ┌──────────┐    ┌──────────┐    ┌────────────┐    ┌─────────┐ │
│  │  raw.*   │───▶│ staging  │───▶│intermediate│───▶│  marts  │ │
│  │ (9 CSVs) │    │ (views)  │    │(ephemeral) │    │(tables) │ │
│  └──────────┘    └──────────┘    └────────────┘    └─────────┘ │
│                                                         │       │
│                                              ┌──────────┼───┐   │
│                                              │ dims  │facts│   │
│                                              └──────────┴───┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Layers

| Layer | Materialization | Purpose |
|---|---|---|
| **staging** | view | 1:1 with sources. Clean, rename, cast, standardize. No joins. |
| **intermediate** | ephemeral | Deduplication, joins, reusable logic. Not persisted. |
| **marts** | table / incremental | Business-facing dimensions and facts. The final model. |
| **snapshots** | snapshot | SCD-2 for customer address history. |
| **seeds** | seed | Reference data (currencies, countries, transaction types). |

### Data Flow

```
raw_customers ──▶ stg_customers ──▶ int_customers_deduplicated ──┬▶ dim_customers
                                                                  ├▶ dim_customer_identifiers (hashed)
                                                                  └▶ dim_customer_address_history (SCD-2)

raw_accounts ───▶ stg_accounts ──▶ int_accounts_unified ──┬▶ fact_customer_products (SPINE)
raw_loans ──────▶ stg_loans ─────┘                         ├▶ product_details_checking_account
raw_insurance ──▶ stg_insurance ─┘                         ├▶ product_details_loan
                                                            └▶ product_details_insurance

raw_transactions ▶ stg_transactions ──▶ fact_financial_transactions (INCREMENTAL)
raw_loan_events ─▶ stg_loan_events ───▶ fact_loan_events
raw_insurance_ev ▶ stg_insurance_ev ──▶ fact_insurance_events
```

## 📊 Final Model (19 tables)

<details>
<summary><b>Reference Dimensions (3)</b></summary>

| Table | Source | Description |
|---|---|---|
| `dim_currency` | seed | ISO 4217 currencies (BRL, MXN, COP, USD) |
| `dim_country` | seed | Countries of operation, with regulatory regime |
| `dim_transaction_type` | seed | Payment types with rail and settlement info |

</details>

<details>
<summary><b>Geography (2)</b></summary>

| Table | Source | Description |
|---|---|---|
| `dim_state` | derived from customers | States with FK to country |
| `dim_city` | derived from customers | Cities with FK to state |

</details>

<details>
<summary><b>Customer Layer (3)</b></summary>

| Table | Source | Description |
|---|---|---|
| `dim_customers` | raw_customers | Core entity, no PII exposed |
| `dim_customer_identifiers` | raw_customers | CPF/RFC/CEDULA hashed (LGPD) |
| `dim_customer_address_history` | snapshot | SCD-2 with valid_from/valid_until |

</details>

<details>
<summary><b>Product Layer (3)</b></summary>

| Table | Source | Description |
|---|---|---|
| `dim_product` | raw_products | Product catalog |
| `dim_product_availability` | raw_products | N:N bridge product × country |
| `fact_customer_products` | accounts + loans + insurance | **THE SPINE** — one row per contract |

</details>

<details>
<summary><b>Product Details (3)</b></summary>

| Table | Source | Description |
|---|---|---|
| `product_details_checking_account` | raw_accounts | 1:1 with customer_product |
| `product_details_loan` | raw_loans | Principal, rate, term |
| `product_details_insurance` | raw_insurance | Coverage, premium, beneficiaries |

</details>

<details>
<summary><b>Account Satellites (2)</b></summary>

| Table | Source | Description |
|---|---|---|
| `dim_account_local_identifiers` | raw_accounts | Branch/account (BR), CLABE (MX) |
| `dim_pix_keys` | raw_pix_keys | PIX keys with hashed values |

</details>

<details>
<summary><b>Fact Tables (3)</b></summary>

| Table | Source | Description |
|---|---|---|
| `fact_financial_transactions` | raw_transactions | **INCREMENTAL** — unified money movements |
| `fact_loan_events` | raw_loan_events | Loan lifecycle events |
| `fact_insurance_events` | raw_insurance_events | Insurance lifecycle events |

</details>

## 🔧 Setup

### Prerequisites

- Databricks workspace (Community Edition works)
- Python 3.9+
- dbt-databricks

### 1. Install dbt

```bash
pip install dbt-databricks
```

### 2. Configure connection

```bash
# Generate profiles.yml template
python scripts/load_raw_to_databricks.py --profile
```

Copy the output to `~/.dbt/profiles.yml` and fill in your Databricks credentials.

### 3. Load raw data

**Option A — From your local machine:**
```bash
export DATABRICKS_HOST="https://adb-xxxx.azuredatabricks.net"
export DATABRICKS_HTTP_PATH="/sql/1.0/warehouses/xxxx"
export DATABRICKS_TOKEN="dapi_xxxx"
export CSV_DIR="./data"

pip install databricks-sql-connector
python scripts/load_raw_to_databricks.py
```

**Option B — Databricks notebook (faster):**
```bash
# Generate PySpark code to paste in a notebook
python scripts/load_raw_to_databricks.py --notebook
```

Upload CSVs to a Volume, paste the generated code, and run.

### 4. Run dbt

```bash
dbt deps          # install packages
dbt seed          # load reference data (currencies, countries, transaction types)
dbt snapshot      # run SCD-2 snapshots
dbt run           # build all models
dbt test          # run all tests
dbt docs generate # generate documentation
dbt docs serve    # browse at localhost:8080
```

Or all at once:
```bash
dbt build
```

## 🧪 Testing Strategy

| Category | What | How |
|---|---|---|
| **Primary keys** | Every PK is unique and not null | `unique`, `not_null` |
| **Foreign keys** | Every FK points to a valid parent | `relationships` |
| **Enums** | Status, direction, country codes | `accepted_values` |
| **LGPD** | No raw CPF/RFC in marts | Custom singular test |
| **Business rules** | Transaction amounts always positive | `dbt_expectations` |
| **Source freshness** | Raw data is up to date | `dbt source freshness` |

## 🧹 Data Quality Issues Resolved

The raw data has **intentional quality issues** that the dbt pipeline resolves:

| Issue | Where | Resolution |
|---|---|---|
| Country spelled 4+ ways | raw_customers | `CASE WHEN` mapping in staging |
| Document type inconsistent (CPF/cpf/Cpf) | raw_customers | `UPPER(TRIM())` in staging |
| Dates in 3 formats (ISO, BR, ISO-8601) | everywhere | Parse macro in staging |
| Names in CAPS, extra spaces | raw_customers | `TRIM(LOWER())` in staging |
| Duplicate customer rows | raw_customers | `ROW_NUMBER` dedup in intermediate |
| CPF in plaintext | raw_customers | `MD5()` hash in identifiers model |
| Transaction direction mixed (field vs sign) | raw_transactions | Custom `resolve_direction` macro |
| Transaction type as free text | raw_transactions | Custom `normalize_transaction_type` macro |
| Currency as BRL/R$/brl/reais/empty | raw_transactions | Mapping in staging |
| Interest rate as "3.5%" or "3.5" | raw_loans | `REPLACE` + `CAST` in staging |
| Boolean as sim/S/true/1/TRUE | raw_pix_keys | Normalize to `true`/`false` in staging |
| BR and MX account fields in same table | raw_accounts | Split into local_identifiers satellite |

## 📁 Project Structure

```
├── models/
│   ├── staging/              # 1:1 with sources, clean + standardize
│   │   ├── _sources.yml
│   │   ├── stg_customers.sql
│   │   ├── stg_products.sql
│   │   ├── stg_accounts.sql
│   │   ├── stg_pix_keys.sql
│   │   ├── stg_transactions.sql
│   │   ├── stg_loans.sql
│   │   ├── stg_loan_events.sql
│   │   ├── stg_insurance.sql
│   │   └── stg_insurance_events.sql
│   ├── intermediate/         # dedup, joins, reusable logic
│   │   ├── int_customers_deduplicated.sql
│   │   ├── int_accounts_unified.sql
│   │   └── int_geography_unique_locations.sql
│   └── marts/
│       ├── dimensions/       # dim_customers, dim_city, dim_state, etc.
│       ├── facts/            # fact_financial_transactions, fact_customer_products, etc.
│       └── product_details/  # product_details_checking_account, _loan, _insurance
├── macros/
│   ├── normalize_transaction_type.sql
│   ├── resolve_direction.sql
│   └── normalize_boolean.sql
├── seeds/                    # reference data (currencies, countries, transaction types)
├── snapshots/                # SCD-2 for customer address
├── tests/                    # singular tests (LGPD, business rules)
├── scripts/                  # data loading utilities
├── data/                     # raw CSVs (9 files)
├── .github/workflows/        # CI/CD
├── dbt_project.yml
├── packages.yml
└── README.md
```

## 🛠️ Tech Stack

| Tool | Purpose |
|---|---|
| **dbt Core** | Transformation framework |
| **Databricks** | Warehouse (Delta Lake) |
| **Unity Catalog** | Governance and metastore |
| **GitHub Actions** | CI/CD |
| **SQLFluff** | SQL linting |

## 📚 Packages Used

| Package | Purpose |
|---|---|
| `dbt_utils` | Surrogate keys, date spine, union_relations |
| `dbt_expectations` | Data quality assertions |
| `codegen` | Auto-generate YAML and staging models |

## 👤 About

Built as a portfolio project to demonstrate dbt + Databricks skills for Data Engineering and Analytics Engineering roles. The dimensional model covers a multi-country fintech operation with banking, credit, insurance, and investment products.

## 📄 License

MIT