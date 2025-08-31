# 📊 API Ingestion → S3 → Glue + Athena with AWS CDK

This project implements a **serverless architecture** in AWS to fetch data from the public API [`randomuser.me`](https://randomuser.me), store it in **Amazon S3** in **Parquet** format, catalog it with **AWS Glue**, and query it with **Amazon Athena**.

# 📊 API Ingestion → S3 → Glue + Athena with AWS CDK

![Python](https://img.shields.io/badge/python-3.11-blue.svg)
![AWS CDK](https://img.shields.io/badge/AWS%20CDK-v2-orange)
![Tests](https://img.shields.io/badge/tests-pytest-green)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

This project implements a **serverless architecture** in AWS to fetch data from the public API [`randomuser.me`](https://randomuser.me), store it in **Amazon S3** in **Parquet** format, catalog it with **AWS Glue**, and query it with **Amazon Athena**.

---

## ⚡ Quickstart

```bash
# 1) Clone this repository
git clone <YOUR_REPO_URL>
cd ea_estrategy

# 2) Create a Python virtual environment
python3 -m venv .venv
source .venv/bin/activate

# 3) Install CDK dependencies
cd cdk
pip install -r requirements.txt

# 4) Bootstrap your AWS account (only once per account/region)
cdk bootstrap aws://<ACCOUNT_ID>/<REGION>

# 5) Deploy all stacks (Lambda, S3, Glue, Athena)
cdk deploy --all

# 6) (Optional) Run tests locally
cd ..
pip install -r requirements-dev.txt
pytest -q
```

---

## 🚀 Architecture

- **AWS Lambda**
  - Function written in **Python 3.11**.
  - Calls `https://randomuser.me/api/?results=1`.
  - Flattens the JSON response and stores it in **S3** as **Parquet**.

- **Amazon S3**
  - Ingestion bucket for Parquet files (`ingesta/randomuser/`).
  - Separate bucket for **Athena query results**.

- **AWS Glue**
  - **Database** (`randomuser_db`) in the Data Catalog.
  - **Crawler** that scans S3 and automatically creates/updates tables with the schema.

- **Amazon Athena**
  - **WorkGroup** (`randomuser-wg`) configured to store query results in the results bucket.
  - Enables SQL queries over the ingested data.

- **Amazon Lakeformation**
  - **Enables governance** to s3 files, glue database and tables.
  - Enables pemissions management

---

## 📂 Project Structure

```text
src/app/               # Business logic (pipeline, storage, config, http client)
lambda/app_lambda/     # Lambda handler + runtime requirements
cdk/                   # Infrastructure with AWS CDK (stacks.py, stacks_glue.py, app.py)
tests/                 # Unit tests with pytest + moto
requirements-dev.txt   # Development dependencies (tests, mocks, linters)
