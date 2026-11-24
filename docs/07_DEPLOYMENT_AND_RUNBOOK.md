# 🚀 07. Deployment & Operations Runbook

## 1. Prerequisites & Environment Matrix

* **Local Machine / VM:**
  * Docker & Docker Compose v2.20+
  * Python 3.11+
  * 8 GB+ RAM available for Airflow containers
* **AWS Services:**
  * Amazon S3 (5 buckets: `raw`, `bronze`, `clean`, `gold`, `platinum`)
  * AWS Glue 5.0 (PySpark 3.5 & Iceberg catalog enabled)
  * Amazon Athena v3
  * AWS IAM with S3 and Glue administrative privileges
* **Microsoft Analytics:**
  * Microsoft Fabric Capacity (F2 or trial)
  * Power BI Desktop (Latest version)

---

## 2. Step-by-Step Deployment Runbook

### Step 1: Clone Repository & Configure Environment
```bash
git clone https://github.com/your-username/shelfmind-intelligence.git
cd shelfmind-intelligence

# Copy template to .env
cp .env.example .env
```
Edit `.env` to configure your AWS keys and region:
```ini
AWS_DEFAULT_REGION=eu-north-1
S3_RAW_BUCKET=smart-lakehouse-raw
AWS_ACCESS_KEY_ID=YOUR_AWS_ACCESS_KEY
AWS_SECRET_ACCESS_KEY=YOUR_AWS_SECRET_KEY
DATA_GENERATOR_PROFILE=full
DATA_GENERATOR_SEED=42
DATA_INJECT_DEFECTS=true
```

### Step 2: Launch Local Apache Airflow Infrastructure
```bash
# Start CeleryExecutor, Redis, PostgreSQL, and Airflow containers
docker compose up -d

# Verify container health
docker compose ps
```
Access the Airflow web interface at: `http://localhost:8080`.

### Step 3: Run Local Data Simulation & Preflight Verification
```bash
# Install local dependencies
pip install -r requirements.txt

# Run the deterministic Egyptian FMCG simulation
python -m data_generator.generator --profile test --seed 7 --output-dir data/raw

# Run the 41-rule regression validator
python -m data_generator.validate data/raw
```

### Step 4: Execute the End-to-End Pipeline
* In the Airflow Web UI, unpause and trigger:
  `smart_warehouse_lakehouse_end_to_end`
* Or trigger via Docker CLI:
```bash
docker compose exec airflow-webserver airflow dags trigger smart_warehouse_lakehouse_end_to_end
```

### Step 5: Configure Microsoft Fabric OneLake Mirroring
1. Open your **Microsoft Fabric Workspace**.
2. Select **+ New Item** ➔ **Mirrored AWS Glue Catalog**.
3. Enter connection settings:
   * **Endpoint:** `https://glue.<your-region>.amazonaws.com/iceberg`
   * **AWS Account / Warehouse ID:** `<your-aws-account-id>`
   * **Authentication:** Access Key & Secret Key (or AWS IAM Role).
4. Once connected, Fabric creates shortcuts to all Iceberg tables in the `gold` and `platinum` databases.

### Step 6: Power BI Direct Lake & Custom Theme Setup
1. In the Mirrored Lakehouse, click **New Semantic Model**.
2. Select all 5 Dimensions, 4 Facts, 4 Marts, and the Platinum recommendations table.
3. Open Power BI Desktop, connect to the Fabric Semantic Model in **Direct Lake mode**.
4. Import the custom theme:
   * Go to **View** ➔ **Themes** ➔ **Browse for themes**.
   * Select [`smart_warehouse_theme.json`](../smart_warehouse_theme.json).

---

## 3. Operational Troubleshooting FAQ

### 1. `EndpointConnectionError / Could not connect to the endpoint URL`
* **Root Cause:** Incorrect AWS region specified in `.env` (e.g. `us-north-1` instead of `eu-north-1`).
* **Fix:** Verify your S3 bucket location using AWS CLI:
  ```bash
  aws s3api get-bucket-location --bucket <your-bucket-name>
  ```
  Update `AWS_DEFAULT_REGION` in `.env` and restart Docker.

### 2. `AccessDeniedException: User is not authorized to perform: s3:PutObject`
* **Root Cause:** IAM user running Airflow lacks write permissions on the target S3 bucket.
* **Fix:** Attach the `AmazonS3FullAccess` and `AWSGlueConsoleFullAccess` policies to the IAM user.

### 3. Power BI KPI shows `23 Billion EGP` instead of `632 Million EGP`
* **Root Cause:** Summing periodic daily snapshots across all calendar dates.
* **Fix:** Use the DAX pattern:
  `CALCULATE(SUM(...), [snapshot_date] = MAX([snapshot_date]))`.

### 4. Power BI relationship error: `Ambiguous paths between fact_sales and dim_product`
* **Root Cause:** Bidirectional (`Both`) cross-filtering between multiple fact tables and dimensions.
* **Fix:** Set relationship cross-filter direction to **`Single`** (Dimension ➔ Fact).
