# Big Data Sales Analytics Pipeline

A full end-to-end Big Data pipeline built on a single-node Hadoop/YARN cluster, processing real Amazon product data through Spark, persisting it to a Hive Metastore backed by PostgreSQL, and exposing it to Power BI for interactive reporting.

---

## Architecture

```
Raw CSV (Amazon Products)
        │
        ▼
┌──────────────┐
│     HDFS     │  ← Distributed storage (Hadoop 3.4.1)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│    Spark     │  ← Distributed processing (Spark 3.5.8 via Spark Connect)
│   Connect    │  ← PySpark notebook: cleaning, modeling, saving
└──────┬───────┘
       │
       ▼
┌──────────────────────────┐
│  Hive Metastore          │  ← Table catalog (PostgreSQL backend)
│  + HDFS Warehouse        │  ← Persistent storage of Parquet tables
└──────┬───────────────────┘
       │
       ▼
┌──────────────┐
│ Thrift Server│  ← HiveServer2 protocol (port 10000)
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Power BI   │  ← Interactive dashboards and reporting
└──────────────┘
```

---

## Stack

| Component | Version | Role |
|---|---|---|
| Apache Hadoop | 3.4.1 | HDFS storage + YARN resource management |
| Apache Spark | 3.5.8 | Distributed data processing |
| Apache Hive | 3.1.3 | Metastore schema initialization |
| PostgreSQL | 14.x | Hive Metastore backend |
| PySpark | 3.5.8 | Python API for Spark |
| Power BI Desktop | Latest | Visualization and reporting |
| Ubuntu | 22.04 LTS | Host OS (VM, 8GB RAM, 4 vCPUs) |

---

## Dataset

**Source:** Amazon Products Sales Dataset (Kaggle)  
**File:** `amazon_products_sales_data_uncleaned.csv`  
**Raw rows:** 42,675  
**Clean rows:** 2,871 (after deduplication, null removal, and quality filters)  
**Size:** 38 MB

### Raw columns

| Column | Raw format | Problem |
|---|---|---|
| title | string | Clean |
| rating | "4.6 out of 5 stars" | Text, needs extraction |
| number_of_reviews | "2,457" | Comma-separated string |
| bought_in_last_month | "300+ bought in past month" | Text, needs extraction |
| current/discounted_price | "$89.68" | Dollar sign, string |
| listed_price | "$159.00" | Dollar sign, string |
| collected_at | "2025-08-21 11:14:29" | Datetime string |
| image_url / product_url | URLs | Dropped — no analytical value |

---

## Data Model (Star Schema)

```
                    ┌─────────────────┐
                    │   dim_products  │
                    │─────────────────│
                    │ product_id (PK) │
                    │ title           │
                    │ is_best_seller  │
                    │ is_sponsored    │
                    │ is_couponed     │
                    │ sustainability  │
                    └────────┬────────┘
                             │
┌──────────────┐    ┌────────▼────────┐    ┌─────────────────┐
│   dim_time   │    │   fact_sales    │    │ dim_price_tier  │
│──────────────│    │─────────────────│    │─────────────────│
│ date_id (PK) │◄───│ sale_id (PK)    │───►│ tier_id (PK)    │
│ collected_at │    │ product_id (FK) │    │ price_tier      │
│ year         │    │ date_id (FK)    │    │ (Budget /       │
│ month        │    │ tier_id (FK)    │    │  Mid-Range /    │
│ day          │    │ current_price   │    │  Premium /      │
└──────────────┘    │ listed_price    │    │  Luxury)        │
                    │ discount_pct    │    └─────────────────┘
                    │ rating          │
                    │ number_of_reviews│
                    │ bought_last_month│
                    └─────────────────┘
```

### Price tier definition

| Tier | Price range |
|---|---|
| Budget | < $30 |
| Mid-Range | $30 – $100 |
| Premium | $100 – $500 |
| Luxury | > $500 |

---

## Project Structure

```
├── amazon_sales_pipeline.ipynb   # PySpark notebook (cleaning + modeling)
├── start_servers.sh              # Shell script to start all services
├── amazon_sales_report.pbix      # Power BI report
└── README.md                     # This file
```

---

## Setup & Installation

### Prerequisites

- Ubuntu 22.04 VM with at least 8GB RAM
- Java 11
- Hadoop 3.4.1 (with HDFS + YARN configured)
- Spark 3.5.8
- Apache Hive 3.1.3 (for schematool)
- PostgreSQL 14
- Python 3.10+
- Power BI Desktop (Windows)

### 1. Clone the repository

```bash
git clone https://github.com/yourusername/bigdata-sales-pipeline.git
cd bigdata-sales-pipeline
```

### 2. Install Python dependencies

```bash
pip3 install pyspark[connect] jupyterlab notebook pandas
```

### 3. Set environment variables

Add to `~/.bashrc`:

```bash
export SPARK_HOME=/path/to/spark-3.5.8-bin-hadoop3
export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

export HIVE_HOME=/path/to/apache-hive-3.1.3-bin
export PATH=$PATH:$HIVE_HOME/bin

export HADOOP_CONF_DIR=$HADOOP_HOME/etc/hadoop
```

### 4. Set up PostgreSQL Metastore

```bash
sudo apt install postgresql -y
sudo systemctl start postgresql

sudo -u postgres psql -c "CREATE USER hiveuser WITH PASSWORD 'hivepassword';"
sudo -u postgres psql -c "CREATE DATABASE metastore;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE metastore TO hiveuser;"
```

Copy `hive-site.xml` to both Spark and Hive conf directories:

```bash
cp hive-site.xml $SPARK_HOME/conf/
cp hive-site.xml $HIVE_HOME/conf/
cp postgresql-42.7.1.jar $SPARK_HOME/jars/
cp postgresql-42.7.1.jar $HIVE_HOME/lib/
```

Initialize the Metastore schema:

```bash
schematool -dbType postgres -initSchema
```

### 5. Upload data to HDFS

```bash
start-dfs.sh
start-yarn.sh

hdfs dfs -mkdir -p /data/sales
hdfs dfs -mkdir -p /user/hive/warehouse
hdfs dfs -chmod g+w /user/hive/warehouse
hdfs dfs -put amazon_products_sales_data_uncleaned.csv /data/sales/
```

### 6. Run the pipeline

Use the interactive launcher:

```bash
chmod +x start_servers.sh
./start_servers.sh
```

Choose option **1** (Spark Connect) to run the notebook:

```bash
jupyter lab --ip=0.0.0.0 --no-browser --port=8888
```

Open `amazon_sales_pipeline.ipynb` and run all cells.

When done, run `start_servers.sh` again and choose option **2** (Thrift Server) for Power BI.

---

## Power BI Connection

1. Open Power BI Desktop
2. Get Data → Spark
3. Server: `<VM_IP>:10000`
4. Protocol: `Standard`
5. Load all 4 tables

### Dashboards

| Visual | Type | Insight |
|---|---|---|
| Top 10 most reviewed products | Bar Chart | Product popularity ranking |
| Sales distribution by price tier | Donut Chart | Catalogue price segmentation |
| Average rating by price tier | Column Chart | Does price correlate with quality? |
| Average discount by price tier | Treemap | Where are the best deals? |

---

## Key Findings

- **Budget products dominate** the catalogue (1,049 of 2,871 clean products)
- **Higher price ≠ higher rating**: Budget products average 4.54 stars vs Luxury at 4.41
- **Budget products offer the deepest discounts** (25% average vs 14% for Luxury)
- Data collection spanned **6 days** in August 2025 with August 27th having the most records (2,208)

---

## Challenges & Solutions

| Challenge | Solution |
|---|---|
| Spark Connect using in-memory catalog instead of Hive | Added `--conf spark.sql.catalogImplementation=hive` and all PostgreSQL params to the server launch command |
| `saveAsTable` failing with LOCATION_ALREADY_EXISTS | Cleared old HDFS warehouse directories with `hdfs dfs -rm -r` |
| `schematool` defaulting to Derby instead of PostgreSQL | Copied `hive-site.xml` and JDBC driver to `$HIVE_HOME/conf` and `$HIVE_HOME/lib` |
| CSV column shifting causing rating to contain product specs | Used pattern-matching regex `^\d+\.?\d*\s+out of 5` instead of generic number extraction |
| Prices up to 6 trillion due to dimension strings parsed as numbers | Applied strict filters: `current_price.between(0.01, 10000)` |
| `inferSchema=True` misreading string columns | Switched to `inferSchema=False` and manually cast every column |

---

## Wiem Ben Salem

Built as part of a Big Data university project — May 2026.
