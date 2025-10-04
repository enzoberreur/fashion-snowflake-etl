# 👑 Monogram Paris - ETL Pipeline

**Fashion Vintage Luxury** – ETL pipeline for authentication and sales of collectible fashion pieces (Chanel, Dior, Hermès, YSL).

---

## 👥 Team

* **Enzo Berreur** – Data Engineer
* **Sara Ben Abdelkader** – Data Analyst / ETL
* **Antonin Arroyo** – Back-end Developer
* **Nehemie Bikuka Prince** – Data Engineer

---

## 🏛️ Architecture

### 💎 Ingester Direct – Real-time Transactions

* **Data processed**: Sales, Returns, Reviews, Inventory
* **Purpose**: Immediate ingestion of transactions for near real-time tracking
* **Execution command**:

```bash
python3 ingester_direct.py --all-transactional
```

### 🏰 Ingester Snowpipe – Reference Data

* **Data processed**: Products, Customers, Suppliers, Stores, Promotions
* **Purpose**: Batch ingestion for updating reference data
* **Execution command**:

```bash
python3 ingester_snowpipe.py --all-reference
```

---

## 🚀 Full Usage

### 1️⃣ Generate data

```bash
python3 data_generator.py \
--sales 100000 \
--products 5000 \
--customers 10000 \
--stores 20 \
--promotions 100 \
--returns 5000 \
--reviews 15000 \
--inventory 10000
```

### 2️⃣ Ingest transactions

```bash
python3 ingester_direct.py --all-transactional --batch-size 10000
```

### 3️⃣ Ingest reference data

```bash
python3 ingester_snowpipe.py --all-reference --batch-size 2000
```

### 4️⃣ Verify data

```bash
python3 snowflake_check_data.py
```

---

## 📁 Project Structure

| File                      | Description                                                             |
| ------------------------- | ----------------------------------------------------------------------- |
| `data_generator.py`       | Generates consistent data for sales, customers, products, and inventory |
| `ingester_direct.py`      | Real-time ingestion of transactional data via SQL INSERT                |
| `ingester_snowpipe.py`    | Batch ingestion of reference data using Parquet + COPY                  |
| `snowflake_check_data.py` | Scripts for data validation and quality checks                          |
| `snowflake_config.py`     | Snowflake configuration and connection settings                         |

---

## ⚙️ Configuration

Create a `.env` file with your Snowflake credentials:

```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_WAREHOUSE=INGEST
SNOWFLAKE_DATABASE=INGEST
SNOWFLAKE_SCHEMA=INGEST
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.pem
```

