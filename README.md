# 👑 Monogram Paris - ETL Pipeline

**Fashion Vintage Luxury** - Système ETL pour authentification et vente de pièces de collection (Chanel, Dior, Hermès, YSL)

## 👥 Équipe
- **Enzo Berreur**
- **Sara BenAdbelkader** 
- **Antonin Arroyo**
- **Nehemie Bikuka Prince**

## 🏛️ Architecture

### **💎 Ingester Direct** - Transactions temps réel
- **Données** : Sales, Returns, Reviews, Inventory
- **Usage** : `python3 ingester_direct.py --all-transactional`

### **🏰 Ingester Snowpipe** - Données de référence  
- **Données** : Products, Customers, Suppliers, Stores, Promotions
- **Usage** : `python3 ingester_snowpipe.py --all-reference`

## 🚀 Utilisation

### **Génération + Ingestion Complète**
```bash
# 1. Générer 145k enregistrements 
python3 data_generator.py --sales 100000 --products 5000 --customers 10000 --stores 20 --promotions 100 --returns 5000 --reviews 15000 --inventory 10000

# 2. Ingester transactions 
python3 ingester_direct.py --all-transactional --batch-size 10000

# 3. Ingester référence 
python3 ingester_snowpipe.py --all-reference --batch-size 2000

# 4. Vérifier les données
python3 snowflake_check_data.py
```

## 📁 Structure

| Fichier | Rôle |
|---------|------|
| `data_generator.py` | Génère données fashion vintage cohérentes |
| `ingester_direct.py` | Ingestion temps réel (SQL INSERT) |
| `ingester_snowpipe.py` | Ingestion batch (Parquet + COPY) |
| `snowflake_check_data.py` | Validation des données |
| `snowflake_config.py` | Configuration Snowflake |

## ⚙️ Configuration

**Fichier `.env` requis :**
```env
SNOWFLAKE_ACCOUNT=your_account
SNOWFLAKE_USER=your_user
SNOWFLAKE_WAREHOUSE=INGEST
SNOWFLAKE_DATABASE=INGEST
SNOWFLAKE_SCHEMA=INGEST
SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/private_key.pem
```
