import os, sys, logging
import json
import uuid
import argparse
import snowflake.connector
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import tempfile

from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization

load_dotenv()

logging.basicConfig(level=logging.INFO)


def connect_snow():
    private_key_path = os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH')
    
    with open(private_key_path, 'rb') as f:
        private_key = serialization.load_pem_private_key(
            f.read(),
            password=None
        )
    
    pkb = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        private_key=pkb,
        role="INGEST",
        database="INGEST",
        schema="INGEST", 
        warehouse="INGEST",
        session_parameters={'QUERY_TAG': 'py-snowpipe-sql-method'}, 
    )

def setup_snowflake_objects(snow):
    """Créer automatiquement les tables et pipes nécessaires"""
    cursor = snow.cursor()
    
    try:
        create_products_table = """
        CREATE TABLE IF NOT EXISTS PRODUCTS_DATA_SNOWPIPE (
            PRODUCT_ID VARCHAR(20) NOT NULL,
            NAME VARCHAR(255) NOT NULL,
            CATEGORY VARCHAR(100),
            SUBCATEGORY VARCHAR(100),
            BRAND VARCHAR(100),
            MATERIAL VARCHAR(100),
            COLOR VARCHAR(50),
            PRICE NUMBER(10,2),
            COST NUMBER(10,2),
            WEIGHT_KG NUMBER(8,2),
            DIMENSIONS_CM VARCHAR(50),
            SUPPLIER_ID VARCHAR(20),
            CREATED_DATE DATE,
            LAST_UPDATED DATE,
            IS_ACTIVE BOOLEAN,
            SKU VARCHAR(50),
            PRIMARY KEY (PRODUCT_ID)
        )
        """
        
        cursor.execute(create_products_table)
        print("✅ PRODUCTS_DATA_SNOWPIPE table created/verified")
        
        create_customers_table = """
        CREATE TABLE IF NOT EXISTS CUSTOMERS_DATA_SNOWPIPE (
            CUSTOMER_ID VARCHAR(20) NOT NULL,
            FIRST_NAME VARCHAR(100) NOT NULL,
            LAST_NAME VARCHAR(100) NOT NULL,
            EMAIL VARCHAR(255),
            PHONE VARCHAR(50),
            DATE_OF_BIRTH DATE,
            GENDER VARCHAR(10),
            ADDRESS VARCHAR(500),
            SEGMENT VARCHAR(50),
            REGISTRATION_DATE DATE,
            LAST_PURCHASE_DATE DATE,
            TOTAL_ORDERS NUMBER(10,0),
            LIFETIME_VALUE NUMBER(12,2),
            PREFERRED_CHANNEL VARCHAR(50),
            MARKETING_CONSENT BOOLEAN,
            PRIMARY KEY (CUSTOMER_ID)
        )
        """
        
        cursor.execute(create_customers_table)
        print("✅ CUSTOMERS_DATA_SNOWPIPE table created/verified")
        
        create_suppliers_table = """
        CREATE TABLE IF NOT EXISTS SUPPLIERS_DATA_SNOWPIPE (
            SUPPLIER_ID VARCHAR(20) NOT NULL,
            NAME VARCHAR(255) NOT NULL,
            CONTACT_PERSON VARCHAR(255),
            EMAIL VARCHAR(255),
            PHONE VARCHAR(50),
            ADDRESS VARCHAR(500),
            SPECIALTY VARCHAR(255),
            LEAD_TIME_DAYS NUMBER(5,0),
            MINIMUM_ORDER NUMBER(10,2),
            PAYMENT_TERMS VARCHAR(100),
            QUALITY_RATING NUMBER(3,2),
            ESTABLISHED_DATE DATE,
            IS_ACTIVE BOOLEAN,
            PRIMARY KEY (SUPPLIER_ID)
        )
        """
        
        cursor.execute(create_suppliers_table)
        print("✅ SUPPLIERS_DATA_SNOWPIPE table created/verified")
        
        create_stores_table = """
        CREATE TABLE IF NOT EXISTS STORES_DATA_SNOWPIPE (
            STORE_ID VARCHAR(20) NOT NULL,
            STORE_NAME VARCHAR(255) NOT NULL,
            MANAGER_NAME VARCHAR(255),
            ADDRESS VARCHAR(500),
            CITY VARCHAR(100),
            COUNTRY VARCHAR(100),
            PHONE VARCHAR(50),
            EMAIL VARCHAR(255),
            OPENING_DATE DATE,
            STORE_SIZE_SQM NUMBER(10,2),
            IS_ACTIVE BOOLEAN,
            PRIMARY KEY (STORE_ID)
        )
        """
        
        cursor.execute(create_stores_table)
        print("✅ STORES_DATA_SNOWPIPE table created/verified")
        
        create_promotions_table = """
        CREATE TABLE IF NOT EXISTS PROMOTIONS_DATA_SNOWPIPE (
            PROMOTION_ID VARCHAR(20) NOT NULL,
            NAME VARCHAR(255) NOT NULL,
            DESCRIPTION VARCHAR(500),
            DISCOUNT_TYPE VARCHAR(50),
            DISCOUNT_VALUE NUMBER(8,2),
            START_DATE DATE,
            END_DATE DATE,
            MINIMUM_PURCHASE NUMBER(10,2),
            IS_ACTIVE BOOLEAN,
            CREATED_DATE DATE,
            PRIMARY KEY (PROMOTION_ID)
        )
        """
        
        cursor.execute(create_promotions_table)
        print("✅ PROMOTIONS_DATA_SNOWPIPE table created/verified")
        
        print("ℹ️ Utilisation de la méthode SQL COPY pour l'ingestion (Snowpipe alternatif)")
        
    except Exception as e:
        print(f"❌ Error setting up Snowflake objects: {e}")
        logging.error(f"Error setting up Snowflake objects: {e}")
    finally:
        cursor.close()

def save_to_snowflake_via_sql(snow, batch, temp_dir, table_name):
    """Méthode alternative : Upload fichier Parquet puis COPY via SQL (simule Snowpipe)"""
    logging.info(f'Inserting batch to {table_name} via SQL COPY (Snowpipe alternative)')
    
    # Créer DataFrame et fichier Parquet
    pandas_df = pd.DataFrame(batch, columns=[
        "PRODUCT_ID", "NAME", "CATEGORY", "SUBCATEGORY", "BRAND", "MATERIAL", 
        "COLOR", "PRICE", "COST", "WEIGHT_KG", "DIMENSIONS_CM", "SUPPLIER_ID",
        "CREATED_DATE", "LAST_UPDATED", "IS_ACTIVE", "SKU"
    ])
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"products_{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"
    
    # Écrire le fichier Parquet
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')
    
    cursor = snow.cursor()
    try:
        # Créer un stage temporaire
        stage_name = f"TEMP_STAGE_{uuid.uuid4().hex[:8]}"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
        
        # Upload du fichier vers le stage
        put_command = f"PUT 'file://{out_path}' @{stage_name}"
        cursor.execute(put_command)
        logging.info(f"File uploaded to stage {stage_name}")
        
        # COPY via SQL 
        copy_command = f"""
        COPY INTO {table_name}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT=(TYPE='PARQUET')
        MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
        """
        
        result = cursor.execute(copy_command)
        copy_result = cursor.fetchone()
        # Le résultat COPY peut varier, utilisons la taille du batch comme approximation
        rows_loaded = len(batch)
        logging.info(f"SQL COPY completed: {rows_loaded} rows loaded (estimated from batch size)")
        
        # Nettoyer le stage
        cursor.execute(f"DROP STAGE {stage_name}")
        
        return rows_loaded
        
    finally:
        # Nettoyer le fichier local
        if os.path.exists(out_path):
            os.unlink(out_path)

def process_products_sql_method(filename, batch_size):
    """Process products avec la méthode SQL (alternative à Snowpipe REST)"""
    print(f"Processing products from {filename} with SQL method (Snowpipe alternative)")
    print(f"Batch size: {batch_size}")
    
    snow = connect_snow()
    
    # Configurer automatiquement les objets Snowflake nécessaires
    setup_snowflake_objects(snow)
    
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    total_processed = 0
    
    try:
        with open(filename, 'r') as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    batch.append((
                        record['product_id'], record['name'], record['category'],
                        record['subcategory'], record['brand'], record['material'],
                        record['color'], record['price'], record['cost'],
                        record['weight_kg'], record['dimensions_cm'], record['supplier_id'],
                        record['created_date'], record['last_updated'], record['is_active'], record['sku']
                    ))
                    
                    if len(batch) >= batch_size:
                        rows_loaded = save_to_snowflake_via_sql(snow, batch, temp_dir, 'PRODUCTS_DATA_SNOWPIPE')
                        total_processed += rows_loaded
                        batch = []
                        print(f"Processed {total_processed} records so far...")
        
        # Process remaining records
        if batch:
            rows_loaded = save_to_snowflake_via_sql(snow, batch, temp_dir, 'PRODUCTS_DATA_SNOWPIPE')
            total_processed += rows_loaded
        
        print(f"✅ SQL Snowpipe alternative completed: {total_processed} records processed")
        
    except Exception as e:
        print(f"❌ Error during SQL processing: {e}")
        logging.error(f"Error during SQL processing: {e}")
    finally:
        temp_dir.cleanup()
        snow.close()

def process_any_data_type(filename, data_type, batch_size):
    """Process any type of data with automatic table creation"""
    print(f"Processing {data_type} from {filename} with SQL method (Snowpipe alternative)")
    print(f"Batch size: {batch_size}")
    
    snow = connect_snow()
    setup_snowflake_objects(snow)
    
    batch = []
    temp_dir = tempfile.TemporaryDirectory()
    total_processed = 0
    
    # Mapping des colonnes par type de données
    column_mappings = {
        'products': ["PRODUCT_ID", "NAME", "CATEGORY", "SUBCATEGORY", "BRAND", "MATERIAL", 
                    "COLOR", "PRICE", "COST", "WEIGHT_KG", "DIMENSIONS_CM", "SUPPLIER_ID",
                    "CREATED_DATE", "LAST_UPDATED", "IS_ACTIVE", "SKU"],
        'customers': ["CUSTOMER_ID", "FIRST_NAME", "LAST_NAME", "EMAIL", "PHONE", 
                     "DATE_OF_BIRTH", "GENDER", "ADDRESS", "SEGMENT", "REGISTRATION_DATE",
                     "LAST_PURCHASE_DATE", "TOTAL_ORDERS", "LIFETIME_VALUE", "PREFERRED_CHANNEL", "MARKETING_CONSENT"],
        'suppliers': ["SUPPLIER_ID", "NAME", "CONTACT_PERSON", "EMAIL", "PHONE", "ADDRESS",
                     "SPECIALTY", "LEAD_TIME_DAYS", "MINIMUM_ORDER", "PAYMENT_TERMS", 
                     "QUALITY_RATING", "ESTABLISHED_DATE", "IS_ACTIVE"],
        'stores': ["STORE_ID", "STORE_NAME", "MANAGER_NAME", "ADDRESS", "CITY", "COUNTRY",
                  "PHONE", "EMAIL", "OPENING_DATE", "STORE_SIZE_SQM", "IS_ACTIVE"],
        'promotions': ["PROMOTION_ID", "NAME", "DESCRIPTION", "DISCOUNT_TYPE", "DISCOUNT_VALUE",
                      "START_DATE", "END_DATE", "MINIMUM_PURCHASE", "IS_ACTIVE", "CREATED_DATE"]
    }
    
    table_names = {
        'products': 'PRODUCTS_DATA_SNOWPIPE',
        'customers': 'CUSTOMERS_DATA_SNOWPIPE', 
        'suppliers': 'SUPPLIERS_DATA_SNOWPIPE',
        'stores': 'STORES_DATA_SNOWPIPE',
        'promotions': 'PROMOTIONS_DATA_SNOWPIPE'
    }
    
    if data_type not in column_mappings:
        print(f"❌ Data type '{data_type}' not supported. Supported: {list(column_mappings.keys())}")
        return
    
    columns = column_mappings[data_type]
    table_name = table_names[data_type]
    
    try:
        with open(filename, 'r') as f:
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    
                    # Construire le tuple selon le type de données
                    if data_type == 'products':
                        batch.append((
                            record['product_id'], record['name'], record['category'],
                            record['subcategory'], record['brand'], record['material'],
                            record['color'], record['price'], record['cost'],
                            record['weight_kg'], record['dimensions_cm'], record['supplier_id'],
                            record['created_date'], record['last_updated'], record['is_active'], record['sku']
                        ))
                    elif data_type == 'customers':
                        batch.append((
                            record['customer_id'], record['first_name'], record['last_name'],
                            record['email'], record['phone'], record['date_of_birth'],
                            record['gender'], record['address'], record['segment'],
                            record['registration_date'], record['last_purchase_date'],
                            record['total_orders'], record['lifetime_value'], 
                            record['preferred_channel'], record['marketing_consent']
                        ))
                    elif data_type == 'suppliers':
                        batch.append((
                            record['supplier_id'], record['name'], record['contact_person'],
                            record['email'], record['phone'], record['address'],
                            record['specialty'], record['lead_time_days'], record['minimum_order'],
                            record['payment_terms'], record['quality_rating'],
                            record['established_date'], record['is_active']
                        ))
                    elif data_type == 'stores':
                        # Support pour les deux formats d'address (string ou dict)
                        if isinstance(record.get('address'), str):
                            # Si address est une string (peut être JSON), utiliser telle quelle
                            address_str = record.get('address', '')
                        elif isinstance(record.get('address'), dict):
                            # Si address est un dict
                            address_str = f"{record['address'].get('street', '')}, {record['address'].get('city', '')}, {record['address'].get('country', '')}".strip(', ')
                        else:
                            address_str = ''
                        
                        batch.append((
                            record['store_id'], 
                            record.get('store_name') or record.get('name', ''),  # Support des deux noms de champs
                            record['manager_name'],
                            address_str, 
                            record['city'], 
                            record['country'],
                            record['phone'], 
                            record['email'], 
                            record['opening_date'],
                            record.get('store_size_sqm') or record.get('square_meters', 0), 
                            record['is_active']
                        ))
                    elif data_type == 'promotions':
                        batch.append((
                            record['promotion_id'], 
                            record['name'], 
                            record['description'],
                            record.get('discount_type') or record.get('type', ''),  # Support des deux noms de champs
                            record['discount_value'], 
                            record['start_date'],
                            record['end_date'], 
                            record['minimum_purchase'], 
                            record['is_active'],
                            record.get('created_date') or record['start_date']  # Utiliser created_date si disponible, sinon start_date
                        ))
                    
                    if len(batch) >= batch_size:
                        rows_loaded = save_to_snowflake_generic(snow, batch, temp_dir, table_name, columns)
                        total_processed += rows_loaded
                        batch = []
                        print(f"Processed {total_processed} {data_type} records so far...")
        
        # Process remaining records
        if batch:
            rows_loaded = save_to_snowflake_generic(snow, batch, temp_dir, table_name, columns)
            total_processed += rows_loaded
        
        print(f"✅ {data_type.title()} Snowpipe alternative completed: {total_processed} records processed")
        
    except Exception as e:
        print(f"❌ Error during {data_type} processing: {e}")
        logging.error(f"Error during {data_type} processing: {e}")
    finally:
        temp_dir.cleanup()
        snow.close()

def save_to_snowflake_generic(snow, batch, temp_dir, table_name, columns):
    """Version générique de sauvegarde pour tous types de données"""
    logging.info(f'Inserting batch to {table_name} via SQL COPY (Snowpipe alternative)')
    
    # Créer DataFrame et fichier Parquet
    pandas_df = pd.DataFrame(batch, columns=columns)
    arrow_table = pa.Table.from_pandas(pandas_df)
    file_name = f"{table_name.lower()}_{str(uuid.uuid1())}.parquet"
    out_path = f"{temp_dir.name}/{file_name}"
    
    # Écrire le fichier Parquet
    pq.write_table(arrow_table, out_path, use_dictionary=False, compression='SNAPPY')
    
    cursor = snow.cursor()
    try:
        # Créer un stage temporaire
        stage_name = f"TEMP_STAGE_{uuid.uuid4().hex[:8]}"
        cursor.execute(f"CREATE OR REPLACE TEMPORARY STAGE {stage_name}")
        
        # Upload du fichier vers le stage
        put_command = f"PUT 'file://{out_path}' @{stage_name}"
        cursor.execute(put_command)
        logging.info(f"File uploaded to stage {stage_name}")
        
        # COPY via SQL (équivalent à Snowpipe)
        copy_command = f"""
        COPY INTO {table_name}
        FROM @{stage_name}/{file_name}
        FILE_FORMAT=(TYPE='PARQUET')
        MATCH_BY_COLUMN_NAME=CASE_SENSITIVE
        """
        
        result = cursor.execute(copy_command)
        copy_result = cursor.fetchone()
        rows_loaded = len(batch)
        logging.info(f"SQL COPY completed: {rows_loaded} rows loaded (estimated from batch size)")
        
        # Nettoyer le stage
        cursor.execute(f"DROP STAGE {stage_name}")
        
        return rows_loaded
        
    finally:
        # Nettoyer le fichier local
        if os.path.exists(out_path):
            os.unlink(out_path)

def main():
    parser = argparse.ArgumentParser(description='Snowpipe alternative for REFERENCE DATA using SQL COPY + Parquet')
    parser.add_argument('--products', type=str, help='Products JSON file to ingest')
    parser.add_argument('--customers', type=str, help='Customers JSON file to ingest')  
    parser.add_argument('--suppliers', type=str, help='Suppliers JSON file to ingest')
    parser.add_argument('--stores', type=str, help='Stores JSON file to ingest')
    parser.add_argument('--promotions', type=str, help='Promotions JSON file to ingest')
    parser.add_argument('--all-reference', action='store_true', help='Ingest all reference data files (products, customers, suppliers, stores, promotions)')
    parser.add_argument('--batch-size', type=int, default=100, help='Batch size for processing')
    
    args = parser.parse_args()
    
    if args.all_reference:
        # Ingérer tous les types de données de référence
        reference_files = {
            'products': 'data/products.json',
            'customers': 'data/customers.json',
            'suppliers': 'data/suppliers.json', 
            'stores': 'data/stores.json',
            'promotions': 'data/promotions.json'
        }
        
        print("🔄 Snowpipe Ingester: Traitement de toutes les données de référence")
        for data_type, filepath in reference_files.items():
            if os.path.exists(filepath):
                process_any_data_type(filepath, data_type, args.batch_size)
            else:
                print(f"⚠️  Fichier manquant: {filepath}")
        return
    
    if not any([args.products, args.customers, args.suppliers, args.stores, args.promotions]):
        print("❄️  SNOWPIPE INGESTER - Données de référence")
        print("Spécifiez au moins un fichier: --products, --customers, --suppliers, --stores, --promotions")
        print("Ou utilisez --all-reference pour traiter tous les fichiers de référence")
        return
    
    if args.products:
        process_any_data_type(args.products, 'products', args.batch_size)
    
    if args.customers:
        process_any_data_type(args.customers, 'customers', args.batch_size)
        
    if args.suppliers:
        process_any_data_type(args.suppliers, 'suppliers', args.batch_size)
        
    if args.stores:
        process_any_data_type(args.stores, 'stores', args.batch_size)
        
    if args.promotions:
        process_any_data_type(args.promotions, 'promotions', args.batch_size)

if __name__ == "__main__":
    main()
