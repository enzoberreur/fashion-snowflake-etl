import json
import sys
import os
import argparse
from snowflake_config import SnowflakeConnection
from dotenv import load_dotenv

load_dotenv()

class MultiTableIngester:
    def __init__(self, batch_size=1000):
        self.batch_size = batch_size
        self.sf = SnowflakeConnection()
        
    def setup_tables(self):
        """Create all tables for the ingestion process"""
        self.sf.execute_query("USE ROLE INGEST")
        self.sf.execute_query("USE WAREHOUSE INGEST")
        self.sf.execute_query("USE DATABASE INGEST")
        self.sf.execute_query("USE SCHEMA INGEST")
        
        # Sales table
        sales_sql = """CREATE OR REPLACE TABLE SALES_DATA (
            SALE_ID VARCHAR(10), 
            SALE_DATE DATE, 
            CUSTOMER_ID VARCHAR(10),
            PRODUCT_ID VARCHAR(10), 
            PRODUCT_NAME VARCHAR(100), 
            QUANTITY INTEGER, 
            UNIT_PRICE DECIMAL(10,2), 
            TOTAL_AMOUNT DECIMAL(10,2), 
            CHANNEL VARCHAR(20),
            STORE_ID VARCHAR(10),
            COUNTRY VARCHAR(50), 
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""
        
        # Returns table
        returns_sql = """CREATE OR REPLACE TABLE RETURNS_DATA (
            RETURN_ID VARCHAR(10),
            SALE_ID VARCHAR(10),
            CUSTOMER_ID VARCHAR(10),
            PRODUCT_ID VARCHAR(10),
            RETURN_DATE DATE,
            REASON VARCHAR(50),
            CONDITION VARCHAR(20),
            REFUND_AMOUNT DECIMAL(10,2),
            REFUND_METHOD VARCHAR(30),
            PROCESSED_BY VARCHAR(100),
            STATUS VARCHAR(20),
            NOTES VARCHAR(500),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""
        
        # Reviews table
        reviews_sql = """CREATE OR REPLACE TABLE REVIEWS_DATA (
            REVIEW_ID VARCHAR(10),
            PRODUCT_ID VARCHAR(10),
            CUSTOMER_ID VARCHAR(10),
            RATING INTEGER,
            TITLE VARCHAR(100),
            COMMENT VARCHAR(1000),
            REVIEW_DATE DATE,
            VERIFIED_PURCHASE BOOLEAN,
            HELPFUL_VOTES INTEGER,
            STATUS VARCHAR(20),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""
        
        # Inventory table
        inventory_sql = """CREATE OR REPLACE TABLE INVENTORY_DATA (
            INVENTORY_ID VARCHAR(10),
            PRODUCT_ID VARCHAR(10),
            STORE_ID VARCHAR(10),
            CURRENT_STOCK INTEGER,
            RESERVED_STOCK INTEGER,
            REORDER_LEVEL INTEGER,
            MAX_STOCK_LEVEL INTEGER,
            LAST_RESTOCKED DATE,
            NEXT_DELIVERY_DATE DATE,
            WAREHOUSE_LOCATION VARCHAR(50),
            CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""

        # Direct ingester se concentre uniquement sur les données transactionnelles
        for sql in [sales_sql, returns_sql, reviews_sql, inventory_sql]:
            self.sf.execute_query(sql)
    
    def ingest_sales_data(self, filename):
        """Ingest sales data from JSON file"""
        print(f"Ingesting sales data from {filename}...")
        
        with open(filename, 'r') as f:
            batch = []
            total_inserted = 0
            
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    batch.append((
                        record['sale_id'], record['sale_date'], record['customer_id'],
                        record['product_id'], record['product_name'], record['quantity'], 
                        record['unit_price'], record['total_amount'], record['channel'],
                        record['store_id'], record['country']
                    ))
                    
                    if len(batch) >= self.batch_size:
                        self.sf.execute_batch(
                            "INSERT INTO SALES_DATA (SALE_ID, SALE_DATE, CUSTOMER_ID, PRODUCT_ID, PRODUCT_NAME, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, CHANNEL, STORE_ID, COUNTRY) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch
                        )
                        total_inserted += len(batch)
                        batch = []
                        print(f"Inserted batch: {total_inserted} sales records so far...")
            
            # Insert remaining records
            if batch:
                self.sf.execute_batch(
                    "INSERT INTO SALES_DATA (SALE_ID, SALE_DATE, CUSTOMER_ID, PRODUCT_ID, PRODUCT_NAME, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, CHANNEL, STORE_ID, COUNTRY) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    batch
                )
                total_inserted += len(batch)
        
        print(f"✓ Sales ingestion completed: {total_inserted} records")
        return total_inserted
    
    def ingest_returns_data(self, filename):
        """Ingest returns data from JSON file"""
        print(f"Ingesting returns data from {filename}...")
        
        with open(filename, 'r') as f:
            batch = []
            total_inserted = 0
            
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    batch.append((
                        record['return_id'], record['sale_id'], record['customer_id'],
                        record['product_id'], record['return_date'], record['reason'],
                        record['condition'], record['refund_amount'], record['refund_method'],
                        record['processed_by'], record['status'], record.get('notes')
                    ))
                    
                    if len(batch) >= self.batch_size:
                        self.sf.execute_batch(
                            "INSERT INTO RETURNS_DATA (RETURN_ID, SALE_ID, CUSTOMER_ID, PRODUCT_ID, RETURN_DATE, REASON, CONDITION, REFUND_AMOUNT, REFUND_METHOD, PROCESSED_BY, STATUS, NOTES) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch
                        )
                        total_inserted += len(batch)
                        batch = []
                        print(f"Inserted batch: {total_inserted} returns records so far...")
            
            if batch:
                self.sf.execute_batch(
                    "INSERT INTO RETURNS_DATA (RETURN_ID, SALE_ID, CUSTOMER_ID, PRODUCT_ID, RETURN_DATE, REASON, CONDITION, REFUND_AMOUNT, REFUND_METHOD, PROCESSED_BY, STATUS, NOTES) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    batch
                )
                total_inserted += len(batch)
        
        print(f"✓ Returns ingestion completed: {total_inserted} records")
        return total_inserted
    
    def ingest_reviews_data(self, filename):
        """Ingest reviews data from JSON file"""
        print(f"Ingesting reviews data from {filename}...")
        
        with open(filename, 'r') as f:
            batch = []
            total_inserted = 0
            
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    batch.append((
                        record['review_id'], record['product_id'], record['customer_id'],
                        record['rating'], record['title'], record.get('comment'),
                        record['review_date'], record['verified_purchase'], 
                        record['helpful_votes'], record['status']
                    ))
                    
                    if len(batch) >= self.batch_size:
                        self.sf.execute_batch(
                            "INSERT INTO REVIEWS_DATA (REVIEW_ID, PRODUCT_ID, CUSTOMER_ID, RATING, TITLE, COMMENT, REVIEW_DATE, VERIFIED_PURCHASE, HELPFUL_VOTES, STATUS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch
                        )
                        total_inserted += len(batch)
                        batch = []
                        print(f"Inserted batch: {total_inserted} reviews records so far...")
            
            if batch:
                self.sf.execute_batch(
                    "INSERT INTO REVIEWS_DATA (REVIEW_ID, PRODUCT_ID, CUSTOMER_ID, RATING, TITLE, COMMENT, REVIEW_DATE, VERIFIED_PURCHASE, HELPFUL_VOTES, STATUS) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    batch
                )
                total_inserted += len(batch)
        
        print(f"✓ Reviews ingestion completed: {total_inserted} records")
        return total_inserted
    
    def ingest_inventory_data(self, filename):
        """Ingest inventory data from JSON file"""
        print(f"Ingesting inventory data from {filename}...")
        
        with open(filename, 'r') as f:
            batch = []
            total_inserted = 0
            
            for line in f:
                if line.strip():
                    record = json.loads(line)
                    batch.append((
                        record['inventory_id'], record['product_id'], record['store_id'],
                        record['current_stock'], record['reserved_stock'], record['reorder_level'],
                        record['max_stock_level'], record['last_restocked'], 
                        record['next_delivery_date'], record['warehouse_location']
                    ))
                    
                    if len(batch) >= self.batch_size:
                        self.sf.execute_batch(
                            "INSERT INTO INVENTORY_DATA (INVENTORY_ID, PRODUCT_ID, STORE_ID, CURRENT_STOCK, RESERVED_STOCK, REORDER_LEVEL, MAX_STOCK_LEVEL, LAST_RESTOCKED, NEXT_DELIVERY_DATE, WAREHOUSE_LOCATION) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                            batch
                        )
                        total_inserted += len(batch)
                        batch = []
                        print(f"Inserted batch: {total_inserted} inventory records so far...")
            
            if batch:
                self.sf.execute_batch(
                    "INSERT INTO INVENTORY_DATA (INVENTORY_ID, PRODUCT_ID, STORE_ID, CURRENT_STOCK, RESERVED_STOCK, REORDER_LEVEL, MAX_STOCK_LEVEL, LAST_RESTOCKED, NEXT_DELIVERY_DATE, WAREHOUSE_LOCATION) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
                    batch
                )
                total_inserted += len(batch)
        
        print(f"✅ Inventory ingestion completed: {total_inserted} records inserted into INVENTORY_DATA table")
        return total_inserted

def main():
    parser = argparse.ArgumentParser(description='Direct Ingester for TRANSACTIONAL DATA using SQL INSERT')
    parser.add_argument('--sales', type=str, help='Sales JSON file to ingest')
    parser.add_argument('--returns', type=str, help='Returns JSON file to ingest')
    parser.add_argument('--reviews', type=str, help='Reviews JSON file to ingest')
    parser.add_argument('--inventory', type=str, help='Inventory JSON file to ingest')
    parser.add_argument('--all-transactional', action='store_true', help='Ingest all transactional data files (sales, returns, reviews, inventory)')
    parser.add_argument('--batch-size', type=int, default=1000, help='Batch size for processing')
    
    args = parser.parse_args()
    
    if args.all_transactional:
        # Ingérer tous les types de données transactionnelles
        transactional_files = {
            'sales': 'data/sales.json',
            'returns': 'data/returns.json',
            'reviews': 'data/reviews.json',
            'inventory': 'data/inventory.json'
        }
        
        print("🔄 Direct Ingester: Traitement de toutes les données transactionnelles")
        ingester = MultiTableIngester(batch_size=args.batch_size)
        
        try:
            ingester.setup_tables()
            print("✅ Tables setup completed")
            
            total_records = 0
            for data_type, filepath in transactional_files.items():
                if os.path.exists(filepath):
                    if data_type == 'sales':
                        total_records += ingester.ingest_sales_data(filepath)
                    elif data_type == 'returns':
                        total_records += ingester.ingest_returns_data(filepath)
                    elif data_type == 'reviews':
                        total_records += ingester.ingest_reviews_data(filepath)
                    elif data_type == 'inventory':
                        total_records += ingester.ingest_inventory_data(filepath)
                else:
                    print(f"⚠️  Fichier manquant: {filepath}")
                    
            print(f"\n🎉 Total ingestion completed: {total_records} records across all transactional tables")
            
        finally:
            ingester.sf.close()
        return
    
    if not any([args.sales, args.returns, args.reviews, args.inventory]):
        print("🔥 DIRECT INGESTER - Données transactionnelles")
        print("Spécifiez au moins un fichier: --sales, --returns, --reviews, --inventory")
        print("Ou utilisez --all-transactional pour traiter tous les fichiers transactionnels")
        return
    
    ingester = MultiTableIngester(batch_size=args.batch_size)
    
    try:
        ingester.setup_tables()
        print("✅ Tables setup completed")
        
        total_records = 0
        
        if args.sales:
            total_records += ingester.ingest_sales_data(args.sales)
        
        if args.returns:
            total_records += ingester.ingest_returns_data(args.returns)
        
        if args.reviews:
            total_records += ingester.ingest_reviews_data(args.reviews)
        
        if args.inventory:
            total_records += ingester.ingest_inventory_data(args.inventory)
        
        print(f"\n🎉 Total ingestion completed: {total_records} records across transactional tables")
        
    finally:
        ingester.sf.close()

if __name__ == "__main__":
    main()
