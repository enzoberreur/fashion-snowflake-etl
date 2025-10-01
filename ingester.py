import json
import sys
from snowflake_config import SnowflakeConnection
from data_generator import generate_sales_data
from dotenv import load_dotenv
import io
import contextlib

load_dotenv()

class SalesIngester:
    def __init__(self, batch_size):
        self.batch_size = batch_size
        self.sf = SnowflakeConnection()
        
    def setup_table(self):
        self.sf.execute_query("USE ROLE INGEST")
        self.sf.execute_query("USE WAREHOUSE INGEST")
        self.sf.execute_query("USE DATABASE INGEST")
        self.sf.execute_query("USE SCHEMA INGEST")
        
        sql = """CREATE TABLE IF NOT EXISTS SALES_DATA (
            SALE_ID VARCHAR(10), SALE_DATE DATE, CUSTOMER_ID VARCHAR(10),
            PRODUCT_ID VARCHAR(10), PRODUCT_NAME VARCHAR(100), QUANTITY INTEGER, 
            UNIT_PRICE DECIMAL(10,2), TOTAL_AMOUNT DECIMAL(10,2), CHANNEL VARCHAR(20), 
            COUNTRY VARCHAR(50), CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
        )"""
        self.sf.execute_query(sql)
    
    def generate_batch(self, count):
        batch = []
        for _ in range(count):
            f = io.StringIO()
            with contextlib.redirect_stdout(f):
                generate_sales_data()
            
            json_line = f.getvalue().strip()
            if json_line:
                record = json.loads(json_line)
                batch.append((
                    record['sale_id'], record['sale_date'], record['customer_id'],
                    record['product_id'], record['product_name'], record['quantity'], 
                    record['unit_price'], record['total_amount'], record['channel'], record['country']
                ))
        return batch
    
    def insert_batch(self, batch):
        sql = """INSERT INTO SALES_DATA
        (SALE_ID, SALE_DATE, CUSTOMER_ID, PRODUCT_ID, PRODUCT_NAME, QUANTITY, UNIT_PRICE, TOTAL_AMOUNT, CHANNEL, COUNTRY)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        return self.sf.execute_batch(sql, batch)
    
    def ingest(self, total_records):
        self.sf.connect()
        self.setup_table()
        
        total_inserted = 0
        remaining = total_records
        
        while remaining > 0:
            batch_count = min(self.batch_size, remaining)
            batch = self.generate_batch(batch_count)
            
            if batch:
                rows_inserted = self.insert_batch(batch)
                total_inserted += rows_inserted
                remaining -= batch_count
                print(f"Inserted {total_inserted}/{total_records} records")
            else:
                break
        
        self.sf.close()
        return total_inserted

def main():
    if len(sys.argv) < 2:
        print("Usage: python sales_ingester.py <records> [batch_size]")
        return
    
    records = int(sys.argv[1])
    batch_size = int(sys.argv[2]) if len(sys.argv) > 2 else 10000
    
    ingester = SalesIngester(batch_size)
    inserted = ingester.ingest(records)
    print(f"✓ Ingestion completed: {inserted} records inserted")

if __name__ == "__main__":
    main()