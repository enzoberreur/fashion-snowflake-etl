from snowflake_config import SnowflakeConnection
from dotenv import load_dotenv

load_dotenv()

CHECKS = [
    ("Total rows", "SELECT COUNT(*) FROM SALES_DATA"),
    ("Revenue", "SELECT SUM(TOTAL_AMOUNT) FROM SALES_DATA"),
    ("Avg basket per customer", """
        SELECT AVG(customer_total) FROM (
            SELECT CUSTOMER_ID, SUM(TOTAL_AMOUNT) as customer_total 
            FROM SALES_DATA 
            GROUP BY CUSTOMER_ID
        )
    """),
    ("Price distribution", "SELECT MIN(TOTAL_AMOUNT), MAX(TOTAL_AMOUNT), MEDIAN(TOTAL_AMOUNT) FROM SALES_DATA"),
    ("Quantity analysis", "SELECT AVG(QUANTITY), MIN(QUANTITY), MAX(QUANTITY) FROM SALES_DATA"),
    ("Top products", "SELECT PRODUCT_NAME, COUNT(*) FROM SALES_DATA GROUP BY 1 ORDER BY 2 DESC LIMIT 5"),
    ("Channels", "SELECT CHANNEL, COUNT(*) FROM SALES_DATA GROUP BY 1 ORDER BY 2 DESC"),
    ("Sample", "SELECT * FROM SALES_DATA LIMIT 3")
]

def verify_data():
    with SnowflakeConnection() as sf:
        sf.connect()
        
        [sf.execute_query(cmd) for cmd in ["USE ROLE INGEST", "USE WAREHOUSE INGEST", "USE DATABASE INGEST", "USE SCHEMA INGEST"]]
        
        for desc, query in CHECKS:
            try:
                results = sf.execute_query(query)
                print(f"\n{desc}")
                [print(f"  {row}") for row in (results or ["No data"])[:5]]
            except Exception as e:
                print(f"{desc} ❌ {e}")
        
if __name__ == "__main__":
    verify_data()
