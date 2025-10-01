import os
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

class SnowflakeConnection:
    def __init__(self):
        self.connection = None
        self.cursor = None
        
    def load_private_key(self, path, passphrase=None):
        with open(path, 'rb') as f:
            key = serialization.load_pem_private_key(
                f.read(), 
                password=passphrase.encode() if passphrase else None,
                backend=default_backend()
            )
        return key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption()
        )
    
    def connect(self):
        private_key = self.load_private_key(
            os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'),
            os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE')
        )
        
        self.connection = snowflake.connector.connect(
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            user=os.getenv('SNOWFLAKE_USER'),
            private_key=private_key,
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        self.cursor = self.connection.cursor()
    
    def execute_query(self, query):
        self.cursor.execute(query)
        return self.cursor.fetchall()
    
    def execute_batch(self, query, data):
        self.cursor.executemany(query, data)
        return self.cursor.rowcount
    
    def close(self):
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
