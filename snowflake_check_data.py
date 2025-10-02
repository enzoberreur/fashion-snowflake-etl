#!/usr/bin/env python3
"""
Script pour vérifier le contenu des tables Snowflake et diagnostiquer 
pourquoi certaines tables sont vides après l'ingestion Snowpipe
"""

import snowflake.connector
import os
from dotenv import load_dotenv
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

load_dotenv()

def load_private_key():
    """Charger la clé privée RSA"""
    with open(os.getenv('SNOWFLAKE_PRIVATE_KEY_PATH'), 'rb') as f:
        key = serialization.load_pem_private_key(
            f.read(), 
            password=os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE').encode() if os.getenv('SNOWFLAKE_PRIVATE_KEY_PASSPHRASE') else None,
            backend=default_backend()
        )
    return key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption()
    )

def check_snowflake_tables():
    """Vérifier le contenu des tables Snowflake"""
    
    private_key = load_private_key()
    
    conn = snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        private_key=private_key,
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA')
    )
    
    cursor = conn.cursor()
    
    try:
        # Définir le contexte
        cursor.execute("USE ROLE INGEST")
        cursor.execute("USE WAREHOUSE INGEST")
        cursor.execute("USE DATABASE INGEST")
        cursor.execute("USE SCHEMA INGEST")
        
        print("🔍 DIAGNOSTIC DES TABLES SNOWFLAKE")
        print("="*60)
        
        # Tables à vérifier
        tables_to_check = {
            'DIRECT INGESTER (transactional)': [
                'SALES_DATA', 'RETURNS_DATA', 'REVIEWS_DATA', 'INVENTORY_DATA'
            ],
            'SNOWPIPE INGESTER (reference)': [
                'PRODUCTS_DATA_SNOWPIPE', 'CUSTOMERS_DATA_SNOWPIPE', 
                'SUPPLIERS_DATA_SNOWPIPE', 'STORES_DATA_SNOWPIPE', 'PROMOTIONS_DATA_SNOWPIPE'
            ]
        }
        
        for category, tables in tables_to_check.items():
            print(f"\n📊 {category}:")
            print("-" * 40)
            
            for table in tables:
                try:
                    # Compter les lignes
                    cursor.execute(f"SELECT COUNT(*) FROM {table}")
                    count_result = cursor.fetchone()
                    row_count = count_result[0] if count_result else 0
                    
                    # Obtenir quelques échantillons si la table contient des données
                    if row_count > 0:
                        cursor.execute(f"SELECT * FROM {table} LIMIT 3")
                        samples = cursor.fetchall()
                        print(f"  ✅ {table}: {row_count} lignes")
                        if samples:
                            print(f"     Échantillon: {len(samples[0])} colonnes")
                    else:
                        print(f"  ❌ {table}: VIDE (0 lignes)")
                        
                except Exception as table_error:
                    print(f"  ⚠️  {table}: ERREUR - {table_error}")
        
        # Vérifier les stages temporaires récents
        print(f"\n🗄️ STAGES TEMPORAIRES RÉCENTS:")
        print("-" * 40)
        try:
            cursor.execute("SHOW STAGES")
            stages = cursor.fetchall()
            temp_stages = [stage for stage in stages if 'TEMP_STAGE' in str(stage)]
            print(f"  Stages temporaires trouvés: {len(temp_stages)}")
            
            if temp_stages:
                for i, stage in enumerate(temp_stages[:5]):  # Afficher les 5 premiers
                    print(f"    {i+1}. {stage[1]}")  # Le nom est dans la colonne 1
                    
        except Exception as stage_error:
            print(f"  ⚠️  Erreur récupération stages: {stage_error}")
            
        # Vérifier les erreurs de COPY récentes dans l'historique
        print(f"\n📈 HISTORIQUE DES REQUÊTES COPY RÉCENTES:")
        print("-" * 40)
        try:
            cursor.execute("""
                SELECT query_text, execution_status, error_message, start_time, total_elapsed_time
                FROM information_schema.query_history 
                WHERE query_text ILIKE '%COPY INTO%'
                AND start_time >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
                ORDER BY start_time DESC
                LIMIT 10
            """)
            
            copy_queries = cursor.fetchall()
            if copy_queries:
                for i, query in enumerate(copy_queries):
                    status = "✅" if query[1] == "SUCCESS" else "❌"
                    table_name = "Unknown"
                    if "COPY INTO" in query[0]:
                        # Extraire le nom de la table
                        parts = query[0].split("COPY INTO")[1].split("FROM")[0].strip()
                        table_name = parts.split()[0]
                    
                    print(f"  {status} {table_name}: {query[1]} ({query[4]}ms)")
                    if query[2]:  # Si il y a un message d'erreur
                        print(f"    Erreur: {query[2]}")
            else:
                print("  Aucune requête COPY récente trouvée")
                
        except Exception as history_error:
            print(f"  ⚠️  Erreur récupération historique: {history_error}")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    check_snowflake_tables()
