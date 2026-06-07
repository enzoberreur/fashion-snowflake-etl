"""Diagnose Snowflake table content and recent COPY errors.

Replaces the original snowflake_check_data.py: same human-readable output, but
all key loading and connection handling is delegated to the shared
``SnowflakeConnection`` so the credential-loading code path is unified.
"""
from __future__ import annotations

from dotenv import load_dotenv

from monogram_etl.config.snowflake import SnowflakeConnection
from monogram_etl.utils.logging import get_logger

load_dotenv()
logger = get_logger(__name__)


TABLES_BY_CATEGORY = {
    "DIRECT INGESTER (transactional)": [
        "SALES_DATA", "RETURNS_DATA", "REVIEWS_DATA", "INVENTORY_DATA",
    ],
    "SNOWPIPE INGESTER (reference)": [
        "PRODUCTS_DATA_SNOWPIPE", "CUSTOMERS_DATA_SNOWPIPE",
        "SUPPLIERS_DATA_SNOWPIPE", "STORES_DATA_SNOWPIPE",
        "PROMOTIONS_DATA_SNOWPIPE",
    ],
}


def check_snowflake_tables() -> None:
    """Print row counts, samples, stage cleanup status, recent COPY history."""
    with SnowflakeConnection(query_tag="monogram-diagnostics") as sf:
        print("🔍 DIAGNOSTIC DES TABLES SNOWFLAKE")
        print("=" * 60)

        for category, tables in TABLES_BY_CATEGORY.items():
            print(f"\n📊 {category}:")
            print("-" * 40)

            for table in tables:
                try:
                    rows = sf.execute_query(f"SELECT COUNT(*) FROM {table}")
                    row_count = rows[0][0] if rows else 0
                    if row_count > 0:
                        sample = sf.execute_query(f"SELECT * FROM {table} LIMIT 3")
                        print(f"  ✅ {table}: {row_count} lignes")
                        if sample:
                            print(f"     Échantillon: {len(sample[0])} colonnes")
                    else:
                        print(f"  ❌ {table}: VIDE (0 lignes)")
                except Exception as exc:  # noqa: BLE001
                    print(f"  ⚠️  {table}: ERREUR - {exc}")

        print("\n🗄️ STAGES TEMPORAIRES RÉCENTS:")
        print("-" * 40)
        try:
            stages = sf.execute_query("SHOW STAGES")
            temp_stages = [s for s in stages if "TEMP_STAGE" in str(s)]
            print(f"  Stages temporaires trouvés: {len(temp_stages)}")
            for i, stage in enumerate(temp_stages[:5]):
                print(f"    {i + 1}. {stage[1]}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ⚠️  Erreur récupération stages: {exc}")

        print("\n📈 HISTORIQUE DES REQUÊTES COPY RÉCENTES:")
        print("-" * 40)
        try:
            history = sf.execute_query(
                """
                SELECT query_text, execution_status, error_message, start_time, total_elapsed_time
                FROM information_schema.query_history
                WHERE query_text ILIKE '%COPY INTO%'
                  AND start_time >= DATEADD(hour, -2, CURRENT_TIMESTAMP())
                ORDER BY start_time DESC
                LIMIT 10
                """
            )
            if not history:
                print("  Aucune requête COPY récente trouvée")
            for query in history:
                status_icon = "✅" if query[1] == "SUCCESS" else "❌"
                table_name = "Unknown"
                if "COPY INTO" in (query[0] or ""):
                    parts = query[0].split("COPY INTO")[1].split("FROM")[0].strip()
                    table_name = parts.split()[0]
                print(f"  {status_icon} {table_name}: {query[1]} ({query[4]}ms)")
                if query[2]:
                    print(f"    Erreur: {query[2]}")
        except Exception as exc:  # noqa: BLE001
            print(f"  ⚠️  Erreur récupération historique: {exc}")


if __name__ == "__main__":
    check_snowflake_tables()
