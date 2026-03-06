import psycopg2
import psycopg2.extras
import json
import os
from dotenv import load_dotenv
from parsers import RawTransaction
from typing import List

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))


def get_connection():
    """
    Get a connection to the finance PostgreSQL database.
    Uses environment variables for configuration.
    """
    return psycopg2.connect(
        host=os.getenv("FINANCE_DB_HOST", "finance-postgres"),
        port=int(os.getenv("FINANCE_DB_PORT", "5432")),
        dbname=os.getenv("FINANCE_DB_NAME", "finance"),
        user=os.getenv("FINANCE_DB_USER", "finance"),
        password=os.getenv("FINANCE_DB_PASSWORD", "finance"),
    )


def load_to_bronze(transactions: List[RawTransaction]) -> dict:
    """
    Insert raw transactions into the bronze_transactions table.

    Uses INSERT ... ON CONFLICT DO NOTHING so re-running the same
    file never creates duplicates — the row_hash column enforces
    uniqueness at the database level.

    Returns a summary of what was inserted vs skipped.
    """
    inserted = 0
    skipped = 0

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            for transaction in transactions:
                row_hash = transaction.to_row_hash()
                try:
                    cur.execute("""
                        INSERT INTO bronze_transactions (
                            source_file,
                            bank_name,
                            raw_date,
                            raw_description,
                            raw_amount,
                            raw_category,
                            raw_reference,
                            raw_balance,
                            extra_fields,
                            file_row_number,
                            row_hash
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                        ON CONFLICT (row_hash) DO NOTHING
                    """, (
                        transaction.source_file,
                        transaction.bank_name,
                        transaction.raw_date,
                        transaction.raw_description,
                        transaction.raw_amount,
                        transaction.raw_category,
                        transaction.raw_reference,
                        transaction.raw_balance,
                        json.dumps(transaction.extra_fields),
                        transaction.file_row_number,
                        row_hash,
                    ))

                    if cur.rowcount > 0:
                        inserted += 1
                    else:
                        skipped += 1

                except Exception as e:
                    print(f"Error inserting row {transaction.file_row_number}: {e}")
                    skipped += 1

        conn.commit()
    finally:
        conn.close()

    return {
        "inserted": inserted,
        "skipped": skipped,
        "total": len(transactions),
    }


def ingest_file(filepath: str) -> dict:
    """
    Full ingestion pipeline for a single CSV file.
    Parses it and loads it to Bronze in one call.
    This is what Airflow will call.
    """
    from parsers import parse_csv_file

    print(f"Ingesting file: {filepath}")
    transactions = parse_csv_file(filepath)
    print(f"Parsed {len(transactions)} transactions")

    result = load_to_bronze(transactions)
    print(f"Bronze load complete: {result}")

    return result


if __name__ == "__main__":
    """
    Allow running directly for testing:
    python bronze_loader.py ../data/sample/monzo_sample.csv
    """
    import sys
    if len(sys.argv) < 2:
        print("Usage: python bronze_loader.py <path_to_csv>")
        sys.exit(1)

    filepath = sys.argv[1]
    result = ingest_file(filepath)
    print(f"\nIngestion complete: {result}")