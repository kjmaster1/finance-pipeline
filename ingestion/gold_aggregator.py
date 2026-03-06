import psycopg2
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))


def get_connection():
    return psycopg2.connect(
        host=os.getenv("FINANCE_DB_HOST", "finance-postgres"),
        port=int(os.getenv("FINANCE_DB_PORT", "5432")),
        dbname=os.getenv("FINANCE_DB_NAME", "finance"),
        user=os.getenv("FINANCE_DB_USER", "finance"),
        password=os.getenv("FINANCE_DB_PASSWORD", "finance"),
    )


def compute_monthly_summary() -> dict:
    """
    Aggregate Silver transactions into monthly spending by category.

    Uses INSERT ... ON CONFLICT DO UPDATE (a full upsert) so
    re-running always produces fresh, correct numbers.
    This is different from Bronze where we used DO NOTHING —
    here we WANT to overwrite with updated figures.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gold_monthly_summary (
                    year, month, category, bank_name,
                    total_spent, transaction_count, avg_transaction
                )
                SELECT
                    EXTRACT(YEAR FROM transaction_date)::INTEGER  AS year,
                    EXTRACT(MONTH FROM transaction_date)::INTEGER AS month,
                    category,
                    bank_name,
                    ABS(SUM(amount))                              AS total_spent,
                    COUNT(*)                                      AS transaction_count,
                    ABS(AVG(amount))                              AS avg_transaction
                FROM silver_transactions
                WHERE is_debit = TRUE
                GROUP BY
                    EXTRACT(YEAR FROM transaction_date),
                    EXTRACT(MONTH FROM transaction_date),
                    category,
                    bank_name
                ON CONFLICT (year, month, category, bank_name)
                DO UPDATE SET
                    total_spent       = EXCLUDED.total_spent,
                    transaction_count = EXCLUDED.transaction_count,
                    avg_transaction   = EXCLUDED.avg_transaction,
                    computed_at       = NOW()
            """)
            monthly_rows = cur.rowcount

        conn.commit()
        print(f"Monthly summary: {monthly_rows} rows upserted")
        return {"monthly_rows": monthly_rows}

    finally:
        conn.close()


def compute_recurring_payments() -> dict:
    """
    Identify recurring payments by finding descriptions
    that appear multiple times or are flagged as recurring.

    Groups by description and bank, computing average amount
    and how many times we've seen each payment.
    """
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO gold_recurring_payments (
                    description, avg_amount, frequency,
                    last_seen, times_seen, bank_name
                )
                SELECT
                    description,
                    ABS(AVG(amount))        AS avg_amount,
                    CASE
                        WHEN COUNT(*) >= 12 THEN 'monthly'
                        WHEN COUNT(*) >= 4  THEN 'quarterly'
                        ELSE 'occasional'
                    END                     AS frequency,
                    MAX(transaction_date)   AS last_seen,
                    COUNT(*)                AS times_seen,
                    bank_name
                FROM silver_transactions
                WHERE is_recurring = TRUE
                OR is_debit = TRUE
                GROUP BY description, bank_name
                HAVING COUNT(*) >= 1
                AND (
                    -- Either flagged as recurring
                    BOOL_OR(is_recurring) = TRUE
                    -- Or appears more than once
                    OR COUNT(*) > 1
                )
                ON CONFLICT (description, bank_name)
                DO UPDATE SET
                    avg_amount  = EXCLUDED.avg_amount,
                    frequency   = EXCLUDED.frequency,
                    last_seen   = EXCLUDED.last_seen,
                    times_seen  = EXCLUDED.times_seen,
                    computed_at = NOW()
            """)
            recurring_rows = cur.rowcount

        conn.commit()
        print(f"Recurring payments: {recurring_rows} rows upserted")
        return {"recurring_rows": recurring_rows}

    finally:
        conn.close()


def run_gold_aggregations() -> dict:
    """
    Run all Gold layer aggregations.
    Called by Airflow as the final step in the pipeline.
    """
    print("Computing Gold layer aggregations...")
    monthly = compute_monthly_summary()
    recurring = compute_recurring_payments()

    result = {**monthly, **recurring}
    print(f"Gold aggregation complete: {result}")
    return result


if __name__ == "__main__":
    run_gold_aggregations()