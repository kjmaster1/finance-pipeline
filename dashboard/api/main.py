from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os
from dotenv import load_dotenv
from typing import List
from pydantic import BaseModel
from datetime import date

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '..', '.env'))

app = FastAPI(title="Finance Pipeline Dashboard API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def get_connection():
    return psycopg2.connect(
        host=os.getenv("FINANCE_DB_HOST", "localhost"),
        port=int(os.getenv("FINANCE_DB_PORT", "5433")),
        dbname=os.getenv("FINANCE_DB_NAME", "finance"),
        user=os.getenv("FINANCE_DB_USER", "finance"),
        password=os.getenv("FINANCE_DB_PASSWORD", "finance"),
    )


# ── Response models ──────────────────────────────────────

class MonthlySummary(BaseModel):
    year: int
    month: int
    category: str
    bank_name: str
    total_spent: float
    transaction_count: int
    avg_transaction: float

class RecurringPayment(BaseModel):
    description: str
    avg_amount: float
    frequency: str
    last_seen: date
    times_seen: int
    bank_name: str

class OverviewStats(BaseModel):
    total_spent: float
    total_transactions: int
    top_category: str
    recurring_total: float
    banks_connected: int

class CategoryBreakdown(BaseModel):
    category: str
    total_spent: float
    transaction_count: int
    percentage: float

class MonthlyTrend(BaseModel):
    month_label: str
    total_spent: float
    year: int
    month: int


# ── Endpoints ────────────────────────────────────────────

@app.get("/health")
def health():
    return {"status": "ok"}


@app.get("/overview", response_model=OverviewStats)
def get_overview():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    COALESCE(SUM(total_spent), 0)       AS total_spent,
                    COALESCE(SUM(transaction_count), 0) AS total_transactions,
                    COUNT(DISTINCT bank_name)            AS banks_connected
                FROM gold_monthly_summary
            """)
            row = cur.fetchone()

            cur.execute("""
                SELECT category, SUM(total_spent) AS spent
                FROM gold_monthly_summary
                GROUP BY category
                ORDER BY spent DESC
                LIMIT 1
            """)
            top = cur.fetchone()

            cur.execute("""
                SELECT COALESCE(SUM(avg_amount), 0) AS recurring_total
                FROM gold_recurring_payments
            """)
            recurring = cur.fetchone()

        return OverviewStats(
            total_spent=float(row["total_spent"]),
            total_transactions=int(row["total_transactions"]),
            top_category=top["category"] if top else "N/A",
            recurring_total=float(recurring["recurring_total"]),
            banks_connected=int(row["banks_connected"]),
        )
    finally:
        conn.close()


@app.get("/categories", response_model=List[CategoryBreakdown])
def get_categories():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    category,
                    SUM(total_spent)        AS total_spent,
                    SUM(transaction_count)  AS transaction_count
                FROM gold_monthly_summary
                GROUP BY category
                ORDER BY total_spent DESC
            """)
            rows = cur.fetchall()

        total = sum(float(r["total_spent"]) for r in rows)
        return [
            CategoryBreakdown(
                category=r["category"],
                total_spent=float(r["total_spent"]),
                transaction_count=int(r["transaction_count"]),
                percentage=round(float(r["total_spent"]) / total * 100, 1) if total > 0 else 0,
            )
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/monthly-trends", response_model=List[MonthlyTrend])
def get_monthly_trends():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    year,
                    month,
                    SUM(total_spent) AS total_spent
                FROM gold_monthly_summary
                GROUP BY year, month
                ORDER BY year, month
            """)
            rows = cur.fetchall()

        months = [
            "Jan", "Feb", "Mar", "Apr", "May", "Jun",
            "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
        ]
        return [
            MonthlyTrend(
                month_label=f"{months[r['month'] - 1]} {r['year']}",
                total_spent=float(r["total_spent"]),
                year=r["year"],
                month=r["month"],
            )
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/recurring", response_model=List[RecurringPayment])
def get_recurring():
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute("""
                SELECT
                    description,
                    avg_amount,
                    frequency,
                    last_seen,
                    times_seen,
                    bank_name
                FROM gold_recurring_payments
                ORDER BY avg_amount DESC
            """)
            rows = cur.fetchall()

        return [
            RecurringPayment(
                description=r["description"],
                avg_amount=float(r["avg_amount"]),
                frequency=r["frequency"],
                last_seen=r["last_seen"],
                times_seen=r["times_seen"],
                bank_name=r["bank_name"],
            )
            for r in rows
        ]
    finally:
        conn.close()


@app.get("/transactions", response_model=List[dict])
def get_transactions(limit: int = 50, category: str = None, bank: str = None):
    conn = get_connection()
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            query = """
                SELECT
                    transaction_date,
                    description,
                    amount,
                    category,
                    bank_name,
                    is_debit,
                    is_recurring
                FROM silver_transactions
                WHERE 1=1
            """
            params = []
            if category:
                query += " AND category = %s"
                params.append(category)
            if bank:
                query += " AND bank_name = %s"
                params.append(bank)

            query += " ORDER BY transaction_date DESC LIMIT %s"
            params.append(limit)

            cur.execute(query, params)
            rows = cur.fetchall()

        return [dict(r) for r in rows]
    finally:
        conn.close()