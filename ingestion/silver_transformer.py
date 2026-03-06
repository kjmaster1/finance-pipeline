import psycopg2
import os
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation
from dotenv import load_dotenv
from typing import Optional

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

# ─────────────────────────────────────────────
# Category normalisation map
# Maps raw bank category strings to our standard categories
# ─────────────────────────────────────────────
CATEGORY_MAP = {
    # Groceries
    "groceries": "Groceries",
    "grocery": "Groceries",
    # Eating out
    "eating out": "Eating Out",
    "eating_out": "Eating Out",
    "restaurant": "Eating Out",
    # Transport
    "transport": "Transport",
    "transportation": "Transport",
    # Entertainment
    "entertainment": "Entertainment",
    # Shopping
    "shopping": "Shopping",
    # Bills
    "bills": "Bills",
    "bill": "Bills",
    # Health
    "health": "Health",
    # Housing
    "housing": "Housing",
    # Income
    "income": "Income",
    "salary": "Income",
}

# ─────────────────────────────────────────────
# Keyword-based category inference
# Used when the bank doesn't provide a category (e.g. HSBC)
# or when the category is missing
# ─────────────────────────────────────────────
CATEGORY_KEYWORDS = {
    "Groceries": [
        "tesco", "sainsbury", "asda", "morrisons", "lidl", "aldi",
        "waitrose", "marks spencer", "m&s food", "co-op", "coop",
        "iceland", "ocado", "booths"
    ],
    "Eating Out": [
        "mcdonald", "kfc", "burger king", "subway", "nando",
        "pret", "costa", "starbucks", "caffe nero", "greggs",
        "deliveroo", "uber eats", "just eat", "wagamama",
        "pizza", "restaurant", "cafe", "coffee"
    ],
    "Transport": [
        "tfl", "transport for london", "trainline", "national rail",
        "uber", "lyft", "bus", "tube", "metro", "rail",
        "petrol", "bp", "shell", "esso", "texaco", "fuel"
    ],
    "Entertainment": [
        "netflix", "spotify", "amazon prime", "disney", "apple",
        "cinema", "vue", "odeon", "cineworld", "sky",
        "youtube", "twitch", "steam", "playstation", "xbox"
    ],
    "Shopping": [
        "amazon", "ebay", "asos", "next", "h&m", "zara",
        "primark", "topshop", "john lewis", "argos",
        "halfords", "waterstones", "boots"
    ],
    "Bills": [
        "british gas", "edf", "eon", "npower", "southern electric",
        "bt ", "virgin media", "sky broadband", "vodafone",
        "o2", "ee ", "three", "council tax", "water",
        "broadband", "mobile", "insurance", "utility"
    ],
    "Health": [
        "boots", "lloyds pharmacy", "superdrug",
        "gym", "fitness", "leisure centre", "swimming",
        "dentist", "doctor", "pharmacy", "nhs"
    ],
    "Housing": [
        "rent", "landlord", "mortgage", "estate agent",
        "rightmove", "zoopla"
    ],
}

# ─────────────────────────────────────────────
# Recurring payment detection keywords
# These patterns strongly suggest a subscription or regular payment
# ─────────────────────────────────────────────
RECURRING_KEYWORDS = [
    "netflix", "spotify", "amazon prime", "disney+", "apple",
    "gym", "membership", "subscription", "direct debit", "dd ",
    "standing order", "insurance", "broadband", "mobile",
    "council tax", "rent", "mortgage", "sky ", "bt ",
    "vodafone", "o2", "ee ", "three", "virgin"
]


def get_connection():
    return psycopg2.connect(
        host=os.getenv("FINANCE_DB_HOST", "finance-postgres"),
        port=int(os.getenv("FINANCE_DB_PORT", "5432")),
        dbname=os.getenv("FINANCE_DB_NAME", "finance"),
        user=os.getenv("FINANCE_DB_USER", "finance"),
        password=os.getenv("FINANCE_DB_PASSWORD", "finance"),
    )


def parse_date(raw_date: str) -> datetime.date:
    """
    Try multiple date formats used by UK banks.
    Returns a proper date object or raises ValueError.
    """
    formats = [
        "%d/%m/%Y",  # 01/01/2026  (Monzo, Starling, HSBC)
        "%Y-%m-%d",  # 2026-01-01  (ISO format)
        "%d-%m-%Y",  # 01-01-2026
        "%d %b %Y",  # 01 Jan 2026
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw_date.strip(), fmt).date()
        except ValueError:
            continue
    raise ValueError(f"Cannot parse date: {raw_date}")


def parse_amount(raw_amount: str) -> Decimal:
    """
    Parse amount strings into Decimal.
    Handles: "-67.43", "67.43", "£67.43", "1,234.56"
    """
    cleaned = raw_amount.strip()
    cleaned = cleaned.replace("£", "").replace(",", "").strip()
    try:
        return Decimal(cleaned)
    except InvalidOperation:
        raise ValueError(f"Cannot parse amount: {raw_amount}")


def normalise_category(raw_category: Optional[str], description: str) -> str:
    """
    Convert raw bank category to our standard category.
    Falls back to keyword inference if no category provided.
    """
    if raw_category and raw_category.strip():
        normalised = CATEGORY_MAP.get(raw_category.lower().strip())
        if normalised:
            return normalised

    # Infer from description using keyword matching
    description_lower = description.lower()
    for category, keywords in CATEGORY_KEYWORDS.items():
        for keyword in keywords:
            if keyword in description_lower:
                return category

    return "Other"


def detect_recurring(description: str, raw_category: Optional[str]) -> bool:
    """
    Detect if a transaction is likely a recurring payment.
    Checks description and category for known recurring patterns.
    """
    text = description.lower()
    if raw_category:
        text += " " + raw_category.lower()

    for keyword in RECURRING_KEYWORDS:
        if keyword in text:
            return True
    return False


def transform_bronze_to_silver(bank_name: str = None) -> dict:
    """
    Read unprocessed Bronze rows and transform them into Silver.

    Only processes rows that don't already have a Silver record
    (checked via bronze_id foreign key) so this is safe to re-run.

    Optionally filter by bank_name to process one bank at a time.
    """
    conn = get_connection()
    processed = 0
    failed = 0

    try:
        with conn.cursor() as cur:
            # Find bronze rows not yet in silver
            if bank_name:
                cur.execute("""
                    SELECT b.id, b.bank_name, b.raw_date, b.raw_description,
                           b.raw_amount, b.raw_category, b.raw_reference
                    FROM bronze_transactions b
                    LEFT JOIN silver_transactions s ON s.bronze_id = b.id
                    WHERE s.id IS NULL
                    AND b.bank_name = %s
                    ORDER BY b.id
                """, (bank_name,))
            else:
                cur.execute("""
                    SELECT b.id, b.bank_name, b.raw_date, b.raw_description,
                           b.raw_amount, b.raw_category, b.raw_reference
                    FROM bronze_transactions b
                    LEFT JOIN silver_transactions s ON s.bronze_id = b.id
                    WHERE s.id IS NULL
                    ORDER BY b.id
                """)

            rows = cur.fetchall()
            print(f"Found {len(rows)} unprocessed Bronze rows")

            for row in rows:
                bronze_id, bank, raw_date, raw_desc, raw_amount, raw_cat, raw_ref = row

                try:
                    transaction_date = parse_date(raw_date)
                    amount = parse_amount(raw_amount)
                    category = normalise_category(raw_cat, raw_desc)
                    is_debit = amount < 0
                    is_recurring = detect_recurring(raw_desc, raw_cat)

                    cur.execute("""
                        INSERT INTO silver_transactions (
                            bronze_id, transaction_date, description,
                            amount, currency, category, bank_name,
                            is_debit, is_recurring, reference
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        bronze_id,
                        transaction_date,
                        raw_desc.strip(),
                        float(amount),
                        "GBP",
                        category,
                        bank,
                        is_debit,
                        is_recurring,
                        raw_ref,
                    ))
                    processed += 1

                except Exception as e:
                    print(f"Failed to transform bronze_id {bronze_id}: {e}")
                    failed += 1

        conn.commit()

    finally:
        conn.close()

    result = {
        "processed": processed,
        "failed": failed,
        "total": processed + failed
    }
    print(f"Silver transformation complete: {result}")
    return result


if __name__ == "__main__":
    transform_bronze_to_silver()