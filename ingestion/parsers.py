import hashlib
from dataclasses import dataclass
from typing import Optional

import pandas as pd


@dataclass
class RawTransaction:
    """
    A standardised raw transaction before any cleaning.
    This is what gets written to the Bronze layer.
    """
    source_file: str
    bank_name: str
    raw_date: str
    raw_description: str
    raw_amount: str
    raw_category: Optional[str]
    raw_reference: Optional[str]
    raw_balance: Optional[str]
    extra_fields: dict
    file_row_number: int

    def to_row_hash(self) -> str:
        """
        Generate a unique hash for this transaction.
        This prevents duplicate rows if the same file is ingested twice.
        We hash the core fields that identify a unique transaction.
        """
        unique_string = f"{self.bank_name}:{self.raw_date}:{self.raw_description}:{self.raw_amount}:{self.file_row_number}"
        return hashlib.sha256(unique_string.encode()).hexdigest()


class MonzoParser:
    """
    Parses Monzo CSV exports.

    Monzo format:
    Transaction ID, Date, Time, Type, Name, Emoji, Category,
    Amount, Currency, Local amount, Local currency,
    Notes and #tags, Address, Receipt, Description,
    Category split, Money Out, Money In
    """
    BANK_NAME = "monzo"
    DATE_FORMAT = "%d/%m/%Y"

    def can_parse(self, df: pd.DataFrame) -> bool:
        """Check if this dataframe looks like a Monzo export."""
        required_columns = {"Transaction ID", "Date", "Amount", "Name"}
        return required_columns.issubset(set(df.columns))

    def parse(self, df: pd.DataFrame, source_file: str) -> list[RawTransaction]:
        transactions = []
        for row_num, row in df.iterrows():
            extra = {}
            if pd.notna(row.get("Notes and #tags")):
                extra["notes"] = str(row["Notes and #tags"])
            if pd.notna(row.get("Address")):
                extra["address"] = str(row["Address"])
            if pd.notna(row.get("Type")):
                extra["type"] = str(row["Type"])

            transactions.append(RawTransaction(
                source_file=source_file,
                bank_name=self.BANK_NAME,
                raw_date=str(row.get("Date", "")),
                raw_description=str(row.get("Name", "")),
                raw_amount=str(row.get("Amount", "")),
                raw_category=str(row.get("Category", "")) if pd.notna(row.get("Category")) else None,
                raw_reference=str(row.get("Transaction ID", "")),
                raw_balance=None,
                extra_fields=extra,
                file_row_number=int(row_num),
            ))
        return transactions


class StarlingParser:
    """
    Parses Starling Bank CSV exports.

    Starling format:
    Date, Counter Party, Reference, Type,
    Amount (GBP), Balance (GBP), Spending Category
    """
    BANK_NAME = "starling"
    DATE_FORMAT = "%d/%m/%Y"

    def can_parse(self, df: pd.DataFrame) -> bool:
        required_columns = {"Date", "Counter Party", "Amount (GBP)"}
        return required_columns.issubset(set(df.columns))

    def parse(self, df: pd.DataFrame, source_file: str) -> list[RawTransaction]:
        transactions = []
        for row_num, row in df.iterrows():
            extra = {}
            if pd.notna(row.get("Type")):
                extra["type"] = str(row["Type"])

            transactions.append(RawTransaction(
                source_file=source_file,
                bank_name=self.BANK_NAME,
                raw_date=str(row.get("Date", "")),
                raw_description=str(row.get("Counter Party", "")),
                raw_amount=str(row.get("Amount (GBP)", "")),
                raw_category=str(row.get("Spending Category", "")) if pd.notna(row.get("Spending Category")) else None,
                raw_reference=str(row.get("Reference", "")) if pd.notna(row.get("Reference")) else None,
                raw_balance=str(row.get("Balance (GBP)", "")) if pd.notna(row.get("Balance (GBP)")) else None,
                extra_fields=extra,
                file_row_number=int(row_num),
            ))
        return transactions


class HSBCParser:
    """
    Parses HSBC CSV exports.

    HSBC format:
    Date, Description, Amount, Balance

    Note: HSBC doesn't include categories — we'll infer them
    in the Silver layer using keyword matching.
    """
    BANK_NAME = "hsbc"
    DATE_FORMAT = "%d/%m/%Y"

    def can_parse(self, df: pd.DataFrame) -> bool:
        required_columns = {"Date", "Description", "Amount"}
        return required_columns.issubset(set(df.columns))

    def parse(self, df: pd.DataFrame, source_file: str) -> list[RawTransaction]:
        transactions = []
        for row_num, row in df.iterrows():
            transactions.append(RawTransaction(
                source_file=source_file,
                bank_name=self.BANK_NAME,
                raw_date=str(row.get("Date", "")),
                raw_description=str(row.get("Description", "")),
                raw_amount=str(row.get("Amount", "")),
                raw_category=None,
                raw_reference=None,
                raw_balance=str(row.get("Balance", "")) if pd.notna(row.get("Balance")) else None,
                extra_fields={},
                file_row_number=int(row_num),
            ))
        return transactions


# Parser registry - add new banks here
PARSERS = [
    MonzoParser(),
    StarlingParser(),
    HSBCParser(),
]


def detect_parser(df: pd.DataFrame):
    """
    Auto-detect which bank this CSV came from by checking
    which parser recognises the column structure.
    Returns the matching parser or raises an error.
    """
    for parser in PARSERS:
        if parser.can_parse(df):
            return parser
    raise ValueError(
        f"Unrecognised bank CSV format. Columns found: {list(df.columns)}"
    )


def parse_csv_file(filepath: str) -> list[RawTransaction]:
    """
    Main entry point for parsing a bank CSV file.
    Detects the bank format and returns a list of raw transactions.
    """
    df = pd.read_csv(filepath)
    # Strip whitespace from column names - some banks add spaces
    df.columns = df.columns.str.strip()
    parser = detect_parser(df)
    print(f"Detected bank format: {parser.BANK_NAME}")
    return parser.parse(df, filepath)
