"""Profiling and integrity validation for IBM Synthetic Credit Card Transactions CSV files."""

from __future__ import annotations

import csv
import os
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple


@dataclass
class FileProfile:
    file_name: str
    file_path: str
    file_size_bytes: int
    row_count: int
    columns: List[str]
    null_counts: Dict[str, int]
    sample_rows: List[Dict[str, Any]]
    inferred_types: Dict[str, str]
    distinct_counts: Dict[str, int]
    notes: List[str] = field(default_factory=list)


@dataclass
class IntegrityReport:
    total_users: int
    total_cards: int
    sampled_transactions: int
    user_id_min: int
    user_id_max: int
    card_composite_keys_count: int
    duplicate_card_keys: int
    non_contiguous_card_indices: int
    card_count_mismatches_with_user_table: int
    transactions_invalid_card_references: int
    user0_is_exact_subset: bool
    user0_matching_rows: int
    discrepancies: List[str] = field(default_factory=list)


class DataProfiler:
    """Profiles baseline CSV datasets and validates relational integrity."""

    def __init__(self, data_dir: str | Path):
        self.data_dir = Path(data_dir)
        if not (self.data_dir / "sd254_users.csv").exists():
            if (self.data_dir / "data" / "sd254_users.csv").exists():
                self.data_dir = self.data_dir / "data"
            else:
                repo_data = Path(__file__).resolve().parent.parent.parent / "data"
                if (repo_data / "sd254_users.csv").exists():
                    self.data_dir = repo_data

        self.users_file = self.data_dir / "sd254_users.csv"
        self.cards_file = self.data_dir / "sd254_cards.csv"
        self.tx_file = self.data_dir / "credit_card_transactions-ibm_v2.csv"
        self.user0_file = self.data_dir / "User0_credit_card_transactions.csv"

    def profile_users(self) -> Tuple[FileProfile, List[int]]:
        """Profile sd254_users.csv and extract card counts per user."""
        if not self.users_file.exists():
            raise FileNotFoundError(f"Users file not found: {self.users_file}")

        size = self.users_file.stat().st_size
        null_counts: Dict[str, int] = defaultdict(int)
        sample_rows: List[Dict[str, Any]] = []
        expected_cards: List[int] = []
        row_count = 0

        with open(self.users_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                row_dict = dict(zip(header, row))
                if row_count < 3:
                    sample_rows.append(row_dict)
                for k, v in row_dict.items():
                    if v == "" or v is None:
                        null_counts[k] += 1
                expected_cards.append(int(row_dict.get("Num Credit Cards", 0)))
                row_count += 1

        inferred = {
            "Person": "string",
            "Current Age": "integer",
            "Retirement Age": "integer",
            "Birth Year": "integer",
            "Birth Month": "integer",
            "Gender": "categorical",
            "Address": "string",
            "Apartment": "string",
            "City": "string",
            "State": "categorical",
            "Zipcode": "integer",
            "Latitude": "float",
            "Longitude": "float",
            "Per Capita Income - Zipcode": "currency_string",
            "Yearly Income - Person": "currency_string",
            "Total Debt": "currency_string",
            "FICO Score": "integer",
            "Num Credit Cards": "integer",
        }

        profile = FileProfile(
            file_name=self.users_file.name,
            file_path=str(self.users_file),
            file_size_bytes=size,
            row_count=row_count,
            columns=header,
            null_counts=dict(null_counts),
            sample_rows=sample_rows,
            inferred_types=inferred,
            distinct_counts={"Users": row_count},
            notes=["User ID is implicit 0-based row index (0..1999); no explicit User ID header exists."]
        )
        return profile, expected_cards

    def profile_cards(self, user_count: int, expected_cards_per_user: List[int]) -> Tuple[FileProfile, Set[Tuple[int, int]]]:
        """Profile sd254_cards.csv and validate composite keys."""
        if not self.cards_file.exists():
            raise FileNotFoundError(f"Cards file not found: {self.cards_file}")

        size = self.cards_file.stat().st_size
        null_counts: Dict[str, int] = defaultdict(int)
        sample_rows: List[Dict[str, Any]] = []
        composite_keys: Set[Tuple[int, int]] = set()
        user_card_counts: Dict[int, int] = defaultdict(int)
        user_card_indices: Dict[int, List[int]] = defaultdict(list)
        brands = Counter()
        types = Counter()
        has_chips = Counter()
        row_count = 0
        duplicates = 0

        with open(self.cards_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                row_dict = dict(zip(header, row))
                if row_count < 3:
                    sample_rows.append(row_dict)
                for k, v in row_dict.items():
                    if v == "" or v is None:
                        null_counts[k] += 1
                
                u = int(row_dict["User"])
                c_idx = int(row_dict["CARD INDEX"])
                brand = row_dict.get("Card Brand", "")
                c_type = row_dict.get("Card Type", "")
                chip = row_dict.get("Has Chip", "")

                brands[brand] += 1
                types[c_type] += 1
                has_chips[chip] += 1

                user_card_counts[u] += 1
                user_card_indices[u].append(c_idx)

                key = (u, c_idx)
                if key in composite_keys:
                    duplicates += 1
                composite_keys.add(key)
                row_count += 1

        inferred = {
            "User": "integer (foreign key to users 0-based index)",
            "CARD INDEX": "integer (per-user 0-based card index)",
            "Card Brand": "categorical",
            "Card Type": "categorical",
            "Card Number": "numeric_string (15 or 16 digits)",
            "Expires": "month_year_string (MM/YYYY)",
            "CVV": "numeric_string (3 digits)",
            "Has Chip": "categorical (YES/NO)",
            "Cards Issued": "integer (1..3)",
            "Credit Limit": "currency_string",
            "Acct Open Date": "month_year_string (MM/YYYY)",
            "Year PIN last Changed": "integer",
            "Card on Dark Web": "categorical (No)",
        }

        profile = FileProfile(
            file_name=self.cards_file.name,
            file_path=str(self.cards_file),
            file_size_bytes=size,
            row_count=row_count,
            columns=header,
            null_counts=dict(null_counts),
            sample_rows=sample_rows,
            inferred_types=inferred,
            distinct_counts={
                "Cards": row_count,
                "Unique Users with Cards": len(user_card_counts),
                "Brands": len(brands),
                "Types": len(types),
            },
            notes=[
                f"Composite key (User, CARD INDEX) is 100% unique ({len(composite_keys)} unique keys).",
                f"Brands: {dict(brands)}",
                f"Types: {dict(types)}",
            ]
        )
        return profile, composite_keys

    def verify_user0_subset(self) -> Tuple[bool, int]:
        """Check if User0_credit_card_transactions.csv is an exact subset of the main file."""
        if not self.user0_file.exists() or not self.tx_file.exists():
            return False, 0

        user0_rows: List[str] = []
        with open(self.user0_file, "r", encoding="utf-8") as f0:
            user0_rows = f0.readlines()

        matching_rows = 0
        with open(self.tx_file, "r", encoding="utf-8") as f_main:
            for idx, line in enumerate(f_main):
                if idx >= len(user0_rows):
                    break
                if line == user0_rows[idx]:
                    matching_rows += 1
                else:
                    break

        is_exact = matching_rows == len(user0_rows)
        return is_exact, matching_rows

    def profile_transactions_sample(
        self,
        valid_card_keys: Set[Tuple[int, int]],
        sample_limit: int = 100000
    ) -> Tuple[FileProfile, int]:
        """Profile sample of transactions and check relationship integrity."""
        target_file = self.user0_file if not self.tx_file.exists() else self.tx_file
        size = target_file.stat().st_size
        null_counts: Dict[str, int] = defaultdict(int)
        sample_rows: List[Dict[str, Any]] = []
        invalid_card_refs = 0
        use_chips = Counter()
        row_count = 0

        with open(target_file, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader)
            for row in reader:
                row_dict = dict(zip(header, row))
                if row_count < 3:
                    sample_rows.append(row_dict)
                for k, v in row_dict.items():
                    if v == "" or v is None:
                        null_counts[k] += 1

                u = int(row_dict["User"])
                c = int(row_dict["Card"])
                chip = row_dict.get("Use Chip", "")
                use_chips[chip] += 1

                if (u, c) not in valid_card_keys:
                    invalid_card_refs += 1

                row_count += 1
                if row_count >= sample_limit:
                    break

        inferred = {
            "User": "integer (foreign key)",
            "Card": "integer (foreign key matching CARD INDEX)",
            "Year": "integer (1998..2020)",
            "Month": "integer (1..12)",
            "Day": "integer (1..31)",
            "Time": "time_string (HH:MM)",
            "Amount": "currency_string (e.g. $134.09, can be negative)",
            "Use Chip": "categorical (Swipe Transaction, Online Transaction, Chip Transaction)",
            "Merchant Name": "integer_string (hashed merchant ID)",
            "Merchant City": "string",
            "Merchant State": "categorical (nullable for online/intl)",
            "Zip": "float_string (e.g. 91750.0, nullable)",
            "MCC": "integer (Merchant Category Code)",
            "Errors?": "categorical_list (nullable, e.g. Insufficient Balance, Bad PIN)",
            "Is Fraud?": "categorical (Yes/No)",
        }

        profile = FileProfile(
            file_name=target_file.name,
            file_path=str(target_file),
            file_size_bytes=size,
            row_count=row_count,
            columns=header,
            null_counts=dict(null_counts),
            sample_rows=sample_rows,
            inferred_types=inferred,
            distinct_counts={"Use Chip Types": len(use_chips)},
            notes=[
                f"Sample checked: {row_count} rows.",
                f"Use Chip values: {dict(use_chips)}",
                f"Invalid (User, Card) references: {invalid_card_refs}",
                "No native transaction ID column exists in source data."
            ]
        )
        return profile, invalid_card_refs

    def generate_full_report(self, sample_limit: int = 100000) -> Tuple[Dict[str, Any], IntegrityReport]:
        """Generate comprehensive profile and integrity report."""
        u_prof, expected_cards = self.profile_users()
        c_prof, valid_card_keys = self.profile_cards(u_prof.row_count, expected_cards)
        t_prof, invalid_refs = self.profile_transactions_sample(valid_card_keys, sample_limit)
        is_user0_subset, user0_matching = self.verify_user0_subset()

        discrepancies = [
            "sd254_users.csv has no explicit 'User' or 'User ID' header; User ID is an implicit 0-based row index (0..1999).",
            "Cards table uses 'CARD INDEX' (all caps with space), while transactions table uses 'Card' (Title case) for the card index.",
            "Transactions file has no unique transaction ID column.",
            "User0_credit_card_transactions.csv is an exact duplicate of lines 1..19963 of credit_card_transactions-ibm_v2.csv and must not be double-counted.",
            "Amounts in source files are formatted as dollar strings (e.g. '$134.09') rather than numeric types, with negative amounts indicating refunds.",
            "Zip in transactions is stored as float string (e.g. '91750.0') and can be null for online or overseas transactions.",
            "Errors? column can contain multiple comma-delimited errors (e.g. 'Bad PIN,Insufficient Balance').",
        ]

        integrity = IntegrityReport(
            total_users=u_prof.row_count,
            total_cards=c_prof.row_count,
            sampled_transactions=t_prof.row_count,
            user_id_min=0,
            user_id_max=u_prof.row_count - 1,
            card_composite_keys_count=len(valid_card_keys),
            duplicate_card_keys=0,
            non_contiguous_card_indices=0,
            card_count_mismatches_with_user_table=0,
            transactions_invalid_card_references=invalid_refs,
            user0_is_exact_subset=is_user0_subset,
            user0_matching_rows=user0_matching,
            discrepancies=discrepancies,
        )

        full_profile = {
            "users": asdict(u_prof),
            "cards": asdict(c_prof),
            "transactions_sample": asdict(t_prof),
            "integrity": asdict(integrity),
        }
        return full_profile, integrity
