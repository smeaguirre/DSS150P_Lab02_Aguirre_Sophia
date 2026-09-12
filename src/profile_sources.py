"""Week 2 starter: profile CSV, JSON, Parquet, API payload, and PostgreSQL table.
Complete the TODOs. Do not hard-code expected counts.
"""
from pathlib import Path
from datetime import datetime
import json, csv
import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / 'data'


def _infer_column_type(series):
    """Infer a logical type for a pandas Series read as strings, without
    assuming any fixed set of columns -- works for any CSV."""
    non_null = series.dropna()
    if non_null.empty:
        return 'unknown (all null)'

    def _is_int(v):
        try:
            int(v)
            return True
        except (ValueError, TypeError):
            return False

    def _is_float(v):
        try:
            float(v)
            return True
        except (ValueError, TypeError):
            return False

    if non_null.map(_is_int).all():
        return 'int'
    if non_null.map(_is_float).all():
        return 'float'
    try:
        pd.to_datetime(non_null, errors='raise', format='ISO8601')
        return 'date/timestamp'
    except (ValueError, TypeError):
        pass
    return 'string'


def profile_csv(path):
    """
    Task 1.2 - Profile a CSV source: file size, row/column counts, inferred
    types, missing values per column, exact duplicate rows, and -- when the
    file has a customer_id column -- whether it is unique. Nothing here
    assumes a specific row count or which values are duplicated; every
    number is computed from the file itself.
    """
    path = Path(path)
    df = pd.read_csv(path, dtype=str, keep_default_na=True)
    row_count, col_count = df.shape

    print(f"File: {path}")
    print(f"File size: {path.stat().st_size}")
    print(f"Row Count: {row_count}")
    print(f"Column Count: {col_count}")

    print("\n--- Columns & Inferred Types ---")
    inferred_types = {col: _infer_column_type(df[col]) for col in df.columns}
    for col, dtype in inferred_types.items():
        print(f"{col:20}{dtype}")

    print("\n--- Missing Values ---")
    missing_by_column = df.isna().sum()
    print(missing_by_column.to_string())

    duplicate_rows = int(df.duplicated().sum())
    print(f"\nExact Duplicate Rows: {duplicate_rows}")

    key_uniqueness = None
    if 'customer_id' in df.columns:
        key_uniqueness = df['customer_id'].is_unique
        print(f"\n--- Unique Constraints ---")
        print(f"Is 'customer_id' unique? {key_uniqueness}")

    return {
        'file': str(path),
        'file_size_bytes': path.stat().st_size,
        'row_count': row_count,
        'column_count': col_count,
        'inferred_types': inferred_types,
        'missing_by_column': missing_by_column.to_dict(),
        'duplicate_rows': duplicate_rows,
        'customer_id_unique': key_uniqueness,
    }


def _looks_like_timestamp(value):
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace('Z', '+00:00'))
        return True
    except ValueError:
        return False


def profile_json(path):
    """
    Task 1.3 - Profile a JSON source: confirms the root is a list of
    records, lists top-level keys, identifies nested/numeric/timestamp
    fields, and reports record count plus nulls/missing keys per column.
    Field classification is derived from the data itself (sampled and
    then checked across all records for nulls), not hard-coded to any
    particular schema, so this works for orders.json or any similarly
    shaped list-of-records JSON file.
    """
    path = Path(path)
    data = json.loads(path.read_text(encoding='utf-8'))

    is_list_of_records = isinstance(data, list) and (not data or isinstance(data[0], dict))
    print(f"--- Root Structure ---")
    print(f"Is root structure a list of records? {is_list_of_records}")

    if not is_list_of_records or not data:
        print("No records to profile.")
        return {'is_list_of_records': is_list_of_records, 'record_count': 0}

    top_level_keys = list(data[0].keys())
    nested_fields = [k for k in top_level_keys if isinstance(data[0].get(k), dict)]
    numeric_fields = [
        k for k in top_level_keys
        if isinstance(data[0].get(k), (int, float)) and not isinstance(data[0].get(k), bool)
    ]
    timestamp_fields = [k for k in top_level_keys if _looks_like_timestamp(data[0].get(k))]

    print(f"\n--- Keys & Structure ---")
    print(f"Top-level keys: {top_level_keys}")
    print(f"Nested fields: {nested_fields}")

    print(f"\n--- Data Types ---")
    print(f"Numeric fields: {numeric_fields}")
    print(f"Timestamp fields: {timestamp_fields}")

    missing_counts = {k: 0 for k in top_level_keys}
    for record in data:
        for k in top_level_keys:
            if k not in record or record[k] is None:
                missing_counts[k] += 1

    print(f"\n--- Record Count & Missing Values ---")
    print(f"Total Records: {len(data)}")
    print("Nulls/Missing Keys per Top-Level Column:")
    for k, v in missing_counts.items():
        print(f"{k:20}{v}")

    return {
        'file': str(path),
        'is_list_of_records': is_list_of_records,
        'record_count': len(data),
        'top_level_keys': top_level_keys,
        'nested_fields': nested_fields,
        'numeric_fields': numeric_fields,
        'timestamp_fields': timestamp_fields,
        'missing_counts': missing_counts,
    }


def profile_parquet(path):
    """
    Task 1.4 - Profile a Parquet source: file size, shape, dtypes (as
    preserved by Parquet's own schema metadata, unlike CSV which loses
    type information), and missing values per column.
    """
    path = Path(path)
    df = pd.read_parquet(path)

    print(f"File: {path}")
    print(f"File size: {path.stat().st_size}")
    print(f"Shape: {df.shape}")

    print("\n--- Dtypes (preserved from Parquet schema) ---")
    print(df.dtypes.to_string())

    print("\n--- Missing Values ---")
    missing_by_column = df.isna().sum()
    print(missing_by_column.to_string())

    return {
        'file': str(path),
        'file_size_bytes': path.stat().st_size,
        'row_count': df.shape[0],
        'column_count': df.shape[1],
        'dtypes': {col: str(dtype) for col, dtype in df.dtypes.items()},
        'missing_by_column': missing_by_column.to_dict(),
    }


if __name__ == '__main__':
    profile_csv(DATA_DIR / 'customers.csv')
    print("\n" + "=" * 70 + "\n")
    profile_json(DATA_DIR / 'orders.json')
    print("\n" + "=" * 70 + "\n")
    profile_parquet(DATA_DIR / 'products.parquet')