import duckdb
import pandas as pd
import os

# Connect to DuckDB
db_path = "data/olist.duckdb"
conn = duckdb.connect(db_path)

data_path = "data/raw/"
files = [f for f in os.listdir(data_path) if f.endswith('.csv')]

print("Loading CSVs into DuckDB...\n")

for f in sorted(files):
    # Clean table name
    table_name = f.replace('.csv', '').replace('-', '_')
    file_path = data_path + f

    conn.execute(f"""
        CREATE OR REPLACE TABLE {table_name} AS
        SELECT * FROM read_csv_auto('{file_path}')
    """)

    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"✅ {table_name:<50} {count:>8,} rows")

print("\n--- All tables in DuckDB ---")
tables = conn.execute("SHOW TABLES").fetchall()
for t in tables:
    print(f"  📋 {t[0]}")

conn.close()
print("\n✅ DuckDB database created at data/olist.duckdb")