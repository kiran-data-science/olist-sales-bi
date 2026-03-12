import duckdb

conn = duckdb.connect("data/olist.duckdb")

tables = ["fct_orders", "dim_customers", "dim_sellers", "mart_sales_daily"]

for table in tables:
    output_path = f"data/processed/{table}.parquet"
    conn.execute(f"COPY {table} TO '{output_path}' (FORMAT PARQUET)")
    count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
    print(f"✅ {table:<30} {count:>8,} rows → {output_path}")

conn.close()
print("\n✅ All tables exported to data/processed/")