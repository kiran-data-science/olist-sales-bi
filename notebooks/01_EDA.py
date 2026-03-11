import pandas as pd
import os

data_path = os.path.join("data", "raw")
files = [f for f in os.listdir(data_path) if f.endswith('.csv')]

print(f"✅ Total files found: {len(files)}\n")
print(f"{'File':<55} {'Rows':>8} {'Cols':>6} {'Nulls':>8}")
print("-" * 80)

for f in sorted(files):
    df = pd.read_csv(os.path.join(data_path, f))
    nulls = df.isnull().sum().sum()
    print(f"{f:<55} {len(df):>8,} {df.shape[1]:>6} {nulls:>8,}")

print("\n--- Order Status breakdown ---")
orders = pd.read_csv(os.path.join(data_path, "olist_orders_dataset.csv"))
print(orders['order_status'].value_counts())  