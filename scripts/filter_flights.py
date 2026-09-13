import pandas as pd

print("Reading flights.csv (this takes a minute, it's a 592MB file)...")
df = pd.read_csv(
    "../data/flights.csv",
    dtype={"ORIGIN_AIRPORT": str, "DESTINATION_AIRPORT": str},
    low_memory=False,
)

print(f"Total rows loaded: {len(df):,}")
print(f"Columns: {list(df.columns)}")

# Filter to January 2015 -- multiple major Northeast winter storms that month
jan = df[(df["YEAR"] == 2015) & (df["MONTH"] == 1)]

print(f"January 2015 rows: {len(jan):,}")

jan.to_csv("../data/flights_2015_01.csv", index=False)
print("Saved to data/flights_2015_01.csv")