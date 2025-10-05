import polars as pl
import time

FILE_TYPE = "csv"


def read_files(path: str, file_type: str) -> pl.DataFrame:
    """Read files based on the specified type."""
    if file_type.lower() == "csv":
        return pl.read_csv(path)
    if file_type.lower() == "json":
        return pl.read_json(path)
    raise ValueError(f"Unsupported file type: {file_type}. Use 'csv' or 'json'.")


# Start processing
start_total = time.time()
print("Starting file processing...")

# Read files
start = time.time()
file_path = f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}"
df = read_files(file_path, FILE_TYPE)
total_rows = df.select(pl.len())
print(f"Total rows across all files: {total_rows}")
print(f"Reading files took: {time.time() - start:.2f} seconds")

# Show schema
start = time.time()
print("\nSchema of the 'people' table:")
print(df.collect_schema())
print(f"Schema collection took: {time.time() - start:.2f} seconds")

# Aggregate data
start = time.time()
result = (
    df.group_by("name")
    .agg(pl.col("age").mean().alias("avg_age"))
    .filter(pl.col("avg_age") > 30)
    .sort("avg_age", descending=True)
    .limit(5)
)
print("\nTop 5 names with average age > 30:")
print(result)
print(f"Aggregation operation took: {time.time() - start:.2f} seconds")

# Filter and export
start = time.time()
df.filter(pl.col("age") >= 50).write_json("filtered.json")
print(f"Filter and export operation took: {time.time() - start:.2f} seconds")

print(f"\nTotal processing time: {time.time() - start_total:.2f} seconds")
