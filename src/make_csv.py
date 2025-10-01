from faker import Faker
import polars as pl
from pathlib import Path

# Initialize Faker
fake = Faker()
csv_path = Path("data/fake_csvs")

# Create a directory for CSV files
csv_path.mkdir(parents=True, exist_ok=True)


for i in range(100):
    # if already exists skip
    if csv_path.exists() and (csv_path / f"data_{i}.csv").exists():
        continue
    data = {
        "name": [fake.name() for _ in range(100)],  # 100 rows per file
        "email": [fake.email() for _ in range(100)],
        "age": [fake.random_int(min=18, max=80) for _ in range(100)],
    }
    df = pl.DataFrame(data)
    # Save to CSV
    df.write_csv(csv_path / f"data_{i}.csv")

print("Fake CSV files generated.")
