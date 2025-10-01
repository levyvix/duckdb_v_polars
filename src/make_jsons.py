from faker import Faker
import polars as pl
from pathlib import Path

# Initialize Faker
fake = Faker()

json_path = Path("data/fake_jsons")

# Create a directory for JSON files
json_path.mkdir(parents=True, exist_ok=True)

# Generate 10 fake JSON files (scale to 100,000 as needed)
for i in range(100):  # Change to 100_000 for full scale
    # if already exists skip
    if json_path.exists() and json_path.joinpath(f"data_{i}.json").exists():
        continue

    data = {
        "name": [fake.name() for _ in range(100)],  # 100 rows per file
        "email": [fake.email() for _ in range(100)],
        "age": [fake.random_int(min=18, max=80) for _ in range(100)],
    }
    df = pl.DataFrame(data)
    # Save to JSON
    df.write_json(json_path.joinpath(f"data_{i}.json"))

print("Fake JSON files generated.")
