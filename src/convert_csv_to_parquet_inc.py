from pathlib import Path
import polars as pl

csv_dir = Path("data/fake_csvs")
parquet_dir = Path("data/parquets")
parquet_dir.mkdir(exist_ok=True)

# 1. Descobrir quais CSVs já foram convertidos
converted = {f.stem for f in parquet_dir.glob("*.parquet")}

# 2. Iterar sobre os novos CSVs
for csv_file in csv_dir.glob("*.csv"):
    if csv_file.stem not in converted:
        print(f"Convertendo {csv_file}...")
        # Usar Polars para ler e exportar como Parquet
        df = pl.read_csv(csv_file)
        df.write_parquet(parquet_dir / f"{csv_file.stem}.parquet")
