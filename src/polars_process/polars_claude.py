from pathlib import Path
import polars as pl
import time
from dataclasses import dataclass

FILE_TYPE = "csv"


@dataclass
class FileReader:
    path: str | Path
    file_type: str

    def read_csv_files(self) -> pl.DataFrame:
        """
        Lê arquivos CSV usando o path fornecido.

        Args:
            Nenhum (usa atributos da classe)

        Returns:
            pl.DataFrame: DataFrame com os dados lidos

        Exemplo:
            >>> reader = FileReader("data/*.csv", "csv")
            >>> df = reader.read_csv_files()
        """
        return pl.read_csv(self.path)

    def read_json_files(self) -> pl.DataFrame:
        """
        Lê arquivos JSON usando glob pattern diretamente.

        Args:
            Nenhum (usa atributos da classe)

        Returns:
            pl.DataFrame: DataFrame com os dados concatenados

        Exemplo:
            >>> reader = FileReader("data/*.json", "json")
            >>> df = reader.read_json_files()
        """
        return pl.read_json(self.path)

    def read_files(self) -> pl.DataFrame:
        """
        Lê arquivos baseado no tipo especificado.

        Args:
            Nenhum (usa atributos da classe)

        Returns:
            pl.DataFrame: DataFrame com os dados lidos

        Raises:
            ValueError: Se o tipo de arquivo não for suportado

        Exemplo:
            >>> reader = FileReader("data/*.csv", "csv")
            >>> df = reader.read_files()
        """
        match self.file_type.lower():
            case "csv":
                return self.read_csv_files()
            case "json":
                return self.read_json_files()
            case _:
                raise ValueError(f"Unsupported file type: {self.file_type}")


start_total = time.time()

print("Starting JSON processing...")
start = time.time()

print(f"JSON scan initialization took: {time.time() - start:.2f} seconds")

start = time.time()
path_obj = Path(f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}")
file_reader = FileReader(path=path_obj, file_type=FILE_TYPE)
df = file_reader.read_files()
total_rows = df.select(pl.len())
print(f"Total rows across all files: {total_rows}")
print(f"Counting rows took: {time.time() - start:.2f} seconds")

start = time.time()
print("\nSchema of the 'people' table:")
print(df.collect_schema())
print(f"Schema collection took: {time.time() - start:.2f} seconds")

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

start = time.time()
df.filter(pl.col("age") >= 50).write_json("filtered.json")
print(f"Filter and export operation took: {time.time() - start:.2f} seconds")

print(f"\nTotal processing time: {time.time() - start_total:.2f} seconds")
