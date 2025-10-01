import polars as pl
from dataclasses import dataclass
from pathlib import Path

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


@dataclass
class DatabaseIngestor:
    db_uri: str
    table_name: str

    def ingest_dataframe(self, df: pl.DataFrame):
        """
        Ingesta um DataFrame no banco de dados especificado.

        Args:
            df (pl.DataFrame): DataFrame a ser ingerido

        Returns:
            None

        Exemplo:
            >>> ingestor = DatabaseIngestor("sqlite:///example.db", "my_table")
            >>> ingestor.ingest_dataframe(df)
        """
        df.write_database(
            table_name=self.table_name,
            connection=self.db_uri,
            if_table_exists="replace",
        )


if __name__ == "__main__":
    file_reader = FileReader(
        path=f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}", file_type=FILE_TYPE
    )
    df = file_reader.read_files()

    db_ingestor = DatabaseIngestor(db_uri="sqlite:///data/example.db", table_name="my_table")
    db_ingestor.ingest_dataframe(df)

    print(f"Data ingested into {db_ingestor.db_uri} in table {db_ingestor.table_name}.")
