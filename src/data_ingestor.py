from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt


@dataclass
class FileReader:
    path: str | Path
    file_type: str

    def read_csv_files(self) -> pl.DataFrame:
        return pl.read_csv(self.path)

    def read_json_files(self) -> pl.DataFrame:
        return pl.read_json(self.path)

    def read_files(self) -> pl.DataFrame:
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
        df.write_database(
            table_name=self.table_name,
            connection=self.db_uri,
            if_table_exists="replace",
        )


class DataIngestorService:
    def __init__(
        self,
        fake_csvs_dir: Path,
        fake_jsons_dir: Path,
        sqlite_db_path: Path,
        console: Console,
        prompt: Prompt,
    ):
        self.fake_csvs_dir = fake_csvs_dir
        self.fake_jsons_dir = fake_jsons_dir
        self.sqlite_db_path = sqlite_db_path
        self.console = console
        self.prompt = prompt

    def ingest_data(
        self,
        file_type: Optional[str],
        engine: Optional[str],
        table_name: Optional[str],
    ):
        if file_type is None:
            file_type = self.prompt.ask(
                "[bold blue]Enter file type to ingest[/bold blue] (csv or json)",
                choices=["csv", "json"],
                default="csv",
            )
        if engine is None:
            engine = self.prompt.ask(
                "[bold blue]Enter database engine to use[/bold blue] (e.g., sqlite)",
                choices=["sqlite"],
                default="sqlite",
            )
        if table_name is None:
            table_name = self.prompt.ask(
                "[bold blue]Enter name of the table to ingest into[/bold blue]",
                default="my_table",
            )

        if engine.lower() == "sqlite":
            self.console.print("[bold blue]Starting data ingestion into SQLite...[/bold blue]")

            path_obj = None
            if file_type.lower() == "csv":
                path_obj = self.fake_csvs_dir / "*.csv"
            elif file_type.lower() == "json":
                path_obj = self.fake_jsons_dir / "*.json"
            else:
                self.console.print(
                    f"[bold red]Error:[/bold red] Unsupported file type: {file_type}. Choose 'csv' or 'json'."
                )
                raise SystemExit(1)

            try:
                file_reader = FileReader(path=path_obj, file_type=file_type)
                df = file_reader.read_files()

                db_ingestor = DatabaseIngestor(
                    db_uri=f"sqlite:///{self.sqlite_db_path}", table_name=table_name
                )
                db_ingestor.ingest_dataframe(df)

                self.console.print(
                    Panel(
                        f"[bold green]Successfully ingested data from {file_type.upper()} files into SQLite table '{table_name}'.[/bold green]",
                        title="Ingestion Complete",
                        style="green",
                    )
                )
            except Exception as e:
                self.console.print(f"[bold red]Error during ingestion:[/bold red] {e}")
                raise SystemExit(1)

        else:
            self.console.print(
                f"[bold red]Error:[/bold red] Unsupported database engine: {engine}. Currently only 'sqlite' is supported."
            )
            raise SystemExit(1)
