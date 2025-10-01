import time
from pathlib import Path
from typing import Optional

import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt


class PolarsProcessor:
    def __init__(
        self,
        fake_csvs_dir: Path,
        fake_jsons_dir: Path,
        data_dir: Path,
        console: Console,
        prompt: Prompt,
    ):
        self.fake_csvs_dir = fake_csvs_dir
        self.fake_jsons_dir = fake_jsons_dir
        self.data_dir = data_dir
        self.console = console
        self.prompt = prompt

    def process_data(
        self,
        file_type: Optional[str],
    ):
        if file_type is None:
            file_type = self.prompt.ask(
                "[bold blue]Enter file type to process[/bold blue] (csv or json)",
                choices=["csv", "json"],
                default="csv",
            )

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

        self.console.print(
            f"[bold blue]Starting Polars basic processing for {file_type.upper()} files...[/bold blue]"
        )
        try:
            # The FileReader class is now in data_ingestor.py, so we need to import it
            from src.data_ingestor import FileReader

            file_reader = FileReader(path=path_obj, file_type=file_type)
            df = file_reader.read_files()

            start = time.time()
            total_rows = df.select(pl.len())
            self.console.print(f"Total rows across all files: {total_rows}")
            self.console.print(f"Counting rows took: {time.time() - start:.2f} seconds")

            start = time.time()
            self.console.print("\nSchema of the 'people' table:")
            self.console.print(df.collect_schema())
            self.console.print(f"Schema collection took: {time.time() - start:.2f} seconds")

            start = time.time()
            result = (
                df.group_by("name")
                .agg(pl.col("age").mean().alias("avg_age"))
                .filter(pl.col("avg_age") > 30)
                .sort("avg_age", descending=True)
                .limit(5)
            )
            self.console.print("\nTop 5 names with average age > 30:")
            self.console.print(result)
            self.console.print(f"Aggregation operation took: {time.time() - start:.2f} seconds")

            start = time.time()
            output_file = self.data_dir / f"filtered_{file_type}.json"
            df.filter(pl.col("age") >= 50).write_json(output_file)
            self.console.print(
                f"Filter and export operation took: {time.time() - start:.2f} seconds"
            )
            self.console.print(f"Filtered data exported to [green]{output_file}[/green].")

            self.console.print(
                Panel(
                    f"[bold green]Polars basic processing complete for {file_type.upper()} files.[/bold green]",
                    title="Processing Complete",
                    style="green",
                )
            )

        except Exception as e:
            self.console.print(f"[bold red]Error during Polars basic processing:[/bold red] {e}")
            raise SystemExit(1)
