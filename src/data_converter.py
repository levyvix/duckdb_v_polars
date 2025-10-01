from pathlib import Path
from typing import Optional

import polars as pl
from rich.console import Console
from rich.panel import Panel
from rich.progress import track


class DataConverter:
    def __init__(
        self,
        fake_csvs_dir: Path,
        parquets_dir: Path,
        console: Console,
    ):
        self.fake_csvs_dir = fake_csvs_dir
        self.parquets_dir = parquets_dir
        self.console = console

    def convert_data(
        self,
        source_type: Optional[str],
        target_type: Optional[str],
    ):
        if source_type is None:
            source_type = self.console.input(
                "[bold blue]Enter source file type[/bold blue] (e.g., csv)",
                choices=["csv"],
                default="csv",
            )
        if target_type is None:
            target_type = self.console.input(
                "[bold blue]Enter target file type[/bold blue] (e.g., parquet)",
                choices=["parquet"],
                default="parquet",
            )

        if source_type.lower() == "csv" and target_type.lower() == "parquet":
            self.console.print(
                "[bold blue]Starting incremental CSV to Parquet conversion...[/bold blue]"
            )

            # 1. Discover which CSVs have already been converted
            converted = {f.stem for f in self.parquets_dir.glob("*.parquet")}

            csv_files_to_convert = [
                f for f in self.fake_csvs_dir.glob("*.csv") if f.stem not in converted
            ]

            if not csv_files_to_convert:
                self.console.print(
                    Panel(
                        "[bold yellow]No new CSV files to convert to Parquet.[/bold yellow]",
                        title="Conversion Status",
                        style="yellow",
                    )
                )
                raise SystemExit()

            for csv_file in track(
                csv_files_to_convert, description="Converting CSVs to Parquet..."
            ):
                self.console.print(f"  Converting [green]{csv_file.name}[/green]...")
                df = pl.read_csv(csv_file)
                df.write_parquet(self.parquets_dir / f"{csv_file.stem}.parquet")

            self.console.print(
                Panel(
                    f"[bold green]Successfully converted {len(csv_files_to_convert)} new CSV files to Parquet.[/bold green]",
                    title="Conversion Complete",
                    style="green",
                )
            )

        else:
            self.console.print(
                f"[bold red]Error:[/bold red] Unsupported conversion: {source_type} to {target_type}."
            )
            raise SystemExit(1)
