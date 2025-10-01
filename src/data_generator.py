from pathlib import Path
from typing import Optional

import polars as pl
from faker import Faker
from rich.console import Console
from rich.panel import Panel
from rich.progress import track
from rich.prompt import Prompt


class DataGenerator:
    def __init__(
        self,
        fake: Faker,
        fake_csvs_dir: Path,
        fake_jsons_dir: Path,
        console: Console,
        prompt: Prompt,
    ):
        self.fake = fake
        self.fake_csvs_dir = fake_csvs_dir
        self.fake_jsons_dir = fake_jsons_dir
        self.console = console
        self.prompt = prompt

    def generate_data(
        self,
        file_type: Optional[str],
        num_files: Optional[int],
        rows_per_file: Optional[int],
    ):
        if file_type is None:
            file_type = self.prompt.ask(
                "[bold blue]Enter file type to generate[/bold blue] (csv or json)",
                choices=["csv", "json"],
                default="csv",
            )
        if num_files is None:
            num_files = int(
                self.prompt.ask(
                    "[bold blue]Enter number of fake files to generate[/bold blue]",
                    default="100",
                )
            )
        if rows_per_file is None:
            rows_per_file = int(
                self.prompt.ask(
                    "[bold blue]Enter number of rows per file[/bold blue]",
                    default="100",
                )
            )

        output_dir = None

        if file_type.lower() == "csv":
            output_dir = self.fake_csvs_dir
        elif file_type.lower() == "json":
            output_dir = self.fake_jsons_dir
        else:
            self.console.print(
                f"[bold red]Error:[/bold red] Unsupported file type: {file_type}. Choose 'csv' or 'json'."
            )
            raise SystemExit(1)

        self.console.print(
            f"Generating {num_files} fake {file_type.upper()} files with {rows_per_file} rows each in [bold green]{output_dir}[/bold green]..."
        )

        for i in track(
            range(int(num_files)),
            description=f"Generating {file_type.upper()} files...",
        ):
            file_name = f"data_{i}.{file_type.lower()}"
            file_path = output_dir / file_name

            if file_path.exists():
                continue

            data = {
                "name": [self.fake.name() for _ in range(int(rows_per_file))],
                "email": [self.fake.email() for _ in range(int(rows_per_file))],
                "age": [
                    self.fake.random_int(min=18, max=80)
                    for _ in range(int(rows_per_file))
                ],
            }
            df = pl.DataFrame(data)

            if file_type.lower() == "csv":
                df.write_csv(file_path)
            elif file_type.lower() == "json":
                df.write_json(file_path)

        self.console.print(
            Panel(
                f"[bold green]Successfully generated {num_files} fake {file_type.upper()} files.[/bold green]",
                title="Generation Complete",
                style="green",
            )
        )
