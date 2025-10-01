from pathlib import Path
from typing import Optional

import duckdb  # type: ignore
from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt


class DuckDBProcessor:
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
            f"[bold blue]Starting DuckDB processing for {file_type.upper()} files...[/bold blue]"
        )
        try:
            con = duckdb.connect()

            # Load data once into memory
            con.execute(f"""
            COPY (SELECT * FROM '{path_obj}') TO 'people.parquet' (FORMAT PARQUET);
            CREATE VIEW people AS SELECT * FROM 'people.parquet';
            """)
            self.console.print("Database initialization complete.")

            # Count total rows
            total_rows = con.execute("SELECT COUNT(*) FROM people").fetchone()
            self.console.print(f"Total rows across all files: {total_rows}")

            # Describe
            self.console.print("Schema of the 'people' table:")
            self.console.print(con.execute("DESCRIBE people").fetchdf())

            # Aggregate data
            result = con.execute("""
            SELECT name, AVG(age) AS avg_age
            FROM people
            GROUP BY name
            HAVING AVG(age) > 30
            ORDER BY avg_age DESC
            LIMIT 5
            """).fetchdf()
            self.console.print("Top 5 names with average age > 30:")
            self.console.print(result)

            # Filter and export
            output_file = self.data_dir / f"filtered_duckdb_{file_type}.csv"
            con.execute(f"""
            COPY (
                SELECT name, email, age
                FROM people
                WHERE age >= 50
            ) TO '{output_file}' (HEADER, DELIMITER ',')
            """)
            self.console.print(
                f"Filtered data exported to [green]{output_file}[/green]."
            )
            con.close()

            self.console.print(
                Panel(
                    f"[bold green]DuckDB processing complete for {file_type.upper()} files.[/bold green]",
                    title="Processing Complete",
                    style="green",
                )
            )

        except Exception as e:
            self.console.print(
                f"[bold red]Error during DuckDB processing:[/bold red] {e}"
            )
            raise SystemExit(1)
