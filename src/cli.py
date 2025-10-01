import typer
from rich.console import Console
from rich.prompt import Prompt
from rich.panel import Panel
from rich import print as rprint
from faker import Faker
from typing import Optional
from pathlib import Path

# Import configuration
from config import (
    DATA_DIR,
    FAKE_CSVS_DIR,
    FAKE_JSONS_DIR,
    PARQUETS_DIR,
    SQLITE_DB_PATH,
)

# Import services
from data_generator import DataGenerator
from data_converter import DataConverter
from data_ingestor import DataIngestorService
from polars_process.polars_processor import PolarsProcessor
from duckdb_process.duckdb_processor import DuckDBProcessor
from polars_process.polars_streaming import (
    test_eager_mode,
    test_lazy_without_streaming,
    test_lazy_with_streaming,
    test_streaming_with_sink,
)
from polars_process.gpu_engine import (
    check_gpu_availability,
    test_cpu_execution,
    test_gpu_execution,
    test_streaming_gpu,
)

console = Console()
app = typer.Typer(
    help="A CLI for processing and generating data with Polars and DuckDB."
)

# Initialize core dependencies
faker_instance = Faker()
prompt_instance: Prompt = Prompt()  # Added type hint

# Initialize services with their dependencies
data_generator_service = DataGenerator(
    fake=faker_instance,
    fake_csvs_dir=FAKE_CSVS_DIR,
    fake_jsons_dir=FAKE_JSONS_DIR,
    console=console,
    prompt=prompt_instance,  # Pass the prompt instance
)
data_converter_service = DataConverter(
    fake_csvs_dir=FAKE_CSVS_DIR,
    parquets_dir=PARQUETS_DIR,
    console=console,
)
data_ingestor_service = DataIngestorService(
    fake_csvs_dir=FAKE_CSVS_DIR,
    fake_jsons_dir=FAKE_JSONS_DIR,
    sqlite_db_path=SQLITE_DB_PATH,
    console=console,
    prompt=prompt_instance,
)
polars_processor_service = PolarsProcessor(
    fake_csvs_dir=FAKE_CSVS_DIR,
    fake_jsons_dir=FAKE_JSONS_DIR,
    data_dir=DATA_DIR,
    console=console,
    prompt=prompt_instance,
)
duckdb_processor_service = DuckDBProcessor(
    fake_csvs_dir=FAKE_CSVS_DIR,
    fake_jsons_dir=FAKE_JSONS_DIR,
    data_dir=DATA_DIR,
    console=console,
    prompt=prompt_instance,
)


@app.command(name="generate", help="Generate fake data (CSV or JSON).")
def generate_data(
    file_type: Optional[str] = typer.Option(
        None, "--file-type", "-t", help="Type of file to generate (csv or json)."
    ),
    num_files: Optional[int] = typer.Option(
        None, "--num-files", "-n", help="Number of fake files to generate."
    ),
    rows_per_file: Optional[int] = typer.Option(
        None,
        "--rows-per-file",
        "-r",
        help="Number of rows per file.",
    ),
) -> None:
    """
    Generates fake CSV or JSON files.
    """
    data_generator_service.generate_data(file_type, num_files, rows_per_file)


@app.command(name="convert", help="Convert data between formats.")
def convert_data(
    source_type: Optional[str] = typer.Option(
        None, "--from", "-f", help="Source file type (e.g., csv)."
    ),
    target_type: Optional[str] = typer.Option(
        None, "--to", "-t", help="Target file type (e.g., parquet)."
    ),
) -> None:
    """
    Converts files from one format to another.
    """
    data_converter_service.convert_data(source_type, target_type)


@app.command(name="ingest", help="Ingest data into a database.")
def ingest_data(
    file_type: Optional[str] = typer.Option(
        None, "--file-type", "-t", help="Type of file to ingest (csv or json)."
    ),
    engine: Optional[str] = typer.Option(
        None, "--engine", "-e", help="Database engine to use (e.g., sqlite)."
    ),
    table_name: Optional[str] = typer.Option(
        "my_table", "--table-name", "-tn", help="Name of the table to ingest into."
    ),
) -> None:
    """
    Ingests data from specified file types into a database.
    """
    data_ingestor_service.ingest_data(file_type, engine, table_name)


@app.command(name="process", help="Perform data processing operations.")
def process_data(
    file_type: Optional[str] = typer.Option(
        None, "--file-type", "-t", help="Type of file to process (csv or json)."
    ),
    engine: Optional[str] = typer.Option(
        None,
        "--engine",
        "-e",
        help="Processing engine to use (polars-basic, polars-streaming, polars-gpu, duckdb).",
    ),
) -> None:
    """
    Performs data processing operations using various engines.
    """
    if file_type is None:
        file_type = prompt_instance.ask(
            "[bold blue]Enter file type to process[/bold blue] (csv or json)",
            choices=["csv", "json"],
            default="csv",
        )
    if engine is None:
        engine = prompt_instance.ask(
            "[bold blue]Enter processing engine to use[/bold blue]",
            choices=["polars-basic", "polars-streaming", "polars-gpu", "duckdb"],
            default="polars-basic",
        )

    if engine.lower() == "polars-basic":
        polars_processor_service.process_data(file_type)
    elif engine.lower() == "duckdb":
        duckdb_processor_service.process_data(file_type)
    elif engine.lower() == "polars-streaming":
        rprint(
            f"[bold blue]Starting Polars streaming processing for {file_type.upper()} files...[/bold blue]"
        )
        try:
            path_obj: Optional[Path] = None
            if file_type.lower() == "csv":
                path_obj = FAKE_CSVS_DIR / "*.csv"
            elif file_type.lower() == "json":
                path_obj = FAKE_JSONS_DIR / "*.json"
            else:
                rprint(
                    f"[bold red]Error:[/bold red] Unsupported file type: {file_type}. Choose 'csv' or 'json'."
                )
                raise typer.Exit(code=1)

            rprint("\n1️⃣  MODO EAGER (read_csv + operações)")
            df_eager, time_eager = test_eager_mode(path_obj.as_posix())
            rprint(f"⏱️  Tempo: {time_eager:.2f}s")
            rprint(f"📊 Resultado:\n{df_eager}")

            rprint("\n2️⃣  MODO LAZY SEM STREAMING (scan_csv + collect)")
            df_lazy, time_lazy = test_lazy_without_streaming(path_obj.as_posix())
            rprint(f"⏱️  Tempo: {time_lazy:.2f}s")
            rprint(f"📊 Resultado:\n{df_lazy}")

            rprint("\n3️⃣  MODO LAZY COM STREAMING (scan_csv + collect(streaming=True))")
            df_streaming, time_streaming = test_lazy_with_streaming(path_obj.as_posix())
            rprint(f"⏱️  Tempo: {time_streaming:.2f}s")
            rprint(f"📊 Resultado:\n{df_streaming}")

            rprint("\n4️⃣  STREAMING COM SINK (processa e salva sem carregar)")
            output_file = DATA_DIR / f"filtered_streaming_{file_type}.parquet"
            time_sink = test_streaming_with_sink(
                path_obj.as_posix(), output_file.as_posix()
            )
            rprint(f"⏱️  Tempo: {time_sink:.2f}s")
            rprint(f"💾 Arquivo salvo: {output_file}")

            rprint(
                Panel(
                    f"[bold green]Polars streaming processing complete for {file_type.upper()} files.[/bold green]",
                    title="Processing Complete",
                    style="green",
                )
            )

        except Exception as e:
            rprint(
                f"[bold red]Error during Polars streaming processing:[/bold red] {e}"
            )
            raise typer.Exit(code=1)

    elif engine.lower() == "polars-gpu":
        rprint(
            f"[bold blue]Starting Polars GPU processing for {file_type.upper()} files...[/bold blue]"
        )
        try:
            path_obj: Optional[Path] = None
            if file_type.lower() == "csv":
                path_obj = FAKE_CSVS_DIR / "*.csv"
            elif file_type.lower() == "json":
                path_obj = FAKE_JSONS_DIR / "*.json"
            else:
                rprint(
                    f"[bold red]Error:[/bold red] Unsupported file type: {file_type}. Choose 'csv' or 'json'."
                )
                raise typer.Exit(code=1)

            rprint("\n🔍 Checking GPU availability...")
            gpu_available = check_gpu_availability()

            if gpu_available:
                rprint("[bold green]✅ GPU Engine available![/bold green]\n")
            else:
                rprint(
                    "[bold yellow]⚠️  GPU Engine not available. Install with:[/bold yellow]"
                )
                rprint("   pip install polars-gpu-cu12  # For CUDA 12")
                rprint("   or")
                rprint("   pip install polars-gpu-cu11  # For CUDA 11\n")

            rprint("\n1️⃣  CPU EXECUTION (baseline)")
            df_cpu, time_cpu = test_cpu_execution(path_obj.as_posix())
            rprint(f"⏱️  Time: {time_cpu:.3f}s")
            rprint(f"📊 Result:\n{df_cpu}\n")

            if gpu_available:
                rprint("\n2️⃣  GPU EXECUTION (engine='gpu')")
                df_gpu, time_gpu = test_gpu_execution(path_obj.as_posix())
                if df_gpu is not None:
                    rprint(f"⏱️  Time: {time_gpu:.3f}s")
                    rprint(f"🚀 Speedup: {time_cpu / time_gpu:.2f}x faster")
                    rprint(f"📊 Result:\n{df_gpu}\n")

                rprint("\n3️⃣  GPU + STREAMING EXECUTION (datasets > VRAM)")
                result_df_streaming, time_streaming = test_streaming_gpu(
                    path_obj.as_posix()
                )
                if result_df_streaming is not None:
                    df_streaming = result_df_streaming
                    rprint(f"⏱️  Time: {time_streaming:.3f}s")
                    rprint(f"📊 First rows:\n{df_streaming.head()}\n")

            rprint(
                Panel(
                    f"[bold green]Polars GPU processing complete for {file_type.upper()} files.[/bold green]",
                    title="Processing Complete",
                    style="green",
                )
            )

        except Exception as e:
            rprint(f"[bold red]Error during Polars GPU processing:[/bold red] {e}")
            raise typer.Exit(code=1)

    else:
        rprint(f"[bold red]Error:[/bold red] Unsupported processing engine: {engine}.")
        raise typer.Exit(code=1)


if __name__ == "__main__":
    app()
else:
    # This ensures the app runs even if the module is imported
    app()
