import polars as pl

FILE_TYPE = "csv"


def read_files(path: str, file_type: str) -> pl.DataFrame:
    """Read files based on the specified type."""
    if file_type.lower() == "csv":
        return pl.read_csv(path)
    if file_type.lower() == "json":
        return pl.read_json(path)
    raise ValueError(f"Unsupported file type: {file_type}. Use 'csv' or 'json'.")


def ingest_to_database(df: pl.DataFrame, db_uri: str, table_name: str):
    """Write DataFrame to database, replacing table if it exists."""
    df.write_database(
        table_name=table_name,
        connection=db_uri,
        if_table_exists="replace",
    )


if __name__ == "__main__":
    # Read files
    file_path = f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}"
    df = read_files(file_path, FILE_TYPE)

    # Write to database
    db_uri = "sqlite:///data/example.db"
    table_name = "my_table"
    ingest_to_database(df, db_uri, table_name)

    print(f"Data ingested into {db_uri} in table {table_name}.")
