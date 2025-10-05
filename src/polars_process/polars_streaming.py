import polars as pl
import time
from pathlib import Path
from typing import Tuple

FILE_TYPE = "csv"


def test_eager_mode(path: str) -> Tuple[pl.DataFrame, float]:
    """Test eager mode processing (loads all data into memory)."""
    start = time.time()

    df = (
        pl.read_csv(path)
        .filter(pl.col("age") > 30)
        .group_by("name")
        .agg(pl.col("age").mean().alias("avg_age"))
        .sort("avg_age", descending=True)
        .limit(10)
    )

    elapsed = time.time() - start
    return df, elapsed


def test_lazy_without_streaming(path: str) -> Tuple[pl.DataFrame, float]:
    """Test lazy mode without streaming (optimized but in-memory)."""
    start = time.time()

    df = (
        pl.scan_csv(path)
        .filter(pl.col("age") > 30)
        .group_by("name")
        .agg(pl.col("age").mean().alias("avg_age"))
        .sort("avg_age", descending=True)
        .limit(10)
        .collect()
    )

    elapsed = time.time() - start
    return df, elapsed


def test_lazy_with_streaming(path: str) -> Tuple[pl.DataFrame, float]:
    """Test lazy mode with streaming (processes in chunks)."""
    start = time.time()

    df = (
        pl.scan_csv(path)
        .filter(pl.col("age") > 30)
        .group_by("name")
        .agg(pl.col("age").mean().alias("avg_age"))
        .sort("avg_age", descending=True)
        .limit(10)
        .collect(engine='streaming')
    )

    elapsed = time.time() - start
    return df, elapsed


def test_streaming_with_sink(path: str, output_path: str) -> float:
    """Test streaming with sink (writes directly without loading result)."""
    start = time.time()

    (
        pl.scan_csv(path)
        .filter(pl.col("age") >= 50)
        .select(["name", "email", "age"])
        .sink_parquet(output_path)
    )

    elapsed = time.time() - start
    return elapsed


if __name__ == "__main__":
    path = Path(f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}")

    print("=" * 60)
    print("PERFORMANCE TEST: POLARS STREAMING ENGINE")
    print("=" * 60)

    print("\n1. EAGER MODE (read_csv + operations)")
    print("-" * 60)
    df_eager, time_eager = test_eager_mode(path.as_posix())
    print(f"Time: {time_eager:.2f}s")
    print(f"Result:\n{df_eager}")

    print("\n2. LAZY MODE WITHOUT STREAMING (scan_csv + collect)")
    print("-" * 60)
    df_lazy, time_lazy = test_lazy_without_streaming(path.as_posix())
    print(f"Time: {time_lazy:.2f}s")
    print(f"Result:\n{df_lazy}")

    print("\n3. LAZY MODE WITH STREAMING (scan_csv + collect(streaming=True))")
    print("-" * 60)
    df_streaming, time_streaming = test_lazy_with_streaming(path.as_posix())
    print(f"Time: {time_streaming:.2f}s")
    print(f"Result:\n{df_streaming}")

    print("\n4. STREAMING WITH SINK (process and save without loading)")
    print("-" * 60)
    output_file = "filtered_streaming.parquet"
    time_sink = test_streaming_with_sink(path.as_posix(), output_file)
    print(f"Time: {time_sink:.2f}s")
    print(f"File saved: {output_file}")

    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON")
    print("=" * 60)
    print(f"Eager:              {time_eager:.2f}s (baseline)")
    print(f"Lazy:               {time_lazy:.2f}s ({time_lazy / time_eager:.2f}x)")
    print(f"Lazy + Streaming:   {time_streaming:.2f}s ({time_streaming / time_eager:.2f}x)")
    print(f"Streaming + Sink:   {time_sink:.2f}s ({time_sink / time_eager:.2f}x)")

    print("\nWHEN TO USE EACH MODE:")
    print("-" * 60)
    print("- EAGER: Small datasets, quick prototyping")
    print("- LAZY: Query optimization, data that fits in RAM")
    print("- STREAMING: Data larger than RAM, ETL pipelines")
    print("- SINK: Process and save without loading result in memory")
