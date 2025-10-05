import polars as pl
import time
from pathlib import Path
from typing import Tuple, Optional


def check_gpu_availability() -> bool:
    """Check if GPU support is available."""
    try:
        df = pl.LazyFrame({"test": [1, 2, 3]})
        df.select(pl.col("test")).collect(engine="gpu")
        return True
    except Exception as e:
        print(f"GPU not available: {e}")
        return False


def build_query(path: str) -> pl.LazyFrame:
    """Build the common query logic used by both CPU and GPU tests."""
    return (
        pl.scan_csv(path)
        .filter(pl.col("age") > 30)
        .with_columns(
            [
                (pl.col("age") * 1.1).alias("age_adjusted"),
                pl.col("name").str.to_uppercase().alias("name_upper"),
            ]
        )
        .group_by("name")
        .agg(
            [
                pl.col("age").mean().alias("avg_age"),
                pl.col("age").max().alias("max_age"),
                pl.len().alias("count"),
            ]
        )
        .filter(pl.col("count") > 1)
        .sort("avg_age", descending=True)
        .limit(10)
    )


def test_cpu_execution(path: str) -> Tuple[pl.DataFrame, float]:
    """Execute query in CPU mode."""
    start = time.time()
    df = build_query(path).collect()
    elapsed = time.time() - start
    return df, elapsed


def test_gpu_execution(path: str) -> Tuple[Optional[pl.DataFrame], float]:
    """Execute query in GPU mode."""
    start = time.time()

    try:
        df = build_query(path).collect(engine="gpu")
        elapsed = time.time() - start
        return df, elapsed
    except Exception as e:
        elapsed = time.time() - start
        print(f"Error in GPU execution: {e}")
        return None, elapsed


def test_streaming_gpu(path: str) -> Tuple[Optional[pl.DataFrame], float]:
    """Execute query with GPU + streaming for datasets larger than VRAM."""
    start = time.time()

    try:
        df = (
            pl.scan_csv(path)
            .filter(pl.col("age") >= 50)
            .select(["name", "email", "age"])
            .with_columns([(pl.col("age") / 10).round(0).alias("age_decade")])
            .collect(engine="gpu", streaming=True)
        )
        elapsed = time.time() - start
        return df, elapsed
    except Exception as e:
        elapsed = time.time() - start
        print(f"Error in GPU streaming: {e}")
        return None, elapsed


if __name__ == "__main__":
    FILE_TYPE = "csv"
    path = Path(f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}")

    print("=" * 70)
    print("PERFORMANCE TEST: POLARS GPU ENGINE")
    print("=" * 70)

    print("\nChecking GPU availability...")
    gpu_available = check_gpu_availability()

    if gpu_available:
        print("GPU Engine available!\n")
    else:
        print("GPU Engine not available. Install with:")
        print("  pip install polars-gpu-cu12  # For CUDA 12")
        print("  pip install polars-gpu-cu11  # For CUDA 11\n")

    print("\n1. CPU EXECUTION (baseline)")
    print("-" * 70)
    df_cpu, time_cpu = test_cpu_execution(path.as_posix())
    print(f"Time: {time_cpu:.3f}s")
    print(f"Result:\n{df_cpu}\n")

    if gpu_available:
        print("\n2. GPU EXECUTION (engine='gpu')")
        print("-" * 70)
        df_gpu, time_gpu = test_gpu_execution(path.as_posix())
        if df_gpu is not None:
            print(f"Time: {time_gpu:.3f}s")
            print(f"Speedup: {time_cpu / time_gpu:.2f}x faster")
            print(f"Result:\n{df_gpu}\n")

        print("\n3. GPU + STREAMING (datasets > VRAM)")
        print("-" * 70)
        df_streaming, time_streaming = test_streaming_gpu(path.as_posix())
        if df_streaming is not None:
            print(f"Time: {time_streaming:.3f}s")
            print(f"First rows:\n{df_streaming.head()}\n")

    print("\n" + "=" * 70)
    print("PERFORMANCE SUMMARY")
    print("=" * 70)
    print(f"CPU:                    {time_cpu:.3f}s")

    if gpu_available and df_gpu is not None:
        print(f"GPU:                    {time_gpu:.3f}s ({time_cpu / time_gpu:.2f}x speedup)")
        if df_streaming is not None:
            print(f"GPU + Streaming:        {time_streaming:.3f}s")

    print("\nIMPORTANT NOTES:")
    print("-" * 70)
    print("- GPU engine is in Open Beta (active development)")
    print("- Not all operations are supported (fallback to CPU)")
    print("- Speedup is greater on large datasets and heavy operations")
    print("- GPU streaming allows processing data larger than available VRAM")
    print("- Requires NVIDIA GPU with CUDA 11.2+ or CUDA 12+")

    print("\nINSTALLATION:")
    print("-" * 70)
    print("# For CUDA 12.x")
    print("pip install polars-gpu-cu12")
    print("\n# For CUDA 11.x")
    print("pip install polars-gpu-cu11")
    print("\n# Check CUDA version:")
    print("nvidia-smi")
