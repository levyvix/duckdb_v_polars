import polars as pl
import time
from pathlib import Path
from typing import Tuple

FILE_TYPE = "csv"


def test_eager_mode(path: str) -> Tuple[pl.DataFrame, float]:
    """
    Testa processamento no modo eager (carrega tudo na memória).

    Args:
        path: Caminho para os arquivos com wildcard

    Returns:
        Tuple[pl.DataFrame, float]: DataFrame resultado e tempo de execução

    Exemplo:
        >>> df, elapsed = test_eager_mode("data/*.csv")
    """
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
    """
    Testa processamento lazy sem streaming (otimizado mas na memória).

    Args:
        path: Caminho para os arquivos com wildcard

    Returns:
        Tuple[pl.DataFrame, float]: DataFrame resultado e tempo de execução

    Exemplo:
        >>> df, elapsed = test_lazy_without_streaming("data/*.csv")
    """
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
    """
    Testa processamento lazy COM streaming (processa em chunks).

    Args:
        path: Caminho para os arquivos com wildcard

    Returns:
        Tuple[pl.DataFrame, float]: DataFrame resultado e tempo de execução

    Exemplo:
        >>> df, elapsed = test_lazy_with_streaming("data/*.csv")
    """
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
    """
    Testa streaming com sink (escreve diretamente sem carregar resultado).

    Args:
        path: Caminho para os arquivos com wildcard
        output_path: Caminho para salvar o resultado

    Returns:
        float: Tempo de execução

    Exemplo:
        >>> elapsed = test_streaming_with_sink("data/*.csv", "output.parquet")
    """
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
    print("TESTE DE PERFORMANCE: POLARS STREAMING ENGINE")
    print("=" * 60)

    print("\n1️⃣  MODO EAGER (read_csv + operações)")
    print("-" * 60)
    df_eager, time_eager = test_eager_mode(path.as_posix())
    print(f"⏱️  Tempo: {time_eager:.2f}s")
    print(f"📊 Resultado:\n{df_eager}")

    print("\n2️⃣  MODO LAZY SEM STREAMING (scan_csv + collect)")
    print("-" * 60)
    df_lazy, time_lazy = test_lazy_without_streaming(path.as_posix())
    print(f"⏱️  Tempo: {time_lazy:.2f}s")
    print(f"📊 Resultado:\n{df_lazy}")

    print("\n3️⃣  MODO LAZY COM STREAMING (scan_csv + collect(streaming=True))")
    print("-" * 60)
    df_streaming, time_streaming = test_lazy_with_streaming(path.as_posix())
    print(f"⏱️  Tempo: {time_streaming:.2f}s")
    print(f"📊 Resultado:\n{df_streaming}")

    print("\n4️⃣  STREAMING COM SINK (processa e salva sem carregar)")
    print("-" * 60)
    output_file = "filtered_streaming.parquet"
    time_sink = test_streaming_with_sink(path.as_posix(), output_file)
    print(f"⏱️  Tempo: {time_sink:.2f}s")
    print(f"💾 Arquivo salvo: {output_file}")

    print("\n" + "=" * 60)
    print("COMPARAÇÃO DE PERFORMANCE")
    print("=" * 60)
    print(f"Eager:              {time_eager:.2f}s (baseline)")
    print(f"Lazy:               {time_lazy:.2f}s ({time_lazy / time_eager:.2f}x)")
    print(
        f"Lazy + Streaming:   {time_streaming:.2f}s ({time_streaming / time_eager:.2f}x)"
    )
    print(f"Streaming + Sink:   {time_sink:.2f}s ({time_sink / time_eager:.2f}x)")

    print("\n💡 QUANDO USAR CADA MODO:")
    print("-" * 60)
    print("• EAGER: Dados pequenos, prototipagem rápida")
    print("• LAZY: Otimização de queries, dados que cabem na RAM")
    print("• STREAMING: Dados maiores que RAM, pipelines ETL")
    print("• SINK: Processar e salvar sem carregar resultado na memória")
