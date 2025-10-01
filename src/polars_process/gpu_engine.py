import polars as pl
import time
from pathlib import Path
from typing import Tuple, Optional


def check_gpu_availability() -> bool:
    """
    Verifica se o suporte a GPU está disponível.

    Returns:
        bool: True se GPU está disponível, False caso contrário

    Exemplo:
        >>> if check_gpu_availability():
        ...     print("GPU disponível!")
    """
    try:
        df = pl.LazyFrame({"test": [1, 2, 3]})
        df.select(pl.col("test")).collect(engine="gpu")
        return True
    except Exception as e:
        print(f"⚠️  GPU não disponível: {e}")
        return False


def test_cpu_execution(path: str) -> Tuple[pl.DataFrame, float]:
    """
    Executa query no modo CPU padrão.

    Args:
        path: Caminho para os arquivos com wildcard

    Returns:
        Tuple[pl.DataFrame, float]: DataFrame resultado e tempo de execução

    Exemplo:
        >>> df, elapsed = test_cpu_execution("data/*.csv")
    """
    start = time.time()

    df = (
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
        .collect()
    )

    elapsed = time.time() - start
    return df, elapsed


def test_gpu_execution(path: str) -> Tuple[Optional[pl.DataFrame], float]:
    """
    Executa query no modo GPU acelerado.

    Args:
        path: Caminho para os arquivos com wildcard

    Returns:
        Tuple[Optional[pl.DataFrame], float]: DataFrame resultado e tempo (None se falhar)

    Exemplo:
        >>> df, elapsed = test_gpu_execution("data/*.csv")
    """
    start = time.time()

    try:
        df = (
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
            .collect(engine="gpu")
        )

        elapsed = time.time() - start
        return df, elapsed
    except Exception as e:
        elapsed = time.time() - start
        print(f"❌ Erro na execução GPU: {e}")
        return None, elapsed


def test_streaming_gpu(path: str) -> Tuple[Optional[pl.DataFrame], float]:
    """
    Executa query com GPU + streaming (para datasets maiores que VRAM).

    Args:
        path: Caminho para os arquivos com wildcard

    Returns:
        Tuple[Optional[pl.DataFrame], float]: DataFrame resultado e tempo

    Exemplo:
        >>> df, elapsed = test_streaming_gpu("data/*.csv")
    """
    start = time.time()

    try:
        df = (  # type: ignore
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
        print(f"❌ Erro no streaming GPU: {e}")
        return None, elapsed


if __name__ == "__main__":
    FILE_TYPE = "csv"
    path = Path(f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}")

    print("=" * 70)
    print("TESTE DE PERFORMANCE: POLARS GPU ENGINE")
    print("=" * 70)

    print("\n🔍 Verificando disponibilidade da GPU...")
    gpu_available = check_gpu_availability()

    if gpu_available:
        print("✅ GPU Engine disponível!\n")
    else:
        print("⚠️  GPU Engine não disponível. Instale com:")
        print("   pip install polars-gpu-cu12  # Para CUDA 12")
        print("   ou")
        print("   pip install polars-gpu-cu11  # Para CUDA 11\n")

    print("\n1️⃣  EXECUÇÃO CPU (baseline)")
    print("-" * 70)
    df_cpu, time_cpu = test_cpu_execution(path.as_posix())
    print(f"⏱️  Tempo: {time_cpu:.3f}s")
    print(f"📊 Resultado:\n{df_cpu}\n")

    if gpu_available:
        print("\n2️⃣  EXECUÇÃO GPU (engine='gpu')")
        print("-" * 70)
        df_gpu, time_gpu = test_gpu_execution(path.as_posix())
        if df_gpu is not None:
            print(f"⏱️  Tempo: {time_gpu:.3f}s")
            print(f"🚀 Speedup: {time_cpu / time_gpu:.2f}x mais rápido")
            print(f"📊 Resultado:\n{df_gpu}\n")

        print("\n3️⃣  EXECUÇÃO GPU + STREAMING (datasets > VRAM)")
        print("-" * 70)
        df_streaming, time_streaming = test_streaming_gpu(path.as_posix())
        if df_streaming is not None:
            print(f"⏱️  Tempo: {time_streaming:.3f}s")
            print(f"📊 Primeiras linhas:\n{df_streaming.head()}\n")

    print("\n" + "=" * 70)
    print("📈 RESUMO DE PERFORMANCE")
    print("=" * 70)
    print(f"CPU:                    {time_cpu:.3f}s")

    if gpu_available and df_gpu is not None:
        print(
            f"GPU:                    {time_gpu:.3f}s ({time_cpu / time_gpu:.2f}x speedup)"
        )
        if df_streaming is not None:
            print(f"GPU + Streaming:        {time_streaming:.3f}s")

    print("\n💡 NOTAS IMPORTANTES:")
    print("-" * 70)
    print("• GPU engine está em Open Beta (desenvolvimento ativo)")
    print("• Nem todas operações são suportadas (fallback para CPU)")
    print("• Speedup é maior em datasets grandes e operações pesadas")
    print("• Streaming GPU permite processar dados > VRAM disponível")
    print("• Requer GPU NVIDIA com CUDA 11.2+ ou CUDA 12+")

    print("\n📚 INSTALAÇÃO:")
    print("-" * 70)
    print("# Para CUDA 12.x")
    print("pip install polars-gpu-cu12")
    print("\n# Para CUDA 11.x")
    print("pip install polars-gpu-cu11")
    print("\n# Verificar versão CUDA:")
    print("nvidia-smi")
