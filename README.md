# Comparação de Performance: DuckDB vs. Polars

Este projeto tem como objetivo principal comparar a performance do DuckDB e do Polars em tarefas comuns de processamento e análise de dados. Ele utiliza um conjunto de scripts para gerar dados de teste, realizar operações de ETL (Extração, Transformação e Carga) e medir os tempos de execução para diferentes abordagens.

## Estrutura de Pastas

O projeto está organizado da seguinte forma:

-   `src/`: Contém todos os scripts Python, incluindo o CLI unificado.
-   `data/`: Armazena os datasets, incluindo o banco de dados SQLite (`example.db`) e os dados falsos gerados (`fake_csvs`, `fake_jsons`, `parquets`).

## CLI Unificado

Este projeto agora conta com uma interface de linha de comando (CLI) unificada, desenvolvida com `Typer` e `Rich`, que consolida todas as funcionalidades de geração, conversão, ingestão e processamento de dados. Isso proporciona uma experiência mais interativa e organizada para o usuário.

### Comandos Disponíveis

Você pode ver todos os comandos e opções disponíveis executando:

```bash
python src/cli.py --help
```

#### 1. `generate` - Gerar Dados Falsos

Gera arquivos CSV ou JSON com dados falsos para testes.

-   **Gerar 50 arquivos CSV com 200 linhas cada:**
    ```bash
    python src/cli.py generate --file-type csv --num-files 50 --rows-per-file 200
    ```
-   **Gerar 30 arquivos JSON com 150 linhas cada:**
    ```bash
    python src/cli.py generate --file-type json --num-files 30 --rows-per-file 150
    ```

#### 2. `convert` - Converter Dados

Converte dados entre diferentes formatos. Atualmente, suporta a conversão incremental de CSV para Parquet.

-   **Converter novos arquivos CSV para Parquet:**
    ```bash
    python src/cli.py convert --from csv --to parquet
    ```

#### 3. `ingest` - Ingerir Dados em Banco de Dados

Ingere dados de arquivos CSV ou JSON em um banco de dados. Atualmente, suporta SQLite.

-   **Ingerir dados CSV em uma tabela SQLite `my_csv_data`:**
    ```bash
    python src/cli.py ingest --file-type csv --engine sqlite --table-name my_csv_data
    ```
-   **Ingerir dados JSON em uma tabela SQLite `my_json_data`:**
    ```bash
    python src/cli.py ingest --file-type json --engine sqlite --table-name my_json_data
    ```

#### 4. `process` - Processar Dados

Realiza operações de processamento de dados utilizando diferentes engines (Polars, DuckDB).

-   **Processamento básico com Polars (CSV):**
    ```bash
    python src/cli.py process --file-type csv --engine polars-basic
    ```
-   **Processamento básico com Polars (JSON):**
    ```bash
    python src/cli.py process --file-type json --engine polars-basic
    ```
-   **Processamento com DuckDB (CSV):**
    ```bash
    python src/cli.py process --file-type csv --engine duckdb
    ```
-   **Processamento com DuckDB (JSON):**
    ```bash
    python src/cli.py process --file-type json --engine duckdb
    ```
-   **Processamento com Polars Streaming (CSV):**
    ```bash
    python src/cli.py process --file-type csv --engine polars-streaming
    ```
-   **Processamento com Polars GPU (CSV):**
    ```bash
    python src/cli.py process --file-type csv --engine polars-gpu
    ```
    *(Nota: Para processamento GPU, certifique-se de ter as dependências Polars GPU necessárias instaladas, por exemplo, `pip install polars-gpu-cu12` para CUDA 12.)*

## Como Começar

### Pré-requisitos

-   Python 3.11+
-   [uv](https://github.com/astral-sh/uv) instalado.

### Instalação

1.  Clone o repositório.
2.  Instale as dependências utilizando `uv` e o arquivo `pyproject.toml`:

    ```bash
    uv sync
    ```

### Executando as Funcionalidades

Todas as funcionalidades agora são acessíveis através do CLI unificado. Execute os comandos conforme a seção "CLI Unificado" acima.

## Dependências

Este projeto utiliza as seguintes dependências:

-   `duckdb>=1.4.0`
-   `faker>=37.8.0`
-   `pandas>=2.3.3`
-   `polars>=1.33.1`
-   `pyarrow>=21.0.0`
-   `sqlalchemy>=2.0.43`
-   `typer>=0.19.2`
-   `rich>=14.1.0`