from pathlib import Path
import duckdb
import time

FILE_TYPE = "csv"

path_obj = Path(f"data/fake_{FILE_TYPE}s/*.{FILE_TYPE}")

start_time = time.time()
con = duckdb.connect()

# Carregar dados UMA VEZ em memória
con.execute(f"""
COPY (SELECT * FROM '{path_obj}') TO 'people.parquet' (FORMAT PARQUET);
CREATE VIEW people AS SELECT * FROM 'people.parquet';
""")
print(f"Database initialization time: {time.time() - start_time:.2f} seconds\n")

# Count total rows
start_time = time.time()
total_rows = con.execute("SELECT COUNT(*) FROM people").fetchone()
print(f"Total rows across all CSVs: {total_rows}")
print(f"Count operation time: {time.time() - start_time:.2f} seconds\n")

# Describe
start_time = time.time()
print("Schema of the 'people' table:")
print(con.execute("DESCRIBE people").fetchdf())
print(f"Schema description time: {time.time() - start_time:.2f} seconds\n")

# Aggregate data
start_time = time.time()
result = con.execute("""
SELECT name, AVG(age) AS avg_age
FROM people
GROUP BY name
HAVING AVG(age) > 30
ORDER BY avg_age DESC
LIMIT 5
""")

print("Top 5 names with average age > 30:")
print(result.fetchdf())
print(f"Aggregation operation time: {time.time() - start_time:.2f} seconds\n")

# Filter and export
start_time = time.time()
con.execute("""
COPY (
    SELECT name, email, age
    FROM people
    WHERE age >= 50
) TO 'filtered_output.csv' (HEADER, DELIMITER ',')
""")
print("Filtered data exported to 'filtered_output.csv'.")
print(f"Export operation time: {time.time() - start_time:.2f} seconds")

con.close()
