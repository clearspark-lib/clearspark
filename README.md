<img src="https://raw.githubusercontent.com/clearspark-lib/clearspark/main/assets/images/readme-logo.png" style="transform: rotate(90deg);"  width="100%"/>


# ⭐ clearspark

**clearspark** is a lightweight PySpark utility library that makes common data transformation patterns cleaner, faster to write, and easier to read. Stop rewriting the same boilerplate `when/otherwise` chains — clearspark gives you expressive, validated, one-liner functions for bucketing, categorizing, loading, and saving data.

---

## Installation
```bash
pip install clearspark
```

---

## Importing
```python
from clearspark.functions import with_buckets, with_categories, load_data, save_data
```

---

## Why clearspark?

Working with PySpark day-to-day often means writing the same repetitive patterns over and over: nested `when().when().otherwise()` chains for bucketing numeric columns, `isin()` chains for categorizing string values, and verbose boilerplate just to load or save a table with a filter. clearspark wraps all of that into clean, validated functions so you can focus on the logic, not the syntax.

---

## Functions

### `load_data` — Load a table or file path into a DataFrame
### `with_buckets` — Classify numeric values into labeled ranges
### `with_categories` — Map values into broader groups using a dictionary
### `save_data` — Save a DataFrame to a catalog table or file path

---

## Full Example: Raw Spark vs. clearspark

Imagine you're building a sales report. You need to:
1. Load the `gold.sales` table, selecting only relevant columns and filtering active records
2. Classify the `revenue` column into ranges (buckets)
3. Map the `industry` column to broader sector categories
4. Save the result back as a partitioned Delta table

---

### 🔴 Raw PySpark (the hard way)
```python
from pyspark.sql.functions import when, col, lit

# --- 1. Load data ---
df = spark.read.format("delta").table("gold.sales")
df = df.select("id", "company", "industry", "revenue", "status")
df = df.filter(col("status") == "active")

# --- 2. Bucket revenue ---
df = df.withColumn(
    "revenue_range",
    when(col("revenue").isNull(), lit("00. no info"))
    .when(col("revenue") < 100,   lit("01. <100"))
    .when((col("revenue") >= 100) & (col("revenue") <= 500),  lit("02. 100-500"))
    .when((col("revenue") >= 500) & (col("revenue") <= 1000), lit("03. 500-1000"))
    .otherwise(lit("04. >1000"))
)

# --- 3. Categorize industry ---
df = df.withColumn(
    "sector",
    when(col("industry").isNull(), lit("no info"))
    .when(col("industry").isin(["bank", "fintech", "insurance"]), lit("Finance"))
    .when(col("industry").isin(["saas", "cloud", "hardware"]),    lit("Tech"))
    .otherwise(lit("uncategorized"))
)

# --- 4. Save data ---
df.write.format("delta") \
    .mode("overwrite") \
    .partitionBy("sector") \
    .saveAsTable("gold.sales_report")
```

**Problems with this approach:**
- Every new bucket or category requires manually editing chains
- No validation — typos in column names silently produce wrong results
- Bucket ordering logic (zero-padded labels) is your responsibility
- Save logic is verbose and easy to misconfigure
- Hard to read, hard to review, hard to reuse

---

### 🟢 clearspark (the clean way)
```python
from clearspark.functions import load_data, with_buckets, with_categories, save_data

# --- 1. Load data ---
df = load_data(
    data_path="gold.sales",
    spark_session=spark,
    select_columns=["id", "company", "industry", "revenue", "status"],
    filter_spec="status = 'active'"
)

# --- 2. Bucket revenue ---
df = with_buckets(
    spark_df=df,
    value_column_name="revenue",
    bucket_column_name="revenue_range",
    buckets=[100, 500, 1000]
)

# --- 3. Categorize industry ---
mapping = {
    "Finance": ["bank", "fintech", "insurance"],
    "Tech":    ["saas", "cloud", "hardware"]
}

df = with_categories(
    spark_df=df,
    value_column_name="industry",
    category_column_name="sector",
    mapping=mapping
)

# --- 4. Save data ---
save_data(
    df=df,
    data_path="gold.sales_report",
    mode="overwrite",
    partition_by=["sector"]
)
```

**Result is identical — but now:**
- Adding a new bucket is just adding a number to the list
- Adding a new category is just adding a key to the dictionary
- Column names are validated before execution
- Null handling and label formatting are automatic
- Save parameters are validated before writing — no silent misconfiguration
- The code reads like documentation

---

## Output Preview

| id | company     | industry  | revenue | status | revenue_range  | sector        |
|----|-------------|-----------|---------|--------|----------------|---------------|
| 1  | Acme Corp   | bank      | 850.0   | active | 03. 500-1000   | Finance       |
| 2  | SkyCo       | saas      | 50.0    | active | 01. <100       | Tech          |
| 3  | DataFirm    | logistics | 1200.0  | active | 04. >1000      | uncategorized |
| 4  | NullVenture | null      | null    | active | 00. no info    | no info       |

---

## Function Reference

### `load_data(data_path, spark_session, select_columns=None, filter_spec=None, data_format="delta")`

Loads data from a catalog table or file path with optional column selection and filtering.
```python
# Load a catalog table
df = load_data("gold.sales", spark)

# Load a file path with filter
df = load_data(
    data_path="s3://my-bucket/data/",
    spark_session=spark,
    select_columns=["id", "amount"],
    filter_spec="amount > 100",
    data_format="parquet"
)
```

---

### `with_buckets(spark_df, value_column_name, bucket_column_name, buckets)`

Classifies numeric values into labeled ranges. Buckets don't need to be sorted — clearspark handles that. Labels are automatically zero-padded for correct sort order in charts and reports.
```python
df = with_buckets(df, "age", "age_group", [18, 35, 60])

# Resulting labels:
# "00. no info"  → null
# "01. <18"      → age < 18
# "02. 18-35"    → 18 <= age <= 35
# "03. 35-60"    → 35 <= age <= 60
# "04. >60"      → age > 60
```

---

### `with_categories(spark_df, value_column_name, category_column_name, mapping)`

Maps values into broader group labels using a dictionary. Unmatched values become `"uncategorized"`, nulls become `"no info"`.
```python
mapping = {
    "LATAM": ["brazil", "mexico", "argentina"],
    "EMEA":  ["france", "germany", "uk"]
}

df = with_categories(df, "country", "region", mapping)
```

---

### `save_data(df, data_path, data_format="delta", mode="overwrite", partition_by=None)`

Saves a DataFrame to a catalog table or file path. Abstracts the destination type the same way `load_data` abstracts the source — if the path contains no `"/"`, it writes to a catalog table; otherwise it writes to a file path.
```python
# Save to a catalog table
save_data(df, "gold.sales_report")

# Save to a file path with partitioning
save_data(
    df=df,
    data_path="s3://my-bucket/output/sales_report/",
    data_format="parquet",
    mode="append",
    partition_by=["year", "month"]
)
```

Accepted values for `mode`: `"overwrite"`, `"append"`, `"ignore"`, `"error"`.

---

## Requirements

- Python >= 3.8
- PySpark >= 3.0

---

## License

MIT
