# Spark Unified Catalog

A Spark session catalog that routes table operations to Delta Lake, Apache
Iceberg, or Apache Hudi automatically, so queries work across mixed formats
without format-specific catalog prefixes.

`UnifiedSparkCatalog` wraps the session catalog, detects each table's format
from its metadata, and delegates to the matching format catalog. The approach is
inspired by Trino's table redirection.

## Features

- **Delta Lake, Iceberg, Hudi, and Hive** tables through one catalog.
- **Automatic format detection** from table metadata, with path-based inference
  and fallback across catalogs.
- **Lazy catalog initialization** and reuse of existing catalogs.
- **Spark 3.4 and 3.5** support; AWS S3 with IAM credentials; OpenLineage compatible.

## How it works

1. Load table metadata from the session catalog.
2. Identify the format with `TableTypeDetector`.
3. Route the operation to the Delta, Iceberg, or Hudi catalog.
4. If the table isn't found in the primary catalog, fall back to the others.

```
┌─────────────────────────────────────────┐
│            UnifiedSparkCatalog           │
├─────────────────────────────────────────┤
│   Delta      Iceberg      Hudi           │
│   Catalog    Catalog      Catalog        │
│                                          │
│   Session Catalog (Hive Metastore)       │
└─────────────────────────────────────────┘
```

## Requirements

- Java 17+
- Apache Spark 3.4 or 3.5
- Maven 3.6+

## Build

```bash
git clone https://github.com/grab/unified-spark-catalog.git
cd unified-spark-catalog

mvn clean package              # Spark 3.5 (default)
mvn clean package -Pspark-3.4  # Spark 3.4
```

## Dependency

The artifactId carries the Spark version (Spark ecosystem convention):

```xml
<dependency>
  <groupId>com.grab</groupId>
  <artifactId>unified-spark-catalog-3.5_2.12</artifactId>  <!-- or unified-spark-catalog-3.4_2.12 -->
  <version>1.3.1</version>
</dependency>
```

## Usage

Configure the session to use the catalog:

```scala
val spark = SparkSession.builder()
  .config("spark.sql.catalog.spark_catalog", "com.grab.UnifiedSparkCatalog")
  .config("spark.sql.extensions",
    "io.delta.sql.DeltaSparkSessionExtension," +
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()
```

Then query any format without a prefix:

```sql
CREATE TABLE delta_table   (id INT, name STRING) USING DELTA   LOCATION 's3://bucket/delta-table';
CREATE TABLE iceberg_table (id INT, name STRING) USING ICEBERG LOCATION 's3://bucket/iceberg-table';

SELECT * FROM delta_table JOIN iceberg_table USING (id);
```

The Delta, Iceberg, and Hudi catalogs activate automatically when their JARs are
on the classpath; no enable flags are needed.

### AWS S3

```properties
spark.hadoop.fs.s3a.access.key=...
spark.hadoop.fs.s3a.secret.key=...
spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Test

```bash
mvn test                          # Spark 3.5
mvn test -Pspark-3.4              # Spark 3.4
mvn test -Dtest=UnifiedSparkCatalogTest
```

## Compatibility

| Spark | Status |
|-------|--------|
| 3.4.x | Supported |
| 3.5.x | Supported (default) |
