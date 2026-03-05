# Spark Unified Catalog

A unified Spark catalog implementation that provides seamless access to multiple table formats (Delta Lake, Apache Iceberg, Apache Hudi) through a single interface, eliminating the need for users to specify format-specific catalog prefixes.

## Overview

The `UnifiedSessionCatalog` acts as an intelligent wrapper that automatically routes table operations to the appropriate underlying catalog based on table format detection. This approach is inspired by Trino's table redirection feature and enables users to work with mixed table formats in a single Spark session without format-specific syntax.

## Key Features

### 🔄 **Unified Table Format Support**
- **Delta Lake**: Full support for Delta tables with ACID transactions
- **Apache Iceberg**: Support for Iceberg tables with time travel capabilities
- **Apache Hudi**: Support for Hudi tables with incremental processing
- **Hive Tables**: Backward compatibility with existing Hive metastore tables

### 🧠 **Intelligent Format Detection**
- Automatic table format detection based on metadata
- Path-based format inference for direct table access
- Fallback mechanisms for robust table operations

### ⚡ **Performance Optimizations**
- Lazy catalog initialization to reduce startup overhead
- Existing catalog reuse for better plugin compatibility

### 🔧 **Enterprise Features**
- AWS S3 integration with proper IAM credential handling
- Support for multiple Spark versions (3.3, 3.4, 3.5)
- OpenLineage compatibility for data lineage tracking

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                  UnifiedSessionCatalog                      │
├─────────────────────────────────────────────────────────────┤
│  ┌─────────────┐  ┌──────────────┐  ┌─────────────┐        │
│  │ Delta       │  │ Iceberg      │  │ Hudi        │        │
│  │ Catalog     │  │ Catalog      │  │ Catalog     │        │
│  └─────────────┘  └──────────────┘  └─────────────┘        │
│                                                             │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │           Session Catalog (Hive Metastore)              │ │
│  └─────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────┘
```

### How It Works

1. **Table Detection**: When a table operation is requested, the catalog first attempts to load metadata from the session catalog
2. **Format Identification**: Uses `TableTypeDetector` to identify the table format based on properties and metadata
3. **Operation Routing**: Routes the operation to the appropriate specialized catalog (Delta, Iceberg, or Hudi)
4. **Fallback Handling**: If a table is not found in the primary catalog, attempts to locate it in other catalogs

## Installation

### Prerequisites

- Java 11 or higher
- Apache Spark 3.3, 3.4, or 3.5
- Maven 3.6+

### Building from Source

```bash
# Clone the repository
git clone https://github.com/grab/spark-catalog.git
cd spark-catalog

# Build for default Spark version (3.5)
mvn clean package

# Build for specific Spark version
mvn clean package -Pspark-3.3  # For Spark 3.3
mvn clean package -Pspark-3.4  # For Spark 3.4
mvn clean package -Pspark-3.5  # For Spark 3.5
```

### Maven Dependency

Add to your `pom.xml`:

```xml
<dependency>
  <groupId>com.grab</groupId>
  <artifactId>spark-catalog</artifactId>
  <version>1.3.1</version>
</dependency>
```

## Usage

### Spark Configuration

Configure your Spark session to use the unified catalog:

```scala
val spark = SparkSession.builder()
  .appName("UnifiedCatalogExample")
  .config("spark.sql.catalog.spark_catalog", "com.grab.UnifiedSessionCatalog")
  .config("spark.sql.extensions",
    "io.delta.sql.DeltaSparkSessionExtension," +
    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
  .getOrCreate()
```

### Working with Mixed Table Formats

```sql
-- Create tables in different formats
CREATE TABLE delta_table (id INT, name STRING) USING DELTA LOCATION 's3://bucket/delta-table'
CREATE TABLE iceberg_table (id INT, name STRING) USING ICEBERG LOCATION 's3://bucket/iceberg-table'
CREATE TABLE hudi_table (id INT, name STRING) USING HUDI LOCATION 's3://bucket/hudi-table'

-- Query any table without format-specific prefixes
SELECT * FROM delta_table;
SELECT * FROM iceberg_table;
SELECT * FROM hudi_table;

-- Join across different formats seamlessly
SELECT d.id, i.name, h.timestamp
FROM delta_table d
JOIN iceberg_table i ON d.id = i.id
JOIN hudi_table h ON d.id = h.id;
```

## Configuration Options

### AWS S3 Integration

```properties
# S3 Configuration
spark.hadoop.fs.s3a.access.key=your-access-key
spark.hadoop.fs.s3a.secret.key=your-secret-key
spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com
spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

### Catalog-Specific Settings

```properties
# Delta Lake
spark.sql.catalog.spark_catalog.delta.enabled=true

# Iceberg
spark.sql.catalog.spark_catalog.iceberg.enabled=true

# Hudi
spark.sql.catalog.spark_catalog.hudi.enabled=true
```

## Testing

### Running Tests

```bash
# Run all tests for default Spark version (3.5)
mvn test

# Run tests for specific Spark version
mvn test -Pspark-3.3
mvn test -Pspark-3.4
mvn test -Pspark-3.5

# Run specific test suites
mvn test -Dtest=UnifiedSessionCatalogTest
mvn test -Dtest=UnifiedSessionCatalogBuiltInFunctionTest
mvn test -Dtest=UnifiedSessionCatalogNonDefaultTest
```

### Test Structure

- **Unit Tests**: Core catalog functionality and table type detection
- **Integration Tests**: End-to-end scenarios with real table formats

## Release Information

**Current Version**: 1.3.1

**Key Features in This Release**:
- ✅ **Unified Catalog**: Support for Delta Lake, Apache Iceberg, and Apache Hudi
- ✅ **Format Detection**: Automatic table format detection and routing
- ✅ **Multi-Spark Version Support**: Compatible with Spark 3.3, 3.4, and 3.5
- ✅ **Production Ready**: Thoroughly tested with enterprise table formats

## Development

### Project Structure

```
src/
├── main/
│   ├── java/com/grab/
│   │   ├── UnifiedSessionCatalog.java       # Main unified catalog implementation
│   │   └── TableTypeDetector.java           # Table format detection logic
│   └── resources/
│       └── META-INF/services/
│           └── org.apache.spark.sql.SparkSessionExtensions
└── test/
    ├── java/com/grab/
    │   ├── UnifiedSessionCatalogTest.java
    │   ├── UnifiedSessionCatalogNonDefaultTest.java
    │   └── UnifiedSessionCatalogBuiltInFunctionTest.java
    └── resources/
        └── java.security
```

## Compatibility

| Spark Version | Status | Notes |
|--------------|--------|-------|
| 3.3.x | ✅ Supported | Stable |
| 3.4.x | ✅ Supported | Stable |
| 3.5.x | ✅ Supported | Default, Latest features |

