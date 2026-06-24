# Contributing

## Prerequisites

- Java 17+
- Apache Maven 3.6+
- Apache Spark 3.4 or 3.5 (provided at runtime)

## Build and test

```bash
mvn clean package              # Spark 3.5 (default)
mvn clean package -Pspark-3.4  # Spark 3.4
mvn test
mvn test -Dtest=UnifiedSparkCatalogTest
```

## Submitting changes

1. Fork and branch off `main`.
2. Keep commits focused; add tests where it makes sense.
3. Run the tests locally before opening a PR.
4. Open a PR against `main` describing the change and motivation.

CI builds and tests Spark 3.4 and 3.5 (`.github/workflows/build-and-test.yml`);
the PR must be green to merge.

## Reporting issues

File bugs and feature requests via GitHub Issues. Include a minimal reproduction
(Spark version, table format, configuration), expected vs. actual behaviour, and
relevant logs.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md).
