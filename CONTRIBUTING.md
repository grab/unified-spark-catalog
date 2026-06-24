# Contributing to spark-catalog

Thanks for your interest in contributing! This document describes how to build
the project, run the tests, and submit a change.

## Prerequisites

- Java 17+
- Apache Maven 3.6+
- Apache Spark 3.4 or 3.5 (provided at runtime)

## Building

```bash
# Default build (Spark 3.5)
mvn clean package

# Spark 3.4
mvn clean package -Pspark-3.4
```

## Running tests

```bash
# Default (Spark 3.5)
mvn test

# Spark 3.4
mvn test -Pspark-3.4

# A single test class
mvn test -Dtest=UnifiedSessionCatalogTest
```

## Submitting changes

1. Fork the repository and create a feature branch off `main`.
2. Make your change. Keep commits focused and include tests where it makes
   sense.
3. Run the full test suite locally before opening a pull request.
4. Open a pull request against `main` with a clear description of the change
   and the motivation.
5. CI runs the build and tests for Spark 3.4 and 3.5 (see
   `.github/workflows/build-and-test.yml`); your PR must be green before it
   can be merged.

## Reporting issues

Please file bugs and feature requests via GitHub Issues. Include:

- A minimal reproduction (Spark version, table format, configuration).
- The behaviour you observed vs. what you expected.
- Relevant logs or stack traces.

## Code of Conduct

This project adheres to the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating you agree to uphold its terms.
