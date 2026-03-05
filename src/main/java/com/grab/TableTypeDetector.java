package com.grab;

import java.util.Map;

import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.Identifier;

/**
 * Helper class to detect table types based on their properties.
 */
public class TableTypeDetector {
    private static final String SPARK_TABLE_PROVIDER_KEY = "spark.sql.sources.provider";
    private static final String SPARK_TABLE_PROVIDER_KEY_ALTERNATE = "provider";
    private static final String DELTA_LAKE_PROVIDER = "delta";
    private static final String ICEBERG_TABLE_TYPE_NAME = "table_type";
    private static final String ICEBERG_TABLE_TYPE_VALUE = "iceberg";
    private static final String HUDI_PARQUET_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieParquetInputFormat";
    private static final String HUDI_PARQUET_REALTIME_INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieParquetRealtimeInputFormat";
    private static final String HUDI_INPUT_FORMAT = "org.apache.hudi.hadoop.HoodieInputFormat";
    private static final String HUDI_REALTIME_INPUT_FORMAT = "org.apache.hudi.hadoop.realtime.HoodieRealtimeInputFormat";
    private static final String HUDI_PROVIDER = "hudi";
    public static boolean isDeltaLakeTable(Table table) {
        return isDeltaLakeTable(table.properties());
    }

    public static boolean isDeltaLakeTable(Map<String, String> tableParameters) {
        return DELTA_LAKE_PROVIDER.equalsIgnoreCase(tableParameters.getOrDefault(SPARK_TABLE_PROVIDER_KEY, "")) ||
        DELTA_LAKE_PROVIDER.equalsIgnoreCase(tableParameters.getOrDefault(SPARK_TABLE_PROVIDER_KEY_ALTERNATE, ""));
    }

    private static boolean isDeltaNamespace(String[] namespace) {
        return namespace.length == 1 && namespace[0].toLowerCase().equals("delta");
    }

    private static boolean isSupportedStoragePath(String path) {
        String[] supportedPrefixes = {"s3://", "s3a://", "file:/", "s3n://", "abfs://", "abfss://"};
        for (String prefix : supportedPrefixes) {
            if (path.contains(prefix)) {
                return true;
            }
        }
        return false;
    }

    public static boolean isDeltaLakePath(Identifier ident) {
        return isDeltaNamespace(ident.namespace()) && isSupportedStoragePath(ident.name());
    }

    public static boolean isIcebergTable(Table table) {
        return isIcebergTable(table.properties());
    }

    public static boolean isIcebergTable(Map<String, String> tableParameters) {
        return ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableParameters.getOrDefault(ICEBERG_TABLE_TYPE_NAME, "")) ||
        ICEBERG_TABLE_TYPE_VALUE.equalsIgnoreCase(tableParameters.getOrDefault(SPARK_TABLE_PROVIDER_KEY_ALTERNATE, ""));
    }

    public static boolean isHudiTable(Table table) {
        return isHudiTable(table.properties().getOrDefault(SPARK_TABLE_PROVIDER_KEY_ALTERNATE, ""));
    }

    public static boolean isHudiTable(String provider) {
        return provider.equals(HUDI_PROVIDER);
    }
} 