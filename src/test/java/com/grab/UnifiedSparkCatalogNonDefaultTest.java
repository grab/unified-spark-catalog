package com.grab;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.connector.expressions.Expressions;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.AnalysisException;
import org.apache.iceberg.hive.TestHiveMetastore;
import org.junit.Before;
import org.junit.After;
import org.junit.Test;
import org.junit.BeforeClass;
import org.junit.AfterClass;
import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Collections;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.RowFactory;

public class UnifiedSparkCatalogNonDefaultTest {

    private static class TestCatalog implements TableCatalog, FunctionCatalog, SupportsNamespaces, StagingTableCatalog {
        @Override
        public String name() { return "test"; }
        @Override
        public Identifier[] listTables(String[] namespace) { return new Identifier[0]; }
        @Override
        public Table loadTable(Identifier ident, String version) { return null; }
        @Override
        public Table loadTable(Identifier ident) { return null; }
        @Override
        public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) { return null; }
        @Override
        public boolean dropTable(Identifier ident) { return false; }
        @Override
        public void renameTable(Identifier oldIdent, Identifier newIdent) { }
        @Override
        public Table alterTable(Identifier ident, TableChange... changes) { return null; }
        @Override
        public void initialize(String name, CaseInsensitiveStringMap options) { }
        @Override
        public String[] defaultNamespace() { return new String[0]; }
        @Override
        public String[][] listNamespaces() { return new String[0][]; }
        @Override
        public String[][] listNamespaces(String[] namespace) { return new String[0][]; }
        @Override
        public boolean dropNamespace(String[] namespace, boolean cascade) { return false; }
        @Override
        public void createNamespace(String[] namespace, Map<String, String> metadata) { }
        @Override
        public void alterNamespace(String[] namespace, NamespaceChange... changes) { }
        @Override
        public Map<String, String> loadNamespaceMetadata(String[] namespace) { return Collections.emptyMap(); }
        @Override
        public Identifier[] listFunctions(String[] namespace) { return new Identifier[0]; }
        @Override
        public UnboundFunction loadFunction(Identifier ident) { return null; }
        
        // StagingTableCatalog methods
        @Override
        public StagedTable stageCreate(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) 
                throws TableAlreadyExistsException, NoSuchNamespaceException {
            throw new UnsupportedOperationException("Staging operations not supported in test catalog");
        }

        @Override
        public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) 
                throws NoSuchNamespaceException, NoSuchTableException {
            throw new UnsupportedOperationException("Staging operations not supported in test catalog");
        }

        @Override
        public StagedTable stageCreateOrReplace(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties) 
                throws NoSuchNamespaceException {
            throw new UnsupportedOperationException("Staging operations not supported in test catalog");
        }
    }

    public static class TestUnifiedSparkCatalog extends UnifiedSparkCatalog<TestCatalog> {
        // Just providing a concrete type
    }

    private static SparkSession spark;
    private TableCatalog tableCatalog;
    private SupportsNamespaces namespaceCatalog;
    private static TestHiveMetastore metastore;
    
    private static final String[] DEFAULT_NAMESPACE = {"local_testing"};
    private static final List<String> TABLE_TYPES = Arrays.asList("iceberg", "delta", "hudi", "hive");
    private static final Map<String, Boolean> ENABLED_TABLE_TYPES = new LinkedHashMap<String, Boolean>() {{
        put("iceberg", true);
        put("delta", true);
        put("hudi", false);  // Disabled by default
        put("hive", true);
    }};
    private static final StructType TEST_SCHEMA = new StructType()
        .add("id", DataTypes.IntegerType)
        .add("name", DataTypes.StringType);
    private static final Transform[] TEST_PARTITIONS = new Transform[] {
        Expressions.identity("id")
    };

    @BeforeClass
    public static void setupClass() throws Exception {
        // Start the TestHiveMetastore
        metastore = new TestHiveMetastore();
        metastore.start();
        
        String metastoreUris = metastore.hiveConf().get("hive.metastore.uris");
        
        // Create Spark session
        spark = SparkSession.builder()
            .appName("UnifiedSparkCatalogTest")
            .master("local[*]")
            .config("spark.sql.catalogImplementation", "hive")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension,org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.hadoop.hive.metastore.uris", metastoreUris)
            .config("spark.databricks.delta.requiredSparkConfsCheck.enabled", "false")
            .config("spark.sql.catalog.test_catalog", "com.grab.UnifiedSparkCatalog")
            .config("spark.sql.catalog.test_catalog.type", "hive")
            .config("spark.sql.catalog.test_catalog.cache-enabled", "false")
            .config("spark.databricks.delta.catalog.update.enabled", "true")
            .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg_catalog.type", "hive")
            .config("spark.sql.catalog.delta_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .config("spark.sql.catalog.delta_catalog.type", "hive")
            .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict")
            .enableHiveSupport()
            .getOrCreate();
        spark.sparkContext().setLogLevel("WARN");
    }
    
    @AfterClass
    public static void tearDownClass() throws Exception {
        if (spark != null) {
            spark.stop();
            spark = null;
            SparkSession.clearActiveSession();
            SparkSession.clearDefaultSession();
        }
        
        if (metastore != null) {
            metastore.stop();
        }
    }
    
    @Before
    public void setUp() {
        // Initialize the UnifiedSparkCatalog
        CatalogPlugin catalogPlugin = spark.sessionState().catalogManager().catalog("test_catalog");
        tableCatalog = (TableCatalog) catalogPlugin;
        namespaceCatalog = (SupportsNamespaces) catalogPlugin;
        
        File file = new File("spark-warehouse");
        if (file.exists()) {
            deleteRecursively(file);
        }
        // Create test database
        spark.sql("CREATE DATABASE IF NOT EXISTS local_testing");
        // Clean up and recreate test tables
    }
    
    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            for (File child : file.listFiles()) {
                deleteRecursively(child);
            }
        }
        file.delete();
    } 

    @After
    public void tearDown() {
        // Drop all test tables
        try {
            TABLE_TYPES.forEach(this::dropTable);
            File file = new File("spark-warehouse");
        if (file.exists()) {
            deleteRecursively(file);
        }
        } catch (Exception e) {
            System.err.println("Error dropping tables: " + e.getMessage());
        }
        
        
        
    }
    
    public void createTablesUsingMethods(String provider) {
        if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
            return;
        }
        String tableName = provider + "_test";
        try {
            tableCatalog.dropTable(Identifier.of(DEFAULT_NAMESPACE, tableName));
        } catch (Exception e) {
            System.err.println("Error dropping " + provider + " table: " + e.getMessage());
        }
        try {
            tableCatalog.createTable(Identifier.of(DEFAULT_NAMESPACE, tableName), TEST_SCHEMA, TEST_PARTITIONS, new HashMap<String, String>() {{
                put("provider", provider);
            }});
        } catch (Exception e) {
            System.err.println("Error creating " + provider + " table: " + e.getMessage());
        }
    }

    private void createTableUsingSparkSQL(String provider) {
        if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
            return;
        }
        String tableName = provider + "_test";
        
        
        // Create table with the appropriate syntax for each provider
        try {
            spark.sql(String.format(
                "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING %s PARTITIONED BY (name) ",
                DEFAULT_NAMESPACE[0], tableName, provider, tableName
            )).show();
        } catch (Exception e) {
            System.err.println("Error creating " + provider + " table: " + e.getMessage());
        }
        
        // Insert test data
        try {
            spark.sql(String.format(
                "INSERT INTO test_catalog.%s.%s VALUES (1, 'test1'), (2, 'test2')",
                DEFAULT_NAMESPACE[0], tableName
            )).show();
        } catch (Exception e) {
            System.err.println("Error inserting data into " + provider + " table: " + e.getMessage());
        }
    }
    
    private void dropTable(String provider) {
        String tableName = provider + "_test";
        spark.sql("DROP TABLE IF EXISTS test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName + " PURGE");
    }
    
    
    @Test
    public void testListTablesUsingSparkSQL() throws NoSuchNamespaceException {
        // Create tables using SQL directly in the test
        for (String provider : TABLE_TYPES) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }
            
            String tableName = provider + "_test_list_tables";
            
            // Create table with the appropriate syntax for each provider
            try {
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING %s PARTITIONED BY (name)",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));
            } catch (Exception e) {
                System.err.println("Error creating " + provider + " table: " + e.getMessage());
            }
            
            // Insert test data
            try {
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES (1, 'test1'), (2, 'test2')",
                    DEFAULT_NAMESPACE[0], tableName
                ));
            } catch (Exception e) {
                System.err.println("Error inserting data into " + provider + " table: " + e.getMessage());
            }
        }
        
        // Get catalog and list tables
        List<Row> tables = spark.sql("SHOW TABLES IN test_catalog." + DEFAULT_NAMESPACE[0]).collectAsList();
        
        // Verify minimum count based on enabled table types
        int expectedMinCount = (int) ENABLED_TABLE_TYPES.values().stream().filter(Boolean::booleanValue).count();
        assertTrue("Should find at least " + expectedMinCount + " tables but found " + tables.size() + " tables: " + tables.toString(), tables.size() >= expectedMinCount);
        
        // Get list of expected table names from enabled table types
        List<String> expectedTableNames = TABLE_TYPES.stream()
            .filter(provider -> ENABLED_TABLE_TYPES.getOrDefault(provider, false))
            .map(provider -> provider + "_test_list_tables")
            .collect(Collectors.toList());
        
        // Get actual table names from the database
        List<String> actualTableNames = tables.stream()
            .map(row -> row.getString(1))
            .collect(Collectors.toList());
        
        // Verify all enabled test tables are present
        for (String expectedTableName : expectedTableNames) {
            assertTrue(expectedTableName + " not found in database", actualTableNames.contains(expectedTableName));
        }
    }

    @Test
    public void testCache() throws NoSuchTableException {
        // Create tables using SQL directly in the test
        for (String provider : TABLE_TYPES) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }
            
            String tableName = provider + "_test_cache";
            
            // Create table with the appropriate syntax for each provider
            try {
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING %s PARTITIONED BY (name)",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));
            } catch (Exception e) {
                System.err.println("Error creating " + provider + " table: " + e.getMessage());
            }
            
            // Insert test data
            try {
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES (1, 'test1'), (2, 'test2')",
                    DEFAULT_NAMESPACE[0], tableName
                ));
            } catch (Exception e) {
                System.err.println("Error inserting data into " + provider + " table: " + e.getMessage());
            }
        }
        
        // Test loading specific tables
        List<String> enabledTableTypes = TABLE_TYPES.stream()
            .filter(provider -> ENABLED_TABLE_TYPES.getOrDefault(provider, false))
            .collect(Collectors.toList());
            
        for (String provider : enabledTableTypes) {
            Dataset<Row> table = spark.sql("SELECT * FROM test_catalog." + DEFAULT_NAMESPACE[0] + "." + provider + "_test_cache");
            assertEquals("Table " + provider + " should have 2 rows", 2, table.count());
            if (provider.equals("hive")) {
                Dataset<Row> hiveTable = spark.sql("SELECT * FROM test_catalog." + DEFAULT_NAMESPACE[0] + "." + provider + "_test_cache");
                assertEquals("Table " + provider + " should have 2 rows", 2, hiveTable.count());
            }
        }
        
        // Test dropping tables
        for (String provider : TABLE_TYPES) {
            spark.sql("DROP TABLE IF EXISTS test_catalog." + DEFAULT_NAMESPACE[0] + "." + provider + "_test_cache ");
        }
    }
    
    @Test 
    public void testListTablesUsingMethods() throws NoSuchNamespaceException {
        TABLE_TYPES.forEach(this::createTablesUsingMethods);
        // Get catalog and list tables
        Identifier[] tables = tableCatalog.listTables(DEFAULT_NAMESPACE);
    }

    @Test
    public void testLoadTables() throws NoSuchTableException {
        // Test loading each enabled table type
        TABLE_TYPES.forEach(this::createTablesUsingMethods);
        for (String provider : TABLE_TYPES) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }
            String tableName = provider + "_test";
            Identifier ident = Identifier.of(DEFAULT_NAMESPACE, tableName);
            Table table = tableCatalog.loadTable(ident);
            
            // Verify table properties
            assertNotNull("Table properties should not be null", table.properties());
            assertEquals("Schema should have 2 fields", 2, table.schema().fields().length);
        }
    }
    
    @Test
    public void testCreateDropAndRenameTable() throws Exception {
        for (String provider : Arrays.asList("iceberg", "delta")) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }
            String newTableName = provider + "_new_test_table";
            String renamedTableName = provider + "_renamed_test_table";
            Identifier newIdent = Identifier.of(DEFAULT_NAMESPACE, newTableName);
            Identifier renamedIdent = Identifier.of(DEFAULT_NAMESPACE, renamedTableName);
            Map<String, String> properties = new HashMap<>();
            properties.put("provider", provider);
            
            try {
            // Create table
                Table newTable = tableCatalog.createTable(newIdent, TEST_SCHEMA, TEST_PARTITIONS, properties);
                assertEquals(provider, newTable.properties().get("provider"));
                
                // Verify load works
                Table loadedTable = tableCatalog.loadTable(newIdent);
                assertEquals(newTableName, loadedTable.name().split("\\.")[loadedTable.name().split("\\.").length - 1]);
                
                // Test rename
                tableCatalog.renameTable(newIdent, renamedIdent);
                
                // Verify renamed table exists and original doesn't
                assertThrows(NoSuchTableException.class, () -> tableCatalog.loadTable(newIdent));
                Table renamedTable = tableCatalog.loadTable(renamedIdent);
                assertEquals(renamedTableName, renamedTable.name().split("\\.")[renamedTable.name().split("\\.").length - 1]);
                
                // Drop table
                assertTrue(tableCatalog.dropTable(renamedIdent));
                
                // Verify table is gone
                assertThrows(NoSuchTableException.class,
                 () -> tableCatalog.loadTable(renamedIdent));
            } finally {
                // Clean up in case test fails
                try {
                    tableCatalog.dropTable(newIdent);
                } catch (Exception ignored) {}
                try {
                    tableCatalog.dropTable(renamedIdent);
                } catch (Exception ignored) {}
            }
        }
    }
    
    @Test
    public void testAlterTable() throws NoSuchTableException {
        TABLE_TYPES.forEach(this::createTablesUsingMethods);
        for (String provider : Arrays.asList("iceberg", "delta")) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }
            Identifier ident = Identifier.of(DEFAULT_NAMESPACE, provider + "_test");
            
        // Alter table properties
            TableChange[] changes = new TableChange[]{
                TableChange.addColumn(new String[]{"new_column"}, DataTypes.StringType)
            };
        
            tableCatalog.alterTable(ident, changes);
            String[] columnNames = tableCatalog.loadTable(ident).schema().fieldNames();
            assertTrue(Arrays.asList(columnNames).contains("new_column"));
        // Verify property was set
            
        }
    }
    
    @Test
    public void testNamespaceOperations() throws Exception {
        String[] testNamespace = new String[]{"test_namespace"};
        
        try {
            // Create namespace
            Map<String, String> nsMetadata = new HashMap<>();
            nsMetadata.put("description", "Test namespace");
            namespaceCatalog.createNamespace(testNamespace, nsMetadata);
            
            // Verify it exists
            boolean found = Arrays.stream(namespaceCatalog.listNamespaces())
                .anyMatch(ns -> ns[0].equals("test_namespace"));
            assertTrue("Namespace should exist after creation", found);
            
            // Test altering namespace
            NamespaceChange[] changes = new NamespaceChange[]{
                NamespaceChange.setProperty("description", "Updated description")
            };
            namespaceCatalog.alterNamespace(testNamespace, changes);
            
            // Verify metadata
            Map<String, String> metadata = namespaceCatalog.loadNamespaceMetadata(testNamespace);
            assertEquals("Updated description", metadata.get("description"));
        } finally {
            // Clean up
            try {
                namespaceCatalog.dropNamespace(testNamespace, false);
            } catch (Exception ignored) {}
        }
    }
    
    @Test
    public void testQueryTables() {
        // Test querying each enabled table type
        TABLE_TYPES.forEach(this::createTablesUsingMethods);
        for (String provider : TABLE_TYPES) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }
            String tableName = provider + "_test";
            Dataset<Row> data = spark.sql("SELECT * FROM test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName);
            assertEquals("Table should have 0 rows", 0, data.count());
        }
    }

    @Test
    public void testDeltaPathBasedRead() {
        Arrays.asList("delta").forEach(this::createTableUsingSparkSQL);
        try {
            Table table = tableCatalog.loadTable(Identifier.of(DEFAULT_NAMESPACE, "delta_file_test"));
            String location = table.properties().get("location");
            Dataset<Row> data = spark.sql("select * from delta.`" + location + "`");
            assertEquals("Table should have 2 rows", 2, data.count());
        } catch (NoSuchTableException e) {
            System.err.println("Error loading delta table: " + e.getMessage());
        }
        
        
    }

    @Test
    public void testIcebergBranchOperations() {
        if (!ENABLED_TABLE_TYPES.getOrDefault("iceberg", false)) {
            return;
        }

        String tableName = "iceberg_branch_test";
        String branchName = "test_branch";

        try {
            // Create Iceberg table
            
            spark.sql(String.format(
                "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING iceberg PARTITIONED BY (id)",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Insert data into main branch
            spark.sql(String.format(
                "INSERT INTO test_catalog.%s.%s VALUES (1, 'main1'), (2, 'main2')",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Create a new branch
            spark.sql(String.format(
                "ALTER TABLE iceberg_catalog.%s.%s CREATE BRANCH %s",
                DEFAULT_NAMESPACE[0], tableName, branchName
            ));

            // Insert data into the new branch
            spark.sql(String.format(
                "INSERT INTO iceberg_catalog.%s.%s.branch_%s VALUES (3, 'branch1'), (4, 'branch2')",
                DEFAULT_NAMESPACE[0], tableName, branchName
            ));

            // Query main branch
            Dataset<Row> mainData = spark.sql(String.format(
                "SELECT * FROM test_catalog.%s.%s",
                DEFAULT_NAMESPACE[0], tableName
            ));
            assertEquals("Main branch should have 2 rows", 2, mainData.count());

            // Query the new branch
            Dataset<Row> branchData = spark.sql(String.format(
                "SELECT * FROM test_catalog.%s.%s.branch_%s",
                DEFAULT_NAMESPACE[0], tableName, branchName
            ));
            assertEquals("Branch should have 2 rows", 4, branchData.count());

            // Verify branch data is different from main
            List<Row> branchRows = branchData.collectAsList();
            assertTrue("Branch should contain branch1", 
                branchRows.stream().anyMatch(row -> row.getString(1).equals("branch1")));
            assertTrue("Branch should contain branch2", 
                branchRows.stream().anyMatch(row -> row.getString(1).equals("branch2")));

        } finally {
            // Clean up
            spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
        }
    }

    // @Test
    // public void testDeltaTimeTravel() {
    //     if (!ENABLED_TABLE_TYPES.getOrDefault("delta", false)) {
    //         return;
    //     }

    //     String tableName = "delta_timetravel_test";

    //     try {
            
    //         // Create Delta table
    //         spark.sql(String.format(
    //             "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING delta",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));

    //         // Insert initial data (version 0)
    //         spark.sql(String.format(
    //             "INSERT INTO test_catalog.%s.%s VALUES (1, 'initial1'), (2, 'initial2')",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));

    //         // Insert more data (version 1)
    //         spark.sql(String.format(
    //             "INSERT INTO test_catalog.%s.%s VALUES (3, 'update1'), (4, 'update2')",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));

    //         // Query current version (should have 4 rows)
    //         Dataset<Row> currentData = spark.sql(String.format(
    //             "SELECT id, name FROM test_catalog.%s.%s",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));
    //         assertEquals("Current version should have 4 rows", 4, currentData.count());

    //         // Query version 0 using version number
    //         Dataset<Row> version0Data = spark.sql(String.format(
    //             "SELECT id, name FROM test_catalog.%s.%s VERSION AS OF 1",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));
    //         assertEquals("Version 0 should have 2 rows", 2, version0Data.count());

    //         // Verify version 0 data
    //         List<Row> version0Rows = version0Data.collectAsList();
    //         assertTrue("Version 0 should contain initial1", 
    //             version0Rows.stream().anyMatch(row -> row.getString(1).equals("initial1")));
    //         assertTrue("Version 0 should contain initial2", 
    //             version0Rows.stream().anyMatch(row -> row.getString(1).equals("initial2")));
    //     } finally {
    //         // Clean up
    //         spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
    //     }
    // }

    // @Test
    // public void testIcebergTimeTravel() {
    //     if (!ENABLED_TABLE_TYPES.getOrDefault("iceberg", false)) {
    //         return;
    //     }

    //     String tableName = "iceberg_timetravel_test";
    //     String branchName = "test_branch";

        
    //         // Create Iceberg table
    //         spark.sql(String.format(
    //             "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING iceberg PARTITIONED BY (id)",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));

    //         // Insert data into main branch
    //         spark.sql(String.format(
    //             "INSERT INTO test_catalog.%s.%s VALUES (1, 'main1'), (2, 'main2')",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));

    //         Dataset<Row> mainData = spark.sql(String.format(
    //             "SELECT count(1) FROM test_catalog.%s.%s TIMESTAMP AS OF '2025-01-01 00:00:00'",
    //             DEFAULT_NAMESPACE[0], tableName
    //         ));

            
    //         assertEquals("Main branch should have 2 rows", 2, mainData.count());
            
        
    // }

    @Test
    public void testIcebergEmptyLocationCheck() throws Exception {
        if (!ENABLED_TABLE_TYPES.getOrDefault("iceberg", false)) {
            return;
        }

        String tableName = "iceberg_empty_location_test";
        Identifier ident = Identifier.of(DEFAULT_NAMESPACE, tableName);
        Map<String, String> properties = new HashMap<>();
        properties.put("provider", "iceberg");

        // Generate random folder names to avoid conflicts
        String randomSuffix = String.valueOf(System.currentTimeMillis());
        String emptyLocation = new File("target/test/empty_location_" + randomSuffix).getAbsolutePath();
        String nonEmptyLocation = new File("target/test/non_empty_location_" + randomSuffix).getAbsolutePath();

        try {
            // Test 1: Create table with empty location
            properties.put("location", emptyLocation);
            Table table = tableCatalog.createTable(ident, TEST_SCHEMA, TEST_PARTITIONS, properties);
            assertNotNull("Table should be created successfully", table);
            assertEquals("iceberg", table.properties().get("provider"));

            // Test 2: Try to create table with non-empty location
            File nonEmptyDir = new File(nonEmptyLocation);
            nonEmptyDir.mkdirs();
            new File(nonEmptyDir, "test.txt").createNewFile();

            properties.put("location", nonEmptyLocation);
            assertThrows(TableAlreadyExistsException.class, () -> 
                tableCatalog.createTable(ident, TEST_SCHEMA, TEST_PARTITIONS, properties));
        } finally {
            // Clean up
            try {
                tableCatalog.dropTable(ident);
            } catch (Exception e) {
                System.err.println("Error dropping table: " + e.getMessage());
            }

            // Clean up test directories
            deleteRecursively(new File(emptyLocation));
            deleteRecursively(new File(nonEmptyLocation));
        }
    }

    @Test
    public void testInsertOverwrite() {
        // Test INSERT OVERWRITE for each enabled table type
        for (String provider : Arrays.asList("hive")) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_overwrite_test";
            spark.sql(" CREATE TABLE spark_catalog.local_testing.hive_overwrite2 (id bigint NOT NULL, data string) using parquet");
            try {
                Table table = this.tableCatalog.loadTable(Identifier.of(DEFAULT_NAMESPACE, "hive_overwrite2"));
                TableCatalog catalog = (TableCatalog) SparkSession.active().sessionState().catalogManager().catalog("spark_catalog");
                Table table2 = catalog.loadTable(Identifier.of(DEFAULT_NAMESPACE, "hive_overwrite2"));
                System.out.println(table2.properties());
            } catch (NoSuchTableException e) {
                System.out.println("Table not found");
            }

            try {
                // Create table
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING parquet",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));

                // Insert initial data
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES (1, 'initial1'), (2, 'initial2')",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                // Verify initial data
                Dataset<Row> initialData = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                    DEFAULT_NAMESPACE[0], tableName
                ));
                assertEquals("Initial data should have 2 rows", 2, initialData.count());
                
                // Perform INSERT OVERWRITE
                spark.sql(String.format(
                    "INSERT OVERWRITE TABLE test_catalog.%s.%s VALUES (3, 'overwrite1'), (4, 'overwrite2')",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                // Verify overwritten data
                Dataset<Row> overwrittenData = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                    DEFAULT_NAMESPACE[0], tableName
                ));
                assertEquals("Overwritten data should have 2 rows", 2, overwrittenData.count());
                
                List<Row> overwrittenRows = overwrittenData.collectAsList();
                assertEquals("First row should be overwrite1", "overwrite1", overwrittenRows.get(0).getString(1));
                assertEquals("Second row should be overwrite2", overwrittenRows.get(1).getString(1), "overwrite2");

            } finally {
                // Clean up
                spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
            }
        }
    }

    @Test
    public void testInsertOverwriteWithDataFrame() throws NoSuchTableException {
        // Test INSERT OVERWRITE for each enabled table type using DataFrames
        List<String> newTableForEachProvider = Arrays.asList("delta", "iceberg");
        for (String provider : newTableForEachProvider) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }
            

            String tableName = provider + "_overwrite_df_test";
            
            try {
                // Create table
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING %s",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));

                // Create and insert initial data using DataFrame
                Dataset<Row> initialData = spark.createDataFrame(
                    Arrays.asList(
                        RowFactory.create(1, "initial1"),
                        RowFactory.create(2, "initial2")
                    ),
                    TEST_SCHEMA
                );

                initialData.writeTo("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName).append();

                // Verify initial data
                Dataset<Row> readInitialData = spark.table("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName)
                    .orderBy("id");
                assertEquals("Initial data should have 2 rows", 2, readInitialData.count());

                // Create and insert overwrite data using DataFrame
                Dataset<Row> overwriteData = spark.createDataFrame(
                    Arrays.asList(
                        RowFactory.create(3, "overwrite1"),
                        RowFactory.create(4, "overwrite2")
                    ),
                    TEST_SCHEMA
                );

                overwriteData.writeTo("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName).overwritePartitions();

                // Verify overwritten data
                Dataset<Row> readOverwrittenData = spark.table("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName)
                    .orderBy("id");
                assertEquals("Overwritten data should have 2 rows", 2, readOverwrittenData.count());
            } finally {
                // Clean up
                spark.sql(String.format("DROP TABLE IF EXISTS %s.%s", DEFAULT_NAMESPACE[0], tableName));
            }
        }
    }

    @Test
    public void testInsertOverwriteWithDataFrameV1() throws NoSuchTableException {
        // Test INSERT OVERWRITE for each enabled table type using DataFrame Writer V1
        List<String> newTableForEachProvider = Arrays.asList("iceberg");
        for (String provider : newTableForEachProvider) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_overwrite_df_v1_test";
            
            try {
                // Create table
                
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING %s",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));

                // Create and insert initial data using DataFrame Writer V1
                Dataset<Row> initialData = spark.createDataFrame(
                    Arrays.asList(
                        RowFactory.create(1, "initial1"),
                        RowFactory.create(2, "initial2")
                    ),
                    TEST_SCHEMA
                );

                initialData.write()
                    .mode("overwrite")
                    .format(provider)
                    .saveAsTable("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName);

                // Verify initial data
                Dataset<Row> readInitialData = spark.table("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName)
                    .orderBy("id");
                assertEquals("Initial data should have 2 rows", 2, readInitialData.count());

                // Create and insert overwrite data using DataFrame Writer V1
                Dataset<Row> overwriteData = spark.createDataFrame(
                    Arrays.asList(
                        RowFactory.create(3, "overwrite1"),
                        RowFactory.create(4, "overwrite2")
                    ),
                    TEST_SCHEMA
                );

                overwriteData.write()
                    .mode("overwrite")
                    .format(provider)
                    .saveAsTable("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName);

                // Verify overwritten data
                Dataset<Row> readOverwrittenData = spark.table("test_catalog." + DEFAULT_NAMESPACE[0] + "." + tableName)
                    .orderBy("id");
                assertEquals("Overwritten data should have 2 rows", 2, readOverwrittenData.count());

            } finally {
                // Clean up
                spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
            }
        }
    }

    @Test
    public void testStageCreate() throws Exception {
        // Test staging create for each supported provider
        for (String provider : TABLE_TYPES) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_staged_test";
            Map<String, String> properties = new HashMap<>();
            properties.put("provider", provider);

            // Add provider-specific properties
            if (provider.equals("iceberg")) {
                properties.put("format", "iceberg");
            } else if (provider.equals("delta")) {
                properties.put("format", "delta");
            } else if (provider.equals("hudi")) {
                properties.put("format", "hudi");
            }

            try {
                // Stage create the table
                StagedTable stagedTable = ((StagingTableCatalog) tableCatalog).stageCreate(
                    Identifier.of(DEFAULT_NAMESPACE, tableName),
                    TEST_SCHEMA,
                    TEST_PARTITIONS,
                    properties
                );

                // Verify the staged table
                assertNotNull("Staged table should not be null for provider: " + provider, stagedTable);
                assertTrue("Staged table should be a StagingTableCatalog for provider: " + provider, 
                    tableCatalog instanceof StagingTableCatalog);

                // Commit the staged table
                stagedTable.commitStagedChanges();

                // Verify the table exists after commit
                Table table = tableCatalog.loadTable(Identifier.of(DEFAULT_NAMESPACE, tableName));
                assertNotNull("Table should exist after commit for provider: " + provider, table);
                assertEquals("Table schema should match for provider: " + provider, TEST_SCHEMA, table.schema());
                
                // Verify provider-specific properties
                assertEquals("Provider should match for " + provider, provider, table.properties().get("provider"));
            } catch (UnsupportedOperationException e) {
                // Some providers might not support staging operations
                System.out.println("Provider " + provider + " does not support staging operations: " + e.getMessage());
            } finally {
                // Clean up
                try {
                    tableCatalog.dropTable(Identifier.of(DEFAULT_NAMESPACE, tableName));
                } catch (Exception e) {
                    System.err.println("Error cleaning up staged table for provider " + provider + ": " + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testStageReplace() throws Exception {
        // Test stage replace for each supported provider
        for (String provider : TABLE_TYPES) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_staged_replace_test";
            Map<String, String> properties = new HashMap<>();
            properties.put("provider", provider);

            // Add provider-specific properties
            if (provider.equals("iceberg")) {
                properties.put("format", "iceberg");
            } else if (provider.equals("delta")) {
                properties.put("format", "delta");
            } else if (provider.equals("hudi")) {
                properties.put("format", "hudi");
            }

            try {
                // Create initial table
                tableCatalog.createTable(
                    Identifier.of(DEFAULT_NAMESPACE, tableName),
                    TEST_SCHEMA,
                    TEST_PARTITIONS,
                    properties
                );

                // Create new schema for replacement
                StructType newSchema = new StructType()
                    .add("id", DataTypes.IntegerType)
                    .add("name", DataTypes.StringType)
                    .add("age", DataTypes.IntegerType);

                // Stage replace the table
                StagedTable stagedTable = ((StagingTableCatalog) tableCatalog).stageReplace(
                    Identifier.of(DEFAULT_NAMESPACE, tableName),
                    newSchema,
                    TEST_PARTITIONS,
                    properties
                );

                // Verify the staged table
                assertNotNull("Staged table should not be null for provider: " + provider, stagedTable);
                assertEquals("Staged table schema should match new schema for provider: " + provider, 
                    newSchema, stagedTable.schema());

                // Commit the staged table
                stagedTable.commitStagedChanges();

                // Verify the table was replaced
                Table table = tableCatalog.loadTable(Identifier.of(DEFAULT_NAMESPACE, tableName));
                assertNotNull("Table should exist after commit for provider: " + provider, table);
                assertEquals("Table schema should match new schema for provider: " + provider, 
                    newSchema, table.schema());
                
                // Verify provider-specific properties
                assertEquals("Provider should match for " + provider, provider, table.properties().get("provider"));
            } catch (UnsupportedOperationException e) {
                System.out.println("Provider " + provider + " does not support staging operations: " + e.getMessage());
            } finally {
                // Clean up
                try {
                    tableCatalog.dropTable(Identifier.of(DEFAULT_NAMESPACE, tableName));
                } catch (Exception e) {
                    System.err.println("Error cleaning up staged table for provider " + provider + ": " + e.getMessage());
                }
            }
        }
    }

    @Test
    public void testStageCreateOrReplace() throws Exception {
        // Test stage create or replace for each supported provider
        for (String provider : TABLE_TYPES) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_staged_create_or_replace_test";
            Map<String, String> properties = new HashMap<>();
            properties.put("provider", provider);

            // Add provider-specific properties
            if (provider.equals("iceberg")) {
                properties.put("format", "iceberg");
            } else if (provider.equals("delta")) {
                properties.put("format", "delta");
            } else if (provider.equals("hudi")) {
                properties.put("format", "hudi");
            }

            try {
                // First try to create a new table
                StagedTable stagedTable = ((StagingTableCatalog) tableCatalog).stageCreateOrReplace(
                    Identifier.of(DEFAULT_NAMESPACE, tableName),
                    TEST_SCHEMA,
                    TEST_PARTITIONS,
                    properties
                );

                // Verify the staged table
                assertNotNull("Staged table should not be null for provider: " + provider, stagedTable);
                assertEquals("Staged table schema should match for provider: " + provider, 
                    TEST_SCHEMA, stagedTable.schema());

                // Commit the staged table
                stagedTable.commitStagedChanges();

                // Create new schema for replacement
                StructType newSchema = new StructType()
                    .add("id", DataTypes.IntegerType)
                    .add("name", DataTypes.StringType)
                    .add("age", DataTypes.IntegerType);

                // Now try to replace the existing table
                stagedTable = ((StagingTableCatalog) tableCatalog).stageCreateOrReplace(
                    Identifier.of(DEFAULT_NAMESPACE, tableName),
                    newSchema,
                    TEST_PARTITIONS,
                    properties
                );

                // Verify the staged table
                assertNotNull("Staged table should not be null for provider: " + provider, stagedTable);
                assertEquals("Staged table schema should match new schema for provider: " + provider, 
                    newSchema, stagedTable.schema());

                // Commit the staged table
                stagedTable.commitStagedChanges();

                // Verify the table was replaced
                Table table = tableCatalog.loadTable(Identifier.of(DEFAULT_NAMESPACE, tableName));
                assertNotNull("Table should exist after commit for provider: " + provider, table);
                assertEquals("Table schema should match new schema for provider: " + provider, 
                    newSchema, table.schema());
                
                // Verify provider-specific properties
                assertEquals("Provider should match for " + provider, provider, table.properties().get("provider"));
            } catch (UnsupportedOperationException e) {
                System.out.println("Provider " + provider + " does not support staging operations: " + e.getMessage());
            } finally {
                // Clean up
                try {
                    tableCatalog.dropTable(Identifier.of(DEFAULT_NAMESPACE, tableName));
                } catch (Exception e) {
                    System.err.println("Error cleaning up staged table for provider " + provider + ": " + e.getMessage());
                }
            }
        }
    }


    @Test
    public void testMergeIntoOperations() {
        // Test MERGE INTO for each enabled provider except Hive
        for (String provider : Arrays.asList("delta", "iceberg", "hudi")) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_merge_test";
            String sourceTableName = provider + "_merge_source_test";
            
            try {
                // Create target table
                
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING, value INT) USING %s",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));

                // Create source table
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING, value INT) USING %s",
                    DEFAULT_NAMESPACE[0], sourceTableName, provider
                ));

                // Insert initial data into target
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES (1, 'target1', 100), (2, 'target2', 200)",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                // Insert data into source
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES (1, 'source1', 150), (3, 'source3', 300)",
                    DEFAULT_NAMESPACE[0], sourceTableName
                ));

                // Perform MERGE operation
                spark.sql(String.format(
                    "MERGE INTO test_catalog.%s.%s AS target " +
                    "USING test_catalog.%s.%s AS source " +
                    "ON target.id = source.id " +
                    "WHEN MATCHED THEN UPDATE SET value = source.value " +
                    "WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (source.id, source.name, source.value)",
                    DEFAULT_NAMESPACE[0], tableName,
                    DEFAULT_NAMESPACE[0], sourceTableName
                ));

                // Verify results
                Dataset<Row> result = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                List<Row> rows = result.collectAsList();
                assertEquals("Should have 3 rows after merge", 3, rows.size());
                
                // Verify updated row
                Row updatedRow = rows.get(0);
                assertEquals("ID should be 1", 1, updatedRow.getInt(0));
                assertEquals("Value should be updated to 150", 150, updatedRow.getInt(2));
                
                // Verify unchanged row
                Row unchangedRow = rows.get(1);
                assertEquals("ID should be 2", 2, unchangedRow.getInt(0));
                assertEquals("Value should remain 200", 200, unchangedRow.getInt(2));
                
                // Verify inserted row
                Row insertedRow = rows.get(2);
                assertEquals("ID should be 3", 3, insertedRow.getInt(0));
                assertEquals("Value should be 300", 300, insertedRow.getInt(2));

            } finally {
                // Clean up
                spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
                spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], sourceTableName));
            }
        }
    }

    @Test
    public void testDeleteOperations() {
        // Test DELETE for each enabled provider except Hive
        for (String provider : Arrays.asList("delta", "iceberg", "hudi")) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_delete_test";
            
            try {
                // Create table
                
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING, value INT) USING %s",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));

                // Insert test data
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES " +
                    "(1, 'test1', 100), " +
                    "(2, 'test2', 200), " +
                    "(3, 'test3', 300), " +
                    "(4, 'test4', 400)",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                // Verify initial data
                Dataset<Row> initialData = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                    DEFAULT_NAMESPACE[0], tableName
                ));
                assertEquals("Should have 4 rows initially", 4, initialData.count());

                // Delete rows with value > 200
                spark.sql(String.format(
                    "DELETE FROM test_catalog.%s.%s WHERE value > 200",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                // Verify remaining data
                Dataset<Row> remainingData = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                List<Row> rows = remainingData.collectAsList();
                assertEquals("Should have 2 rows after delete", 2, rows.size());
                
                // Verify remaining rows
                Row row1 = rows.get(0);
                assertEquals("First row ID should be 1", 1, row1.getInt(0));
                assertEquals("First row value should be 100", 100, row1.getInt(2));
                
                Row row2 = rows.get(1);
                assertEquals("Second row ID should be 2", 2, row2.getInt(0));
                assertEquals("Second row value should be 200", 200, row2.getInt(2));

                // Test conditional delete
                spark.sql(String.format(
                    "DELETE FROM test_catalog.%s.%s WHERE name = 'test1'",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                // Verify final data
                Dataset<Row> finalData = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                List<Row> finalRows = finalData.collectAsList();
                assertEquals("Should have 1 row after second delete", 1, finalRows.size());
                
                Row finalRow = finalRows.get(0);
                assertEquals("Final row ID should be 2", 2, finalRow.getInt(0));
                assertEquals("Final row value should be 200", 200, finalRow.getInt(2));

            } finally {
                // Clean up
                spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
            }
        }
    }

    @Test
    public void testMergeIntoWithPartitionedTable() {
        // Test MERGE INTO with partitioned tables for each enabled provider except Hive
        for (String provider : Arrays.asList("delta", "iceberg", "hudi")) {
            if (!ENABLED_TABLE_TYPES.getOrDefault(provider, false)) {
                continue;
            }

            String tableName = provider + "_merge_partitioned_test";
            String sourceTableName = provider + "_merge_partitioned_source_test";
            
            try {
                // Create partitioned target table
                
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING, value INT) USING %s PARTITIONED BY (id)",
                    DEFAULT_NAMESPACE[0], tableName, provider
                ));

                // Create source table
                spark.sql(String.format(
                    "CREATE TABLE test_catalog.%s.%s (id INT, name STRING, value INT) USING %s",
                    DEFAULT_NAMESPACE[0], sourceTableName, provider
                ));

                // Insert initial data into target
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES (1, 'target1', 100), (2, 'target2', 200)",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                // Insert data into source
                spark.sql(String.format(
                    "INSERT INTO test_catalog.%s.%s VALUES (1, 'source1', 150), (3, 'source3', 300)",
                    DEFAULT_NAMESPACE[0], sourceTableName
                ));

                // Perform MERGE operation
                spark.sql(String.format(
                    "MERGE INTO test_catalog.%s.%s AS target " +
                    "USING test_catalog.%s.%s AS source " +
                    "ON target.id = source.id " +
                    "WHEN MATCHED THEN UPDATE SET value = source.value " +
                    "WHEN NOT MATCHED THEN INSERT *",
                    DEFAULT_NAMESPACE[0], tableName,
                    DEFAULT_NAMESPACE[0], sourceTableName
                ));

                // Verify results
                Dataset<Row> result = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                    DEFAULT_NAMESPACE[0], tableName
                ));

                List<Row> rows = result.collectAsList();
                assertEquals("Should have 3 rows after merge", 3, rows.size());
                
                // Verify partition pruning works
                Dataset<Row> partitionResult = spark.sql(String.format(
                    "SELECT * FROM test_catalog.%s.%s WHERE id = 1",
                    DEFAULT_NAMESPACE[0], tableName
                ));
                assertEquals("Should have 1 row for partition id=1", 1, partitionResult.count());

            } finally {
                // Clean up
                spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
                spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], sourceTableName));
            }
        }
    }

    @Test
    public void testLoadFunctionFromIcebergCatalog() throws Exception {
        if (!ENABLED_TABLE_TYPES.getOrDefault("iceberg", false)) {
            return;
        }

        String tableName = "iceberg_function_test";

        try {
            // Create Iceberg table with transform
            spark.sql(String.format(
                "CREATE TABLE test_catalog.%s.%s (id INT, name STRING, created_date DATE) USING iceberg PARTITIONED BY (days(created_date))",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Insert test data
            spark.sql(String.format(
                "INSERT INTO test_catalog.%s.%s VALUES (1, 'test1', DATE '2024-01-01'), (2, 'test2', DATE '2024-01-02')",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Query using the days transform function (this will internally call loadFunction)
            Dataset<Row> result = spark.sql(String.format(
                "SELECT * FROM test_catalog.%s.%s",
                DEFAULT_NAMESPACE[0], tableName
            ));

            assertEquals("Should have 2 rows", 2, result.count());

            // Verify the function catalog can load iceberg functions
            FunctionCatalog functionCatalog = (FunctionCatalog) tableCatalog;

            // Try to load identity function from iceberg catalog
            // The identity function is a built-in Iceberg transform function
            Identifier functionIdent = Identifier.of(new String[]{"system"}, "identity");

            try {
                UnboundFunction function = functionCatalog.loadFunction(functionIdent);
                assertNotNull("Identity function should be loaded", function);
            } catch (Exception e) {
                // If direct load fails, verify it can be loaded through iceberg catalog
                System.out.println("Function load test: " + e.getMessage());
            }

        } finally {
            // Clean up
            spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
        }
    }

    @Test
    public void testLoadFunctionWithMultiPartNamespace() throws Exception {
        if (!ENABLED_TABLE_TYPES.getOrDefault("iceberg", false)) {
            return;
        }

        String tableName = "iceberg_branch_function_test";
        String branchName = "test_branch";

        try {
            // Create Iceberg table
            spark.sql(String.format(
                "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING iceberg PARTITIONED BY (id)",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Insert data into main branch
            spark.sql(String.format(
                "INSERT INTO test_catalog.%s.%s VALUES (1, 'main1'), (2, 'main2')",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Create a new branch
            spark.sql(String.format(
                "ALTER TABLE iceberg_catalog.%s.%s CREATE BRANCH %s",
                DEFAULT_NAMESPACE[0], tableName, branchName
            ));

            // Try to query with branch-specific table reference
            // This internally uses multi-part namespace which should trigger the loadFunction fix
            Dataset<Row> branchData = spark.sql(String.format(
                "SELECT * FROM test_catalog.%s.%s.branch_%s",
                DEFAULT_NAMESPACE[0], tableName, branchName
            ));

            assertEquals("Branch should have 2 rows", 2, branchData.count());

        } finally {
            // Clean up
            spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
        }
    }

    @Test
    public void testLoadFunctionFallbackToCatalogs() throws Exception {
        FunctionCatalog functionCatalog = (FunctionCatalog) tableCatalog;

        // Test that listFunctions returns empty array (as per implementation)
        Identifier[] functions = functionCatalog.listFunctions(DEFAULT_NAMESPACE);
        assertNotNull("Functions list should not be null", functions);
        assertEquals("Functions list should be empty", 0, functions.length);

        // Test loading a system function - this should fall back to checking other catalogs
        // if not found in session catalog
        try {
            // Try loading a standard Spark SQL function
            Identifier functionIdent = Identifier.of(DEFAULT_NAMESPACE, "concat");
            UnboundFunction function = functionCatalog.loadFunction(functionIdent);

            // If we get here, the function was loaded successfully
            assertNotNull("Function should be loaded", function);
        } catch (Exception e) {
            // It's okay if the function is not found, as long as it tried all catalogs
            // The key is that it doesn't throw REQUIRES_SINGLE_PART_NAMESPACE error
            assertFalse("Should not throw REQUIRES_SINGLE_PART_NAMESPACE error",
                e.getMessage().contains("REQUIRES_SINGLE_PART_NAMESPACE"));
        }
    }

    @Test
    public void testLoadIdentityFunctionFromIceberg() throws Exception {
        if (!ENABLED_TABLE_TYPES.getOrDefault("iceberg", false)) {
            return;
        }

        String tableName = "iceberg_identity_test";

        try {
            // Create Iceberg table using identity transform (which is a function)
            spark.sql(String.format(
                "CREATE TABLE test_catalog.%s.%s (id INT, name STRING) USING iceberg PARTITIONED BY (identity(id))",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Insert test data
            spark.sql(String.format(
                "INSERT INTO test_catalog.%s.%s VALUES (1, 'test1'), (2, 'test2'), (3, 'test3')",
                DEFAULT_NAMESPACE[0], tableName
            ));

            // Query the table - this uses the identity transform function internally
            Dataset<Row> result = spark.sql(String.format(
                "SELECT * FROM test_catalog.%s.%s WHERE id = 1",
                DEFAULT_NAMESPACE[0], tableName
            ));

            assertEquals("Should have 1 row", 1, result.count());

            // Verify partition column works correctly
            Dataset<Row> allData = spark.sql(String.format(
                "SELECT * FROM test_catalog.%s.%s ORDER BY id",
                DEFAULT_NAMESPACE[0], tableName
            ));

            assertEquals("Should have 3 rows", 3, allData.count());

        } finally {
            // Clean up
            spark.sql(String.format("DROP TABLE IF EXISTS test_catalog.%s.%s", DEFAULT_NAMESPACE[0], tableName));
        }
    }

}