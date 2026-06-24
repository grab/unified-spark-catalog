package com.grab;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Function;
import java.io.File;
import scala.Option;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NamespaceAlreadyExistsException;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.apache.spark.sql.catalyst.analysis.NoSuchDatabaseException;
import org.apache.spark.sql.catalyst.analysis.NonEmptyNamespaceException;
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException;
import org.apache.spark.sql.connector.catalog.CatalogExtension;
import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.DelegatingCatalogExtension;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.NamespaceChange;
import org.apache.spark.sql.connector.catalog.SupportsNamespaces;
import org.apache.spark.sql.connector.catalog.StagingTableCatalog;
import org.apache.spark.sql.connector.catalog.StagedTable;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.connector.catalog.TableCatalog;
import org.apache.spark.sql.connector.catalog.TableChange;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

public class UnifiedSparkCatalog <T extends TableCatalog & FunctionCatalog & SupportsNamespaces & StagingTableCatalog>
        implements CatalogExtension, StagingTableCatalog, SupportsNamespaces, FunctionCatalog {
    private String catalogName;
    private TableCatalog icebergCatalog;
    private TableCatalog deltaCatalog;
    private TableCatalog hudiCatalog;
    T sessionCatalog;
    private CatalogPlugin rawDelegate; // Store the raw delegate for function catalog operations
    private Logger logger = LogManager.getLogger(UnifiedSparkCatalog.class);
    private LinkedHashMap<String, TableCatalog> catalogsByProvider;
    private CaseInsensitiveStringMap initializationOptions;
    private volatile boolean catalogsInitialized = false;

    /**
     * Check if a catalog exists in the Spark catalog manager
     *
     * @param catalogName The name of the catalog to check
     * @return true if the catalog exists, false otherwise
     */
    private boolean catalogExistsInManager(String catalogName) {
        try {
            SparkSession.active().sessionState().catalogManager().catalog(catalogName);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * Get an existing catalog from the Spark catalog manager
     *
     * @param catalogName The name of the catalog to retrieve
     * @return The catalog if it exists, null otherwise
     */
    private TableCatalog getExistingCatalog(String catalogName) {
        try {
            CatalogPlugin catalog = SparkSession.active().sessionState().catalogManager().catalog(catalogName);
            if (catalog instanceof TableCatalog) {
                return (TableCatalog) catalog;
            }
        } catch (Exception e) {
            logger.debug("Catalog {} not found in catalog manager: {}", catalogName, e.getMessage());
        }
        return null;
    }

    /**
     * Helper method to safely initialize a catalog
     *
     * @param catalogType The type name of the catalog (e.g., "iceberg", "delta", "hudi")
     * @param className The fully qualified class name of the catalog
     * @param options The initialization options
     * @return The initialized catalog or null if initialization failed
     */
    private TableCatalog initializeCatalog(String catalogType, String className, CaseInsensitiveStringMap options) {
        try {
            Class<?> catalogClass = Class.forName(className);
            TableCatalog catalog = (TableCatalog) catalogClass.getDeclaredConstructor().newInstance();
            catalog.initialize(catalogType, options);
            logger.info("{}: Successfully initialized {} catalog", catalogName, catalogType);
            return catalog;
        } catch (ClassNotFoundException e) {
            logger.warn("{} catalog classes not found in classpath, {} tables will not be available: {}",
                    catalogType, catalogType, e.getMessage());
        } catch (Exception e) {
            logger.warn("Failed to initialize {} catalog: {}", catalogType, e.getMessage());
        }
        return null;
    }

    /**
     * Lazy initialization of catalogs - checks existing catalogs first, then creates new ones if needed
     */
    private synchronized void initializeCatalogsLazily() {
        if (catalogsInitialized) {
            return;
        }

        // Initialize catalog lookup map
        this.catalogsByProvider = new LinkedHashMap<>();

        // Initialize session catalog if not already done
        if (this.sessionCatalog == null) {
            initializeSessionCatalog();
        }

        // Check for existing Iceberg catalog
        if (catalogExistsInManager("iceberg_catalog")) {
            this.icebergCatalog = getExistingCatalog("iceberg_catalog");
            logger.info("Using existing iceberg_catalog from catalog manager");
        } else {
            this.icebergCatalog = initializeCatalog("iceberg", "org.apache.iceberg.spark.SparkCatalog", initializationOptions);
        }

        // Check for existing Delta catalog
        if (catalogExistsInManager("delta_catalog")) {
            this.deltaCatalog = getExistingCatalog("delta_catalog");
            logger.info("Using existing delta_catalog from catalog manager");
        } else {
            this.deltaCatalog = initializeCatalog("delta", "org.apache.spark.sql.delta.catalog.DeltaCatalog", initializationOptions);
        }

        // Check for existing Hudi catalog
        if (catalogExistsInManager("hudi_catalog")) {
            this.hudiCatalog = getExistingCatalog("hudi_catalog");
            logger.info("{}: Using existing hudi_catalog from catalog manager", catalogName);
        } else {
            this.hudiCatalog = initializeCatalog("hudi", "org.apache.spark.sql.hudi.catalog.HoodieCatalog", initializationOptions);
        }

        // Add catalogs to the lookup map
        if (!this.catalogName.equals("spark_catalog")) {
            catalogsByProvider.put("hive", this.sessionCatalog);
        }
        if (icebergCatalog != null) {
            catalogsByProvider.put("iceberg", icebergCatalog);
        }
        if (deltaCatalog != null) {
            catalogsByProvider.put("delta", deltaCatalog);
            if (deltaCatalog instanceof DelegatingCatalogExtension) {
                DelegatingCatalogExtension deltaCatalogExtension = (DelegatingCatalogExtension) this.deltaCatalog;
                deltaCatalogExtension.setDelegateCatalog(this.sessionCatalog);
            }
        }
        if (hudiCatalog != null) {
            catalogsByProvider.put("hudi", hudiCatalog);
            // Set delegate catalog for Hudi since it extends DelegatingCatalogExtension
            if (hudiCatalog instanceof DelegatingCatalogExtension) {
                DelegatingCatalogExtension hudiCatalogExtension = (DelegatingCatalogExtension) this.hudiCatalog;
                hudiCatalogExtension.setDelegateCatalog(this.sessionCatalog);
            }
        }
        catalogsInitialized = true;


    }

    @Override
    public void initialize(String name, CaseInsensitiveStringMap options) {
        this.catalogName = name;
        this.initializationOptions = options;
        // Catalogs will be initialized lazily when first needed
    }


    @Override
    public String name() {
        return catalogName != null ? "spark_catalog" : "unified";
    }

    @Override
    public Identifier[] listTables(String[] namespace) throws NoSuchNamespaceException {
        ensureCatalogsInitialized();
        requireSessionCatalog();
        return this.sessionCatalog.listTables(namespace);
    }

    public void initializeSessionCatalog() {
        this.sessionCatalog = (T) SparkSession.active().sessionState().catalogManager().catalog("spark_catalog");
    }

    /**
     * Ensure catalogs are initialized before use
     */
    private void ensureCatalogsInitialized() {
        if (!catalogsInitialized) {
            initializeCatalogsLazily();
        }
    }

    /**
     * Check if sessionCatalog is available for table operations
     */
    private void requireSessionCatalog() throws IllegalStateException {
        if (this.sessionCatalog == null) {
            throw new IllegalStateException(
                "Session catalog is not available. The delegate catalog does not support table operations.");
        }
    }

    @Override
    public Table loadTable(Identifier ident, String version) throws NoSuchTableException {
        ensureCatalogsInitialized();
        Table table = this.sessionCatalog.loadTable(ident);
        if (TableTypeDetector.isDeltaLakeTable(table) && deltaCatalog != null) {
            return deltaCatalog.loadTable(ident, version);
        } else if (TableTypeDetector.isIcebergTable(table) && icebergCatalog != null) {
            return icebergCatalog.loadTable(ident, version);
        } else if (TableTypeDetector.isHudiTable(table) && hudiCatalog != null) {
            return hudiCatalog.loadTable(ident, version);
        }
        return table;
    }

    @Override
    public Table loadTable(Identifier ident, long timestamp) throws NoSuchTableException {
        ensureCatalogsInitialized();
        Table table = this.sessionCatalog.loadTable(ident);
        if (TableTypeDetector.isDeltaLakeTable(table) && deltaCatalog != null) {
            return deltaCatalog.loadTable(ident, timestamp);
        } else if (TableTypeDetector.isIcebergTable(table) && icebergCatalog != null) {
            return icebergCatalog.loadTable(ident, timestamp);
        } else if (TableTypeDetector.isHudiTable(table) && hudiCatalog != null) {
            return hudiCatalog.loadTable(ident, timestamp);
        }
        return table;
    }

    /**
     * Helper method to try loading a table from all available catalogs
     * @param ident Table identifier
     * @return The loaded table
     * @throws NoSuchTableException if table is not found in any catalog
     */
    @Override
    public Table loadTable(Identifier ident) throws NoSuchTableException {
        ensureCatalogsInitialized();

        try {
            Table table = this.sessionCatalog.loadTable(ident);
            if (TableTypeDetector.isIcebergTable(table) && icebergCatalog != null) {
                return icebergCatalog.loadTable(ident);
            } else if (TableTypeDetector.isDeltaLakeTable(table) && deltaCatalog != null) {
                return deltaCatalog.loadTable(ident);
            } else if (TableTypeDetector.isHudiTable(table) && hudiCatalog != null) {
                return hudiCatalog.loadTable(ident);
            } else {
                return table;
            }
        } catch (NoSuchTableException e) {
            if (TableTypeDetector.isDeltaLakePath(ident)) {
                return deltaCatalog != null ? deltaCatalog.loadTable(ident) : this.sessionCatalog.loadTable(ident);
            }
            throw e;
        } catch (Exception e) {
            if (e instanceof NoSuchDatabaseException && TableTypeDetector.isDeltaLakePath(ident)) {
                return deltaCatalog != null ? deltaCatalog.loadTable(ident) : this.sessionCatalog.loadTable(ident);
            }
            if (e instanceof AnalysisException &&
            e.getMessage().contains("REQUIRES_SINGLE_PART_NAMESPACE") && icebergCatalog != null) {
                logger.info("Detected iceberg table with branch or tag qualifier for {}, loading from iceberg catalog", ident);
                return icebergCatalog.loadTable(ident);
            }
            logger.error("Error loading table: {}", e.getClass());
            throw e;
        }
    }


    /**
     * Get the appropriate catalog for the given provider type
     * @param provider Provider type (iceberg, delta, hudi)
     * @return The corresponding catalog
     * @throws IllegalArgumentException if the requested catalog is not available
     */
    private TableCatalog getCatalogForProvider(String provider) {
        ensureCatalogsInitialized();

        if (provider == null) {
            return this.sessionCatalog;
        }

        TableCatalog catalog = catalogsByProvider.get(provider.toLowerCase());

        return catalog != null ? catalog : this.sessionCatalog;
    }

    @Override
    public Table createTable(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
    throws TableAlreadyExistsException, NoSuchNamespaceException {
        ensureCatalogsInitialized();

        String provider = properties.get("provider");
        logger.info("Creating table {} with provider: {}", ident, provider);
        if (provider == null) {
            return this.sessionCatalog.createTable(ident, schema, partitions, properties);
        } else {
            TableCatalog catalog = getCatalogForProvider(provider);
            if (provider.equals("iceberg")) {
                // In case of iceberg, we need to check if the location at which the table is being created is empty or not
                String location = properties.get("location");
                if (location != null) {
                    File locationFile = new File(location);
                    String[] entries = locationFile.list();
                    if (entries != null && entries.length > 0) {
                        throw new TableAlreadyExistsException("Location is not empty: " + location, Option.empty());
                    }
                }
            }
            return catalog.createTable(ident, schema, partitions, properties);
        }
    }

    /**
     * Helper method to try an operation on multiple catalogs until one succeeds
     * @param operation The operation to try
     * @return true if any catalog succeeded, false otherwise
     */
    private boolean tryOperationOnAllCatalogs(Function<TableCatalog, Boolean> operation) {
        ensureCatalogsInitialized();

        // Try specialized catalogs
        for (TableCatalog catalog : catalogsByProvider.values()) {
            try {
                if (operation.apply(catalog)) {
                    return true;
                }
            } catch (Exception e) {
                logger.debug("Operation failed on catalog: {}", e.getMessage());
            }
        }

        // Try session catalog first
        try {
            if (operation.apply(sessionCatalog)) {
                return true;
            }
        } catch (Exception e) {
            logger.debug("Operation failed on session catalog: {}", e.getMessage());
        }

        return false;
    }

    @Override
    public boolean dropTable(Identifier ident) {
        ensureCatalogsInitialized();

        try {
            TableCatalog catalog = getCatalogForTable(ident);
            return catalog.dropTable(ident);
        } catch (NoSuchTableException e) {
            // If table not found, try specialized catalogs as fallback
            if (TableTypeDetector.isDeltaLakePath(ident)) {
                return deltaCatalog != null && deltaCatalog.dropTable(ident);
            }

            // Try each specialized catalog as fallback
            for (TableCatalog catalog : catalogsByProvider.values()) {
                if (catalog != this.sessionCatalog) { // Skip session catalog as we already tried it
                    try {
                        if (catalog.dropTable(ident)) {
                            return true;
                        }
                    } catch (Exception ex) {
                        logger.debug("Drop table failed on catalog {}: {}", catalog.name(), ex.getMessage());
                    }
                }
            }

            // If all else fails, try session catalog
            try {
                return this.sessionCatalog.dropTable(ident);
            } catch (Exception ex) {
                logger.debug("Drop table failed on session catalog: {}", ex.getMessage());
                return false;
            }
        } catch (Exception e) {
            logger.error("Error determining table type for drop: {}", e.getMessage());
            // Fallback to trying all catalogs
            return tryOperationOnAllCatalogs(catalog -> catalog.dropTable(ident));
        }
    }

    @Override
    public boolean purgeTable(Identifier ident) {
        ensureCatalogsInitialized();

        try {
            TableCatalog catalog = getCatalogForTable(ident);
            return catalog.purgeTable(ident);
        } catch (NoSuchTableException e) {
            // If table not found, try specialized catalogs as fallback
            if (TableTypeDetector.isDeltaLakePath(ident)) {
                return deltaCatalog != null && deltaCatalog.purgeTable(ident);
            }

            // Try each specialized catalog as fallback
            for (TableCatalog catalog : catalogsByProvider.values()) {
                if (catalog != this.sessionCatalog) { // Skip session catalog as we already tried it
                    try {
                        if (catalog.purgeTable(ident)) {
                            return true;
                        }
                    } catch (Exception ex) {
                        logger.debug("Purge table failed on catalog {}: {}", catalog.name(), ex.getMessage());
                    }
                }
            }

            // If all else fails, try session catalog
            try {
                return this.sessionCatalog.purgeTable(ident);
            } catch (Exception ex) {
                logger.debug("Purge table failed on session catalog: {}", ex.getMessage());
                return false;
            }
        } catch (Exception e) {
            logger.error("Error determining table type for purge: {}", e.getMessage());
            // Fallback to trying all catalogs
            return tryOperationOnAllCatalogs(catalog -> catalog.purgeTable(ident));
        }
    }

    /**
     * Get the catalog for a specific table
     * @param ident The table identifier
     * @return The appropriate catalog for the table
     * @throws NoSuchTableException if the table doesn't exist
     */
    private TableCatalog getCatalogForTable(Identifier ident) throws NoSuchTableException {
        Table table = loadTable(ident);
        String provider = table.properties().get("provider");
        return getCatalogForProvider(provider);
    }



    @Override
    public void renameTable(Identifier from, Identifier to) throws NoSuchTableException, TableAlreadyExistsException {
        ensureCatalogsInitialized();

        try {
            TableCatalog catalog = getCatalogForTable(from);
            catalog.renameTable(from, to);
        } catch (NoSuchTableException e) {
            // If table not found, try specialized catalogs as fallback
            if (TableTypeDetector.isDeltaLakePath(from)) {
                if (deltaCatalog != null) {
                    deltaCatalog.renameTable(from, to);
                    return;
                }
            }

            // Try each specialized catalog as fallback
            for (TableCatalog catalog : catalogsByProvider.values()) {
                if (catalog != this.sessionCatalog) { // Skip session catalog as we already tried it
                    try {
                        catalog.renameTable(from, to);
                        return;
                    } catch (Exception ex) {
                        logger.debug("Rename table failed on catalog {}: {}", catalog.name(), ex.getMessage());
                    }
                }
            }

            // If all else fails, try session catalog
            this.sessionCatalog.renameTable(from, to);
        } catch (Exception e) {
            logger.error("Error determining table type for rename: {}", e.getMessage());
            // Fallback to the original method
            TableCatalog catalog = getCatalogForTable(from);
            catalog.renameTable(from, to);
        }
    }

    @Override
    public Table alterTable(Identifier ident, TableChange... changes) throws NoSuchTableException {
        ensureCatalogsInitialized();

        try {
            TableCatalog catalog = getCatalogForTable(ident);
            return catalog.alterTable(ident, changes);
        } catch (NoSuchTableException e) {
            // If table not found, try specialized catalogs as fallback
            if (TableTypeDetector.isDeltaLakePath(ident)) {
                if (deltaCatalog != null) {
                    return deltaCatalog.alterTable(ident, changes);
                }
            }

            // Try each specialized catalog as fallback
            for (TableCatalog catalog : catalogsByProvider.values()) {
                if (catalog != this.sessionCatalog) { // Skip session catalog as we already tried it
                    try {
                        return catalog.alterTable(ident, changes);
                    } catch (Exception ex) {
                        logger.debug("Alter table failed on catalog {}: {}", catalog.name(), ex.getMessage());
                    }
                }
            }

            // If all else fails, try session catalog
            return this.sessionCatalog.alterTable(ident, changes);
        } catch (Exception e) {
            logger.error("Error determining table type for alter: {}", e.getMessage());
            // Fallback to the original method
            TableCatalog catalog = getCatalogForTable(ident);
            return catalog.alterTable(ident, changes);
        }
    }

    @Override
    public void setDelegateCatalog(CatalogPlugin delegate) {
        // Store the raw delegate for function-only operations
        this.rawDelegate = delegate;

        // Only cast if delegate is a TableCatalog (minimum requirement)
        if (delegate instanceof TableCatalog) {
            // Safe to cast - delegate is at least a TableCatalog
            this.sessionCatalog = (T) delegate;

            // Update Delta catalog's delegate if needed
            if (this.deltaCatalog != null && this.deltaCatalog instanceof DelegatingCatalogExtension) {
                DelegatingCatalogExtension deltaCatalogExtension = (DelegatingCatalogExtension) this.deltaCatalog;
                deltaCatalogExtension.setDelegateCatalog(delegate);
            }

            // Update Hudi catalog's delegate if needed
            if (this.hudiCatalog != null && this.hudiCatalog instanceof DelegatingCatalogExtension) {
                DelegatingCatalogExtension hudiCatalogExtension = (DelegatingCatalogExtension) this.hudiCatalog;
                hudiCatalogExtension.setDelegateCatalog(delegate);
            }

            // Update the catalog map
            if (catalogsByProvider != null) {
                catalogsByProvider.put("hive", this.sessionCatalog);
            }
        } else {
            // Delegate is not a TableCatalog (e.g., BuiltInFunctionCatalog)
            logger.debug("Delegate is not a TableCatalog: {}", delegate.getClass().getName());
        }

        logger.info("Delegate catalog set to: {}", delegate);
    }

    @Override
    public String[] defaultNamespace() {
        return new String[] { "default" };
    }

    @Override
    public String[][] listNamespaces() throws NoSuchNamespaceException {
        ensureCatalogsInitialized();
        requireSessionCatalog();
        return this.sessionCatalog.listNamespaces();
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(String[] namespace) throws NoSuchNamespaceException {
        ensureCatalogsInitialized();
        return this.sessionCatalog.loadNamespaceMetadata(namespace);
    }

    @Override
    public void createNamespace(String[] namespace, Map<String, String> metadata) throws NamespaceAlreadyExistsException {
        ensureCatalogsInitialized();
        this.sessionCatalog.createNamespace(namespace, metadata);
    }

    @Override
    public boolean dropNamespace(String[] namespace, boolean cascade) throws NoSuchNamespaceException, NonEmptyNamespaceException {
        ensureCatalogsInitialized();
        return this.sessionCatalog.dropNamespace(namespace, cascade);
    }

    @Override
    public void alterNamespace(String[] namespace, NamespaceChange... changes) throws NoSuchNamespaceException {
        ensureCatalogsInitialized();
        this.sessionCatalog.alterNamespace(namespace, changes);
    }

    @Override
    public String[][] listNamespaces(String[] namespace) throws NoSuchNamespaceException {
        ensureCatalogsInitialized();
        return this.sessionCatalog.listNamespaces(namespace);
    }

    @Override
    public Identifier[] listFunctions(String[] namespace) {
        return new Identifier[0];
    }

    @Override
    public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
        ensureCatalogsInitialized();

        try {
            return this.sessionCatalog.loadFunction(ident);
        } catch (AnalysisException e) {
            // Handle multi-part namespace (e.g., iceberg with branch/tag)
            if (e.getMessage().contains("REQUIRES_SINGLE_PART_NAMESPACE")) {
                logger.info("Function {} requires multi-part namespace, loading from iceberg catalog", ident);
                if (icebergCatalog instanceof FunctionCatalog) {
                    return ((FunctionCatalog) icebergCatalog).loadFunction(ident);
                }
            }

            // If it's a NoSuchFunctionException, try loading from other catalogs
            if (e instanceof NoSuchFunctionException) {
                for (TableCatalog catalog : catalogsByProvider.values()) {
                    if (catalog instanceof FunctionCatalog && catalog != this.sessionCatalog) {
                        try {
                            return ((FunctionCatalog) catalog).loadFunction(ident);
                        } catch (Exception ex) {
                            logger.debug("Failed to load function from catalog {}: {}", catalog.name(), ex.getMessage());
                        }
                    }
                }
            }

            throw e;
        }
    }

    @Override
    public StagedTable stageCreate(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
            throws TableAlreadyExistsException, NoSuchNamespaceException {
        ensureCatalogsInitialized();

        String provider = properties.get("provider");
        TableCatalog catalog = getCatalogForProvider(provider);
        logger.info("Staging create table {} with provider: {}", ident, provider);
        if (catalog instanceof StagingTableCatalog) {
            return ((StagingTableCatalog) catalog).stageCreate(ident, schema, partitions, properties);
        }
        // For Hive tables without explicit provider, delegate to session catalog
        if (provider == null && this.sessionCatalog instanceof StagingTableCatalog) {
            return ((StagingTableCatalog) this.sessionCatalog).stageCreate(ident, schema, partitions, properties);
        }
        throw new UnsupportedOperationException("Staging operations are not supported for provider: " + provider);
    }

    @Override
    public StagedTable stageReplace(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
            throws NoSuchNamespaceException, NoSuchTableException {
        ensureCatalogsInitialized();

        String provider = properties.get("provider");
        TableCatalog catalog = getCatalogForProvider(provider);
        if (catalog instanceof StagingTableCatalog) {
            return ((StagingTableCatalog) catalog).stageReplace(ident, schema, partitions, properties);
        }
        // For Hive tables without explicit provider, delegate to session catalog
        if (provider == null && this.sessionCatalog instanceof StagingTableCatalog) {
            return ((StagingTableCatalog) this.sessionCatalog).stageReplace(ident, schema, partitions, properties);
        }
        throw new UnsupportedOperationException("Staging operations are not supported for provider: " + provider);
    }

    @Override
    public StagedTable stageCreateOrReplace(Identifier ident, StructType schema, Transform[] partitions, Map<String, String> properties)
            throws NoSuchNamespaceException {
        ensureCatalogsInitialized();

        String provider = properties.get("provider");
        TableCatalog catalog = getCatalogForProvider(provider);
        logger.info("stageCreateOrReplace create table {} with provider: {}", ident, provider);
        if (catalog instanceof StagingTableCatalog) {
            return ((StagingTableCatalog) catalog).stageCreateOrReplace(ident, schema, partitions, properties);
        }
        // For Hive tables without explicit provider, delegate to session catalog
        if (provider == null && this.sessionCatalog instanceof StagingTableCatalog) {
            return ((StagingTableCatalog) this.sessionCatalog).stageCreateOrReplace(ident, schema, partitions, properties);
        }
        throw new UnsupportedOperationException("Staging operations are not supported for provider: " + provider);
    }
}
