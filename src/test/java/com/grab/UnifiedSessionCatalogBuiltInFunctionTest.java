package com.grab;

import org.apache.spark.sql.connector.catalog.CatalogPlugin;
import org.apache.spark.sql.connector.catalog.FunctionCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;
import org.apache.spark.sql.connector.catalog.functions.UnboundFunction;
import org.apache.spark.sql.catalyst.analysis.NoSuchFunctionException;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.HashMap;

/**
 * Test to verify UnifiedSessionCatalog handles BuiltInFunctionCatalog without ClassCastException
 */
public class UnifiedSessionCatalogBuiltInFunctionTest {

    private UnifiedSessionCatalog catalog;

    @Before
    public void setup() {
        catalog = new UnifiedSessionCatalog();
        catalog.initialize("test_catalog", new CaseInsensitiveStringMap(new HashMap<>()));
    }

    @Test
    public void testSetDelegateCatalogWithFunctionOnlyCatalog() {
        // Create a mock FunctionCatalog that is NOT a TableCatalog
        // This simulates BuiltInFunctionCatalog$ behavior
        CatalogPlugin functionOnlyCatalog = new FunctionCatalog() {
            @Override
            public void initialize(String name, CaseInsensitiveStringMap options) {
                // Do nothing
            }

            @Override
            public String name() {
                return "function_only";
            }

            @Override
            public Identifier[] listFunctions(String[] namespace) {
                return new Identifier[0];
            }

            @Override
            public UnboundFunction loadFunction(Identifier ident) throws NoSuchFunctionException {
                throw new NoSuchFunctionException(ident);
            }
        };

        // This should NOT throw ClassCastException
        try {
            catalog.setDelegateCatalog(functionOnlyCatalog);
            // Success - no exception thrown
        } catch (ClassCastException e) {
            fail("Should not throw ClassCastException when setting a function-only catalog");
        }

        // Verify sessionCatalog is null (not set since delegate is not a TableCatalog)
        assertNull("sessionCatalog should be null when delegate is not a TableCatalog",
                   catalog.sessionCatalog);
    }
}