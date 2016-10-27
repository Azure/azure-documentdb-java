package com.microsoft.azure.documentdb;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.junit.Assert;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.internal.ServiceJNIWrapper;
import com.microsoft.azure.documentdb.internal.Utils;
import com.microsoft.azure.documentdb.internal.query.PartitionedQueryExecutionInfoInternal;

public class ServiceJNITests {
    private static long getServiceProvider() throws IOException {
        HashMap<String, Object> configMap = new HashMap<String, Object>() {{
            put("maxSqlQueryInputLength", 30720);
            put("maxJoinsPerSqlQuery", 5);
            put("maxLogicalAndPerSqlQuery", 200);
            put("maxLogicalOrPerSqlQuery", 200);
            put("maxUdfRefPerSqlQuery", 2);
            put("maxInExpressionItemsCount", 8000);
            put("sqlAllowTop", true);
            put("sqlAllowSubQuery", false);
            put("allowNewKeywords", true);
            put("enableSpatialIndexing", true);
            put("maxSpatialQueryCells", 12);
            put("spatialMaxGeometryPointCount", 256);
        }};

        String configJsonString = Utils.getSimpleObjectMapper().writeValueAsString(configMap);
        long serviceProviderHandler = ServiceJNIWrapper.createServiceProvider(configJsonString);
        Assert.assertNotEquals(0, serviceProviderHandler);

        return serviceProviderHandler;
    }

    @Test
    public void testServiceJNIWrapper_Simple() throws IOException, NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        long serviceProviderHandler = getServiceProvider();
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        partitionKeyDefinition.setPaths(Arrays.asList(new String[] { "/field_0" }));
        partitionKeyDefinition.setKind(PartitionKind.Hash);

        Map<String, String> testCases = new HashMap<String, String>() {{
            put(
                    "SELECT * FROM usertable",
                    "{\"queryInfo\":{\"orderBy\":[],\"rewrittenQuery\":\"\"},\"queryRanges\":[{\"min\":[]," +
                            "\"max\":\"Infinity\",\"isMinInclusive\":true,\"isMaxInclusive\":false}]}"
                    );
            put(
                    "SELECT * FROM usertable u WHERE u.field_0 = \"16\"",
                    "{\"queryInfo\":{\"orderBy\":[],\"rewrittenQuery\":\"\"},\"queryRanges\":[{\"min\":[\"16\"]," +
                            "\"max\":[\"16\"],\"isMinInclusive\":true,\"isMaxInclusive\":true}]}"
            );
            put(
                    "SELECT * FROM usertable u ORDER BY u.field_0",
                    "{\"queryInfo\":{\"orderBy\":[\"Ascending\"],\"rewrittenQuery\":\"SELECT [{\\\"item\\\": u.field_0}] " +
                            "AS orderByItems, u AS payload\\r\\nFROM usertable AS u\\r\\nORDER BY u.field_0\"}," +
                            "\"queryRanges\":[{\"min\":[],\"max\":\"Infinity\",\"isMinInclusive\":true," +
                            "\"isMaxInclusive\":false}]}"
            );
            put(
                    "SELECT TOP 5 * FROM usertable u ORDER BY u.field_0",
                    "{\"queryInfo\":{\"top\":5,\"orderBy\":[\"Ascending\"],\"rewrittenQuery\":" +
                            "\"SELECT TOP 5 [{\\\"item\\\": u.field_0}] AS orderByItems, u AS payload\\r\\n" +
                            "FROM usertable AS u\\r\\nORDER BY u.field_0\"},\"queryRanges\":[{\"min\":[],\"max\":\"Infinity\"," +
                            "\"isMinInclusive\":true,\"isMaxInclusive\":false}]}"
            );
            put(
                    "SELECT TOP 5 * FROM usertable u WHERE u.field_0 >= \"user311117098782091729\"",
                    "{\"queryInfo\":{\"top\":5,\"orderBy\":[],\"rewrittenQuery\":\"\"},\"queryRanges\":" +
                            "[{\"min\":[],\"max\":\"Infinity\",\"isMinInclusive\":true,\"isMaxInclusive\":false}]}"
            );
        }};

        SqlQuerySpec querySpec;
        String serializedPartitionKeyRanges = null;
        Method getSerializedPartitionKeyRangesFromQueryMethod = ServiceJNIWrapper.class.getDeclaredMethod(
                "getSerializedPartitionKeyRangesFromQuery",
                long.class,
                SqlQuerySpec.class,
                PartitionKeyDefinition.class);
        getSerializedPartitionKeyRangesFromQueryMethod.setAccessible(true);
        PartitionedQueryExecutionInfoInternal partitionedQueryExecutionInfoInternal;
        for (Map.Entry<String, String> test : testCases.entrySet()) {
            System.out.println(String.format("Testing query: %s", test.getKey()));

            querySpec = new SqlQuerySpec(test.getKey());
            serializedPartitionKeyRanges = (String) getSerializedPartitionKeyRangesFromQueryMethod.invoke(null,
                    serviceProviderHandler,
                    querySpec,
                    partitionKeyDefinition
            );
            Assert.assertEquals(test.getValue(), serializedPartitionKeyRanges);

            partitionedQueryExecutionInfoInternal = Utils.getSimpleObjectMapper().readValue(
                    serializedPartitionKeyRanges,
                    PartitionedQueryExecutionInfoInternal.class);
            Assert.assertNotNull(partitionedQueryExecutionInfoInternal);
        }

        ServiceJNIWrapper.releaseServiceProvider(serviceProviderHandler);
    }

    @Test
    public void testServiceJNIWrapper_InvalidArgument() throws IOException {
        long serviceProviderHandler = getServiceProvider();

        SqlQuerySpec querySpec = new SqlQuerySpec("{\"query\":\"SELECT * FROM usertable\"}");
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        try {
            ServiceJNIWrapper.getPartitionKeyRangesFromQuery(
                    serviceProviderHandler,
                    querySpec,
                    partitionKeyDefinition
            );
            ServiceJNIWrapper.releaseServiceProvider(serviceProviderHandler);
            Assert.fail("Should throw Invalid Argument exception");
        } catch (IllegalArgumentException e) {
            // The exception is expected
        }
    }

    @Test
    public void testServiceJNIWrapper_NullPointer() throws IOException {
        try {
            ServiceJNIWrapper.createServiceProvider(null);
            Assert.fail("Should throw NullPointerException when using null to create service provider.");
        } catch (NullPointerException e) {
            // The exception is expected
        }

        long serviceProviderHandler = getServiceProvider();
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        try {
            ServiceJNIWrapper.getPartitionKeyRangesFromQuery(
                    serviceProviderHandler,
                    null,
                    partitionKeyDefinition
            );
            ServiceJNIWrapper.releaseServiceProvider(serviceProviderHandler);
            Assert.fail("Should throw NullPointerException when using null to get query execution info.");
        } catch (IllegalArgumentException e) {
            // The exception is expected
        }
    }

    @Test
    public void testServiceJNIWrapper_SmallInitialBuffer()
            throws IOException, NoSuchFieldException, IllegalAccessException, NoSuchMethodException, InvocationTargetException {
        Field initialBufferSizeField = ServiceJNIWrapper.class.getDeclaredField("INITIAL_BUFFER_SIZE");
        initialBufferSizeField.setAccessible(true);
        initialBufferSizeField.set(null, 1);
        testServiceJNIWrapper_Simple();
    }

    @Test
    public void testServiceJNIWrapper_InvalidQuery() throws IOException {
        Map<SqlQuerySpec, String> testCases = new HashMap<SqlQuerySpec, String>() {{
            put(
                    new SqlQuerySpec("SELECT udf.func1(r.a), udf.func2(r.b), udf.func3(r.c), udf.func1(r.d), " +
                            "udf.func2(r.e), udf.func3(r.f) FROM Root r"),
                    "{\"errors\":[{\"severity\":\"Error\",\"location\":{\"start\":0,\"end\":113},\"code\":\"SC3005\"," +
                            "\"message\":\"The SQL query exceeded the maximum number of user-defined function calls. " +
                            "The allowed limit is 2.\"}]}");
            put(
                    new SqlQuerySpec("SELECT * FROM Root r WHERE STARTSWITH(r.key, 'a', 'b')"),
                    "{\"errors\":[{\"severity\":\"Error\",\"location\":{\"start\":27,\"end\":54},\"code\":\"SC2050\"," +
                            "\"message\":\"The STARTSWITH function requires 2 argument(s).\"}]}"
            );
            put(
                    new SqlQuerySpec("SELECT * FROM root WHERE root.key = \"key\" ORDER BY LOWER(root.field) ASC"),
                    "{\"errors\":[{\"severity\":\"Error\",\"message\":\"Unsupported ORDER BY clause. " +
                            "ORDER BY item expression could not be mapped to a document path.\"}]}"
            );
        }};

        long serviceProviderHandler = getServiceProvider();
        PartitionKeyDefinition partitionKeyDefinition = new PartitionKeyDefinition();
        partitionKeyDefinition.setPaths(Arrays.asList(new String[] { "/key" }));
        partitionKeyDefinition.setKind(PartitionKind.Hash);

        for (Map.Entry<SqlQuerySpec, String> entry : testCases.entrySet()) {
            try {
                System.out.println(String.format("Testing invalid query: %s", entry.getKey().toString()));
                ServiceJNIWrapper.getPartitionKeyRangesFromQuery(
                        serviceProviderHandler,
                        entry.getKey(),
                        partitionKeyDefinition
                );
                Assert.fail("Should have gotten an exception due to invalid query");
            } catch (Exception e) {
                Assert.assertEquals(IllegalArgumentException.class, e.getClass());
                Assert.assertEquals(entry.getValue(), e.getMessage());
            }
        }
    }
}
