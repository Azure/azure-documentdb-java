package com.microsoft.azure.documentdb.internal;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.azure.documentdb.PartitionKeyDefinition;
import com.microsoft.azure.documentdb.PathParser;
import com.microsoft.azure.documentdb.SqlQuerySpec;
import com.microsoft.azure.documentdb.internal.query.PartitionedQueryExecutionInfoInternal;

/**
 * This class is used internally within the SDK to provide interop functionality layer with the native assembly.
 * When available, it can help improving queries latency by derive query execution information locally instead of
 * going through gateway.
 */
public class ServiceJNIWrapper {
    private static final long E_POINTER = 0x80004003;
    private static final long E_INVALIDARG = 0x80070057;
    private static final long E_UNEXPECTED = 0x8000FFFF;
    private static final long DISP_E_BUFFERTOOSMALL = 0x80020013;
    private static final long E_INVALID_PAYLOAD = 0x800a0b00;
    private static final long S_OK = 0x0;
    private static final long S_FALSE = 0x1;

    private static int INITIAL_BUFFER_SIZE = 1024;
    private static String LIBRARY_NAME = "Microsoft.Azure.Documents.ServiceInterop";

    private static ServiceJNIWrapper instance = new ServiceJNIWrapper();
    private static boolean isJNILoaded = false;
    private static String systemArchitecture = System.getProperty("os.arch");
    private static String osName = System.getProperty("os.name");

    private native long CreateServiceProvider(String configJsonString, LongWrapper serviceProviderPointer);
    private native long ReleaseServiceProvider(long serviceProviderPointer);
    private native long GetPartitionKeyRangesFromQuery(
            long serviceProviderPointer,
            String querySpec,
            String[] partitionKeyDefinitionPathTokens,
            int[] partitionKeyDefinitionPathTokenLengths,
            int partitionKeyDefinitionPathCount,
            int partitionKind,
            StringBuilder serializedQueryExecutionInfoBuffer,
            int serializedQueryExecutionInfoBufferLength,
            IntWrapper serializedPartitionedQueryExecutionInfoResultLength
    );

    private static void throwIfFailed(long hResult, StringBuilder serializedQueryExecutionInfo) {
        if (hResult == E_POINTER) {
            throw new NullPointerException("Null reference in the JNI wrapper");
        } else if (hResult == E_INVALIDARG) {
            throw new IllegalArgumentException("Invalid argument in the JNI wrapper");
        } else if (hResult == E_UNEXPECTED) {
            throw new IllegalStateException("Unexpected error in the JNI wrapper");
        } else if (hResult == E_INVALID_PAYLOAD) {
            throw new IllegalArgumentException(serializedQueryExecutionInfo.toString());
        } else if (hResult != S_OK && hResult != S_FALSE) {
            throw new IllegalStateException(String.format("Error occurred in the JNI 0x%s", Long.toHexString(hResult)));
        }
    }

    /**
     * Used internally within the SDK to get an instance of ServiceJNIWrapper to invoke native methods.
     * @return an instance of ServiceJNIWrapper
     */
    private static ServiceJNIWrapper getInstance() {
        if (!ServiceJNIWrapper.isJNILoaded) {
            throw new UnsupportedOperationException(String.format("Unable to load %s.", LIBRARY_NAME));
        }
        return ServiceJNIWrapper.instance;
    }

    static {
        loadServiceJNI();
    }

    private static boolean isSupportedPlatform() {
        return osName != null && osName.toLowerCase().indexOf("win") >= 0
                && systemArchitecture != null && systemArchitecture.toLowerCase().indexOf("64") >= 0;
    }

    private static void loadServiceJNI() {
        if (isSupportedPlatform()) {
            try {
                System.loadLibrary(LIBRARY_NAME);
                isJNILoaded = true;
            } catch (UnsatisfiedLinkError e) {
                Logger.getGlobal().log(Level.WARNING, String.format("Unable to find %s.", LIBRARY_NAME));
            }
        } else {
            Logger.getGlobal().log(Level.WARNING,
                    String.format("'%s' with '%s' system is not compatible with native library. JNI not loaded.",
                            osName, systemArchitecture));
            isJNILoaded = false;
        }
    }

    /**
     * Used to check if the native assembly exits for current platform and can be used
     * @return a boolean indicate whether the ServiceJNI is available or not
     */
    public static boolean isServiceJNIAvailable() {
        return ServiceJNIWrapper.isJNILoaded;
    }


    /**
     * Used internally within the SDK. Create a service provider using a configuration string.
     *
     * @param configJsonString a JSON string represents the service configuration
     * @return                 a long integer represents the handler of the service
     */
    public static long createServiceProvider(String configJsonString) {
        LongWrapper serviceProviderPointer = new LongWrapper();
        long hResult = ServiceJNIWrapper.getInstance().CreateServiceProvider(configJsonString, serviceProviderPointer);
        ServiceJNIWrapper.throwIfFailed(hResult, null);

        return serviceProviderPointer.getValue();
    }

    /**
     * Used internally within the SDK. Release a service provider.
     *
     * @param serviceProviderPointer a long integer represents the handler of the service
     */
    public static void releaseServiceProvider(long serviceProviderPointer) {
        long hResult = ServiceJNIWrapper.getInstance().ReleaseServiceProvider(serviceProviderPointer);
        throwIfFailed(hResult, null);
    }

    private static String getSerializedPartitionKeyRangesFromQuery(
        long serviceProviderPointer,
        SqlQuerySpec querySpec,
        PartitionKeyDefinition partitionKeyDefinition) {
        if (querySpec == null) {
            throw new IllegalArgumentException("querySpec");
        }

        if (partitionKeyDefinition == null || partitionKeyDefinition.getPaths() == null) {
            throw new IllegalArgumentException("partitionKeyDefinition");
        }

        StringBuilder serializedQueryExecutionInfoBuffer = new StringBuilder();

        // Get path tokens and length
        List<String> tokensList = new ArrayList<>();
        int[] tokenLengths = new int[partitionKeyDefinition.getPaths().size()];
        int index = 0;
        for (String path : partitionKeyDefinition.getPaths()) {
            Collection<String> parts = PathParser.getPathParts(path);
            tokensList.addAll(parts);
            tokenLengths[index++] = parts.size();
        };

        // Get query text
        String queryText = querySpec.toString();

        serializedQueryExecutionInfoBuffer.setLength(0);
        serializedQueryExecutionInfoBuffer.ensureCapacity(INITIAL_BUFFER_SIZE);
        IntWrapper serializedPartitionedQueryExecutionInfoResultLength = new IntWrapper();
        int partitionKind = partitionKeyDefinition.getKind().ordinal();
        long hResult = ServiceJNIWrapper.getInstance().GetPartitionKeyRangesFromQuery(
                serviceProviderPointer,
                queryText,
                tokensList.toArray(new String[0]),
                tokenLengths,
                partitionKeyDefinition.getPaths().size(),
                partitionKind,
                serializedQueryExecutionInfoBuffer,
                serializedQueryExecutionInfoBuffer.capacity(),
                serializedPartitionedQueryExecutionInfoResultLength
        );

        if(hResult == DISP_E_BUFFERTOOSMALL) {
            serializedQueryExecutionInfoBuffer.setLength(0);
            serializedQueryExecutionInfoBuffer.ensureCapacity(serializedPartitionedQueryExecutionInfoResultLength.getValue());

            hResult = ServiceJNIWrapper.getInstance().GetPartitionKeyRangesFromQuery(
                    serviceProviderPointer,
                    queryText,
                    tokensList.toArray(new String[0]),
                    tokenLengths,
                    partitionKeyDefinition.getPaths().size(),
                    partitionKind,
                    serializedQueryExecutionInfoBuffer,
                    serializedQueryExecutionInfoBuffer.capacity(),
                    serializedPartitionedQueryExecutionInfoResultLength
            );
        }

        ServiceJNIWrapper.throwIfFailed(hResult, serializedQueryExecutionInfoBuffer);

        return serializedQueryExecutionInfoBuffer.toString();
    }

    /**
     * Used internally within the SDK. Get the query execution information for a query.
     *
     * @param serviceProviderPointer a long integer represents the handler of the service
     * @param querySpec              an SqlQuerySpec represents the query
     * @param partitionKeyDefinition the collection partition key definition
     * @return                       the PartitionQueryExecutionInfoInternal instance
     */
    public static PartitionedQueryExecutionInfoInternal getPartitionKeyRangesFromQuery(
            long serviceProviderPointer,
            SqlQuerySpec querySpec,
            PartitionKeyDefinition partitionKeyDefinition) {
        String serializedPartitionKeyRangesFromQuery = ServiceJNIWrapper.getSerializedPartitionKeyRangesFromQuery(
                serviceProviderPointer,
                querySpec,
                partitionKeyDefinition
        );

        PartitionedQueryExecutionInfoInternal partitionedQueryExecutionInfoInternal = null;
        try {
            partitionedQueryExecutionInfoInternal = Utils.getSimpleObjectMapper().readValue(
                    serializedPartitionKeyRangesFromQuery,
                    PartitionedQueryExecutionInfoInternal.class);
        } catch (IOException e) {
            throw new IllegalStateException(String.format("Unable to deserialize partition query execution information: '%s'",
                    serializedPartitionKeyRangesFromQuery));
        }

        return partitionedQueryExecutionInfoInternal;
    }

    /**
     * Wrapper for long value which acts as out parameter in JNI native call
     */
    private static class LongWrapper {
        private long value;

        public long getValue() {
            return value;
        }

        public void setValue(long value) {
            this.value = value;
        }
    }

    /**
     * Wrapper for int value which acts as out parameter in JNI native call
     */
    private static class IntWrapper {
        private int value;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }
    }
}
