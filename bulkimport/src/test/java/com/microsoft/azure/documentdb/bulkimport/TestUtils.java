/**
 * The MIT License (MIT)
 * Copyright (c) 2017 Microsoft Corporation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.microsoft.azure.documentdb.bulkimport;

import static org.mockito.Mockito.when;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import org.mockito.Mockito;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.StoredProcedureResponse;
import com.microsoft.azure.documentdb.internal.DocumentServiceResponse;
import com.microsoft.azure.documentdb.internal.HttpConstants;

public class TestUtils {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static DocumentClientException getThrottleException() {
        DocumentClientException e = Mockito.mock(DocumentClientException.class);
        when(e.getStatusCode()).thenReturn(429);
        when(e.getRetryAfterInMilliseconds()).thenReturn(1l);

        return e;
    }

    public static Map<String, String> withRequestCharge(Map<String, String> headerResponse, double requestCharge) {
        if (headerResponse == null) {
            headerResponse = new HashMap<>();
        }

        headerResponse.put(HttpConstants.HttpHeaders.REQUEST_CHARGE, Double.toString(requestCharge));
        return headerResponse;
    }

    public static BulkImportStoredProcedureResponse getBulkImportStoredProcedureResponse(int count, int error) {
        BulkImportStoredProcedureResponse bispr = new BulkImportStoredProcedureResponse();
        bispr.count = count;
        bispr.errorCode = error;
        return bispr;
    }

    public static StoredProcedureResponse getStoredProcedureResponse(
            BulkImportStoredProcedureResponse bulkImportResponse, Map<String, String> headerResponse)
                    throws JsonProcessingException, InstantiationException, IllegalAccessException, IllegalArgumentException,
                    InvocationTargetException, NoSuchMethodException, SecurityException {

        DocumentServiceResponse documentServiceResponse = Mockito.mock(DocumentServiceResponse.class);
        when(documentServiceResponse.getReponseBodyAsString()).thenReturn(MAPPER.writeValueAsString(bulkImportResponse));
        when(documentServiceResponse.getResponseHeaders()).thenReturn(headerResponse);

        Constructor<StoredProcedureResponse> constructor = StoredProcedureResponse.class
                .getDeclaredConstructor(DocumentServiceResponse.class);
        constructor.setAccessible(true);

        StoredProcedureResponse storedProcedureResponse = constructor.newInstance(documentServiceResponse);

        return storedProcedureResponse;
    }
}
