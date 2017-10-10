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

import static com.microsoft.azure.documentdb.bulkimport.TestUtils.getBulkImportStoredProcedureResponse;
import static com.microsoft.azure.documentdb.bulkimport.TestUtils.getStoredProcedureResponse;
import static com.microsoft.azure.documentdb.bulkimport.TestUtils.getThrottleException;
import static com.microsoft.azure.documentdb.bulkimport.TestUtils.withRequestCharge;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterators;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.DocumentClientException;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.StoredProcedureResponse;

public class BatchInserterTests {


    @Test
    public void callbackCount() {
        BulkImportStoredProcedureOptions options = null;
        String bulkImportSproc = null;
        DocumentClient client = Mockito.mock(DocumentClient.class);
        List<List<String>> batchesToInsert = new ArrayList<>();
        batchesToInsert.add(new ArrayList<>());
        batchesToInsert.add(new ArrayList<>());
        batchesToInsert.add(new ArrayList<>());

        String partitionIndex = "0";
        BatchInserter bi = new BatchInserter(partitionIndex, batchesToInsert, client, bulkImportSproc, options);

        Iterator<Callable<InsertMetrics>> callbackIterator = bi.miniBatchInsertExecutionCallableIterator();

        List<Callable<InsertMetrics>> list = new ArrayList<>();
        Iterators.addAll(list, callbackIterator);

        assertThat(list.size(), equalTo(3));
    }

    @Test
    public void simple() throws Exception {

        BulkImportStoredProcedureOptions options = null;
        String bulkImportSproc = null;
        DocumentClient client = Mockito.mock(DocumentClient.class);
        List<List<String>> batchesToInsert = new ArrayList<>();
        batchesToInsert.add(new ArrayList<>());

        int numberOfDocuments = 10;
        for (int i = 0; i < numberOfDocuments; i++) {
            batchesToInsert.get(0).add("{}");
        }

        String partitionIndex = "0";
        BatchInserter bi = new BatchInserter(partitionIndex, batchesToInsert, client, bulkImportSproc, options);

        Iterator<Callable<InsertMetrics>> callbackIterator = bi.miniBatchInsertExecutionCallableIterator();

        List<Callable<InsertMetrics>> list = new ArrayList<>();
        Iterators.addAll(list, callbackIterator);

        assertThat(list.size(), equalTo(1));

        Callable<InsertMetrics> callable = list.get(0);

        Map<String, String> headers = withRequestCharge(null, 5.5);

        StoredProcedureResponse bulkImportResponse = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(numberOfDocuments, 0), headers);

        when(client.executeStoredProcedure(Mockito.any(String.class), Mockito.any(RequestOptions.class),
                Mockito.any(Object[].class))).thenReturn(bulkImportResponse);

        InsertMetrics metrics = callable.call();

        verify(client, Mockito.times(1)).executeStoredProcedure(Mockito.anyString(), Mockito.any(RequestOptions.class), Mockito.any(Object[].class));

        // verify all documents inserted
        assertThat(metrics.numberOfDocumentsInserted, equalTo(10l));
        // verify no throttle
        assertThat(metrics.numberOfThrottles, equalTo(0l));
        // verify request charge
        assertThat(metrics.requestUnitsConsumed, equalTo(5.5));
        // verify non zero total time
        assertThat(metrics.timeTaken.getNano() > 0, equalTo(true));

        assertThat(bi.getNumberOfDocumentsImported(), equalTo((int) metrics.numberOfDocumentsInserted));
        assertThat(bi.getTotalRequestUnitsConsumed(), equalTo(metrics.requestUnitsConsumed));
    }

    @Test
    public void partialProgress() throws Exception {

        BulkImportStoredProcedureOptions options = null;
        String bulkImportSproc = null;
        DocumentClient client = Mockito.mock(DocumentClient.class);
        List<List<String>> batchesToInsert = new ArrayList<>();
        batchesToInsert.add(new ArrayList<>());

        int numberOfDocuments = 10;
        for (int i = 0; i < numberOfDocuments; i++) {
            batchesToInsert.get(0).add("{}");
        }

        String partitionIndex = "0";
        BatchInserter bi = new BatchInserter(partitionIndex, batchesToInsert, client, bulkImportSproc, options);

        Iterator<Callable<InsertMetrics>> callbackIterator = bi.miniBatchInsertExecutionCallableIterator();

        List<Callable<InsertMetrics>> list = new ArrayList<>();
        Iterators.addAll(list, callbackIterator);

        assertThat(list.size(), equalTo(1));

        Callable<InsertMetrics> callable = list.get(0);

        // 3 documents progress
        StoredProcedureResponse bulkImportResponse1 = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(3, 1),  withRequestCharge(null, 1.5));
        // 1 document progress
        StoredProcedureResponse bulkImportResponse2 = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(1, 1),  withRequestCharge(null, 2.5));
        // 0 document progress
        StoredProcedureResponse bulkImportResponse3 = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(0, 1),  withRequestCharge(null, 3.5));
        // 6 document progress
        StoredProcedureResponse bulkImportResponse4 = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(6, 1),  withRequestCharge(null, 4.5));

        when(client.executeStoredProcedure(Mockito.any(String.class), Mockito.any(RequestOptions.class),
                Mockito.any(Object[].class)))
        .thenReturn(bulkImportResponse1)
        .thenReturn(bulkImportResponse2)
        .thenReturn(bulkImportResponse3)
        .thenReturn(bulkImportResponse4);

        InsertMetrics metrics = callable.call();

        verify(client, Mockito.times(4)).executeStoredProcedure(Mockito.anyString(), Mockito.any(RequestOptions.class), Mockito.any(Object[].class));

        // verify all documents inserted
        assertThat(metrics.numberOfDocumentsInserted, equalTo(10l));
        // verify no throttle
        assertThat(metrics.numberOfThrottles, equalTo(0l));
        // verify request charge
        assertThat(metrics.requestUnitsConsumed, equalTo(1.5+ 2.5 + 3.5 + 4.5));
        // verify non zero total time
        assertThat(metrics.timeTaken.getNano() > 0, equalTo(true));

        assertThat(bi.getNumberOfDocumentsImported(), equalTo((int) metrics.numberOfDocumentsInserted));
        assertThat(bi.getTotalRequestUnitsConsumed(), equalTo(metrics.requestUnitsConsumed));
    }

    @Test
    public void throttle() throws Exception {

        BulkImportStoredProcedureOptions options = new BulkImportStoredProcedureOptions(true, true, "fakeCollectionId", true);

        String bulkImportSproc = null;
        DocumentClient client = Mockito.mock(DocumentClient.class);
        List<List<String>> batchesToInsert = new ArrayList<>();
        batchesToInsert.add(new ArrayList<>());

        int numberOfDocuments = 10;
        for (int i = 0; i < numberOfDocuments; i++) {
            batchesToInsert.get(0).add("{}");
        }

        String partitionIndex = "0";
        BatchInserter bi = new BatchInserter(partitionIndex, batchesToInsert, client, bulkImportSproc, options);

        Iterator<Callable<InsertMetrics>> callbackIterator = bi.miniBatchInsertExecutionCallableIterator();

        List<Callable<InsertMetrics>> list = new ArrayList<>();
        Iterators.addAll(list, callbackIterator);

        assertThat(list.size(), equalTo(1));

        Callable<InsertMetrics> callable = list.get(0);

        Map<String, String> headers = withRequestCharge(null, 5.5);

        StoredProcedureResponse bulkImportResponse = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(numberOfDocuments, 0), headers);

        DocumentClientException throttleException = getThrottleException();

        when(client.executeStoredProcedure(Mockito.any(String.class), Mockito.any(RequestOptions.class),
                Mockito.any(Object[].class))).thenThrow(throttleException).thenReturn(bulkImportResponse);

        InsertMetrics metrics = callable.call();

        // verify that execute stored proc is invoked twice (once with throttle and second time without throttle)
        verify(client, Mockito.times(2)).executeStoredProcedure(Mockito.anyString(), Mockito.any(RequestOptions.class), Mockito.any(Object[].class));

        // verify all documents inserted
        assertThat(metrics.numberOfDocumentsInserted, equalTo((long) numberOfDocuments));
        // verify one throttle
        assertThat(metrics.numberOfThrottles, equalTo(1l));
        // verify request charge
        assertThat(metrics.requestUnitsConsumed, equalTo(5.5));
        // verify non zero total time
        assertThat(metrics.timeTaken.getNano() > 0, equalTo(true));

        assertThat(bi.getNumberOfDocumentsImported(), equalTo((int) metrics.numberOfDocumentsInserted));
        assertThat(bi.getTotalRequestUnitsConsumed(), equalTo(metrics.requestUnitsConsumed));
    }

}
