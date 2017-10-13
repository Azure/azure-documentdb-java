/**
 * The MIT License (MIT)
 * Copyright (c) 2016 Microsoft Corporation
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
import static com.microsoft.azure.documentdb.bulkimport.TestUtils.withRequestCharge;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.microsoft.azure.documentdb.DocumentClient;
import com.microsoft.azure.documentdb.RequestOptions;
import com.microsoft.azure.documentdb.StoredProcedureResponse;

public class CongestionControllerTests {

    private ListeningExecutorService listeningExecutorService;
    private static final int TIMEOUT = 5000;


    @Before
    public void setUp() {
        listeningExecutorService = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }

    @After
    public void shutDown() {
        listeningExecutorService.shutdown();
    }

    @Test(timeout = TIMEOUT)
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

        String paritionKeyRangeId = "0";
        BatchInserter bi = new BatchInserter(paritionKeyRangeId, batchesToInsert, client, bulkImportSproc, options);

        Iterator<Callable<InsertMetrics>> callbackIterator = bi.miniBatchInsertExecutionCallableIterator();

        List<Callable<InsertMetrics>> list = new ArrayList<>();
        Iterators.addAll(list, callbackIterator);

        assertThat(list.size(), equalTo(1));

        Map<String, String> headers = withRequestCharge(null, 5.5);

        StoredProcedureResponse bulkImportResponse = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(numberOfDocuments, 0), headers);

        when(client.executeStoredProcedure(Mockito.any(String.class), Mockito.any(RequestOptions.class),
                Mockito.any(Object[].class))).thenReturn(bulkImportResponse);

        CongestionController cc = new CongestionController(listeningExecutorService, 10000, paritionKeyRangeId, bi);
        ListenableFuture<Void> listenableFuture = cc.executeAllAsync();

        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean success = new AtomicBoolean(false);
        Runnable listener = new Runnable() {

            @Override
            public void run() {

                try {
                    assertThat(cc.completed(), equalTo(true));
                    assertThat(cc.failed(), equalTo(false));
                    assertThat(cc.isRunning(), equalTo(false));

                    assertThat(cc.isRunning(), equalTo(false));

                    assertThat(bi.numberOfDocumentsImported.get(), equalTo(numberOfDocuments));
                    success.set(true);
                } finally {
                    latch.countDown();
                }
            }
        };

        listenableFuture.addListener(listener, listeningExecutorService);

        latch.await();
        assertThat(success.get(), equalTo(true));
    }

    @Test(timeout = TIMEOUT)
    public void multipleRounds() throws Exception {

        BulkImportStoredProcedureOptions options = null;
        String bulkImportSproc = null;
        DocumentClient client = Mockito.mock(DocumentClient.class);
        List<List<String>> batchesToInsert = new ArrayList<>();

        batchesToInsert.add(new ArrayList<>());
        int numberOfDocumentsInFirstBatch = 10;
        for (int i = 0; i < numberOfDocumentsInFirstBatch; i++) {
            batchesToInsert.get(0).add("{}");
        }

        int numberOfDocumentsInSecondBatch = 10;
        batchesToInsert.add(new ArrayList<>());
        for (int i = 0; i < numberOfDocumentsInSecondBatch; i++) {
            batchesToInsert.get(1).add("{}");
        }

        String paritionKeyRangeId = "0";
        BatchInserter bi = new BatchInserter(paritionKeyRangeId, batchesToInsert, client, bulkImportSproc, options);

        Iterator<Callable<InsertMetrics>> callbackIterator = bi.miniBatchInsertExecutionCallableIterator();

        List<Callable<InsertMetrics>> list = new ArrayList<>();
        Iterators.addAll(list, callbackIterator);

        assertThat(list.size(), equalTo(2));


        StoredProcedureResponse bulkImportResponse1 = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(numberOfDocumentsInFirstBatch, 0), withRequestCharge(null, 1.5));
        StoredProcedureResponse bulkImportResponse2 = getStoredProcedureResponse(getBulkImportStoredProcedureResponse(numberOfDocumentsInSecondBatch, 0), withRequestCharge(null, 4.1));

        when(client.executeStoredProcedure(Mockito.any(String.class), Mockito.any(RequestOptions.class),
                Mockito.any(Object[].class))).thenReturn(bulkImportResponse1).thenReturn(bulkImportResponse2);

        CongestionController cc = new CongestionController(listeningExecutorService, 10000, paritionKeyRangeId, bi);
        ListenableFuture<Void> listenableFuture = cc.executeAllAsync();

        CountDownLatch latch = new CountDownLatch(1);

        AtomicBoolean success = new AtomicBoolean(false);
        Runnable listener = new Runnable() {

            @Override
            public void run() {

                try {
                    assertThat(cc.completed(), equalTo(true));
                    assertThat(cc.failed(), equalTo(false));
                    assertThat(cc.isRunning(), equalTo(false));

                    assertThat(cc.isRunning(), equalTo(false));

                    assertThat(bi.numberOfDocumentsImported.get(), equalTo(numberOfDocumentsInFirstBatch + numberOfDocumentsInSecondBatch));
                    assertThat(bi.totalRequestUnitsConsumed.get(), equalTo(1.5 + 4.1));

                    success.set(true);
                } finally {
                    latch.countDown();
                }
            }
        };

        listenableFuture.addListener(listener, listeningExecutorService);

        latch.await();
        assertThat(success.get(), equalTo(true));
    }

}
