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

import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

class CongestionController {

    private final Logger logger = LoggerFactory.getLogger(CongestionController.class);

    /**
     * The degree of concurrency to start at.
     */
    private static final int STARTING_DEGREE_OF_CONCURRENCY = 2;

    /**
     * The maximum degree of concurrency to go upto for a single physical partition.
     */
    private static final int MAX_DEGREE_OF_CONCURRENCY = 20;

    /**
     * The congestion controller uses AIMD (additive increase / multiplicative decrease) and this is the additive increase factor.
     * If we don't receive a throttle in the sample period, then we increase the degree of concurrency by this amount.
     *  For example if this is set to 3 and we need to increase the degree of concurrency then "degreeOfConcurrency += 3".
     */
    private static final int ADDITIVE_INCREASE_FACTOR = 1;

    /**
     * The congestion controller uses AIMD (additive increase / multiplicative decrease) and this is the multiplicative decrease factor.
     * If we receive a throttle in the sample period, then we decrease the degree of concurrency by this amount.
     * For example if this is set to 2 and we need to decrease the degree of concurrency then "degreeOfConcurrency /= 2".
     */
    private static final int MULTIPLICATIVE_DECREASE_FACTOR = 2;

    /**
     * The threshold to grow to.
     * For example if this is set to .9 and the collection has 10k RU, then the code will keep increasing the degree of concurrency until we hit .9 * 10k = 9k RU.
     */
    private static final double THROUGHPUT_THRESHOLD  = 0.9;

    /**
     * The id of the physical partition that this congestion controller is responsible for.
     */
    private final String partitionKeyRangeId;

    /**
     * This determines how often the code will sample the InsertMetrics and check to see if the degree of concurrency needs to be changed.
     */
    private final Duration samplePeriod = Duration.ofSeconds(3);

    /**
     * The {@link BatchInserter} that exposes a stream of {@link Callable} that insert document batches and returns an {@link InsertMetrics}
     */
    private final BatchInserter batchInserter;

    /**
     * The semaphore that throttles the BatchInserter tasks by only allowing at most 'degreeOfConcurrency' to run at a time.
     */
    private final Semaphore throttleSemaphore;

    /**
     * The last snapshot of the aggregatedInsertMetrics that gets atomically replaced by a task that monitors for congestion,
     * while all the task add to this using a lock.
     */
    private InsertMetrics aggregatedInsertMetrics;

    /**
     * Whether or not all the documents have been inserted.
     */
    private long documentsInsertedSoFar;

    static enum State {
        Running, Completed, Failure
    }

    private State state = State.Running;

    /**
     * The degree of concurrency (maximum number of tasks allowed to execute concurrently).
     */
    private int degreeOfConcurrency ;

    /**
     * executor service for running tasks.
     */
    private ListeningExecutorService executor;

    /**
     * Partition throughput
     */
    private int partitionThroughput;

    public CongestionController(ListeningExecutorService executor, int partitionThroughput, String partitionKeyRangeId, BatchInserter batchInserter) {
        this(executor, partitionThroughput, partitionKeyRangeId, batchInserter, null);
    }

    public CongestionController(ListeningExecutorService executor, int partitionThroughput, String partitionKeyRangeId, BatchInserter batchInserter, Integer startingDegreeOfConcurrency) {
        this.partitionKeyRangeId = partitionKeyRangeId;
        this.batchInserter = batchInserter;

        // Starting the semaphore with 'StartingDegreeOfConcurrency' count and will release when no throttles are received
        // and decrease when we get throttled.
        this.degreeOfConcurrency = startingDegreeOfConcurrency != null ? startingDegreeOfConcurrency: STARTING_DEGREE_OF_CONCURRENCY;
        this.throttleSemaphore = new Semaphore(this.degreeOfConcurrency);
        this.aggregatedInsertMetrics = new InsertMetrics();
        this.executor = executor;
        this.partitionThroughput = partitionThroughput;
    }

    private synchronized InsertMetrics atomicGetAndReplace(InsertMetrics metrics) {
        InsertMetrics old = this.aggregatedInsertMetrics;
        this.aggregatedInsertMetrics = metrics;
        return old;
    }

    private Callable<Void> congestionControlTask() {
        return new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                while (isRunning())
                {
                    try {
                        // TODO: FIXME I think semaphore may reach 0 here and if so that will create a deadlock
                        // verify and fix

                        logger.trace("partition key range id {} goes to sleep for {} seconds", partitionKeyRangeId, samplePeriod.getSeconds());
                        Thread.sleep(samplePeriod.getSeconds() * 1000);
                        logger.trace("{} wakes up", partitionKeyRangeId);

                        InsertMetrics insertMetricsSample = atomicGetAndReplace(new InsertMetrics());

                        if (insertMetricsSample.numberOfThrottles > 0) {
                            logger.trace("{} encountered {} throttling, reducing parallelism", partitionKeyRangeId, insertMetricsSample.numberOfThrottles);

                            // We got a throttle so we need to back off on the degree of concurrency.
                            // Get the current degree of concurrency and decrease that (AIMD).
                            logger.trace("{} encountered {} throttling", partitionKeyRangeId, insertMetricsSample.numberOfThrottles);

                            for (int i = 0; i < degreeOfConcurrency / MULTIPLICATIVE_DECREASE_FACTOR; i++) {
                                throttleSemaphore.acquire();
                            }

                            logger.trace("{} encountered {} throttling", partitionKeyRangeId, insertMetricsSample.numberOfThrottles);

                            degreeOfConcurrency -= (degreeOfConcurrency / MULTIPLICATIVE_DECREASE_FACTOR);
                        }

                        if (insertMetricsSample.numberOfDocumentsInserted == 0) {
                            // We haven't made any progress, since the last sampling
                            continue;
                        }

                        logger.trace("{} aggregating inserts metrics", partitionKeyRangeId);

                        if (insertMetricsSample.numberOfThrottles == 0) {
                            if ((insertMetricsSample.requestUnitsConsumed < THROUGHPUT_THRESHOLD * partitionThroughput) &&
                                    degreeOfConcurrency + ADDITIVE_INCREASE_FACTOR <= MAX_DEGREE_OF_CONCURRENCY) {
                                // We aren't getting throttles, so we should bump of the degree of concurrency (AIAD).
                                throttleSemaphore.release(ADDITIVE_INCREASE_FACTOR);
                                degreeOfConcurrency += ADDITIVE_INCREASE_FACTOR;
                            }
                        }

                        double ruPerSecond = insertMetricsSample.requestUnitsConsumed / samplePeriod.getSeconds();
                        documentsInsertedSoFar += insertMetricsSample.numberOfDocumentsInserted;

                        logger.info("Partition index {} : {} Inserted {} docs in {} seconds at {} RU/s with {} tasks. Faced {} throttles",
                                partitionKeyRangeId,
                                insertMetricsSample.numberOfThrottles,
                                degreeOfConcurrency,
                                ruPerSecond,
                                samplePeriod.getSeconds(),
                                insertMetricsSample.numberOfDocumentsInserted,
                                documentsInsertedSoFar
                                );

                    } catch (InterruptedException e) {
                        logger.warn("Interrupted", e);
                        break;
                    }

                }
                return null;
            };
        };
    }

    public ListenableFuture<Void> ExecuteAll()  {

        logger.debug("Executing batching in partition {}", partitionKeyRangeId);
        Iterator<Callable<InsertMetrics>> batchExecutionIterator = batchInserter.miniBatchInsertExecutionCallableIterator();

        List<ListenableFuture<InsertMetrics>> futureList = new ArrayList<>();
        while(batchExecutionIterator.hasNext()) {
            Callable<InsertMetrics> task = batchExecutionIterator.next();

            // Main thread waits on the throttleSem so no more than MaxDegreeOfParallelism Tasks are run at a time.
            try {
                logger.trace("trying to accequire semaphore");
                this.throttleSemaphore.acquire();
                logger.trace("semaphore accequired");
            } catch (InterruptedException e) {
                logger.error("Interrupted", e);
                throw new RuntimeException(e);
            }

            ListenableFuture<InsertMetrics> insertMetricsFuture = executor.submit(task);

            FutureCallback<InsertMetrics> aggregateMetricsReleaseSemaphoreCallback = new FutureCallback<InsertMetrics>() {

                @Override
                public void onSuccess(InsertMetrics result) {
                    synchronized (CongestionController.this) {
                        aggregatedInsertMetrics = InsertMetrics.sum(aggregatedInsertMetrics, result);
                    }
                    logger.trace("releasing semaphore");
                    throttleSemaphore.release();
                }

                @Override
                public void onFailure(Throwable t) {
                    logger.trace("encountered failure {} releasing semaphore", t);
                    throttleSemaphore.release();
                }
            };

            Futures.addCallback(insertMetricsFuture, aggregateMetricsReleaseSemaphoreCallback , MoreExecutors.directExecutor());
            futureList.add(insertMetricsFuture);
        }

        ListenableFuture<List<InsertMetrics>> allFutureResults = Futures.allAsList(futureList);

        FutureCallback<List<InsertMetrics>> completionCallback = new FutureCallback<List<InsertMetrics>>() {

            @Override
            public void onSuccess(List<InsertMetrics> result) {
                logger.info("Completed");
                getAndSet(State.Completed);
            }

            @Override
            public void onFailure(Throwable t) {
                logger.error("Encountered failure", t);
                getAndSet(State.Failure);
            }
        };

        Futures.addCallback(allFutureResults, completionCallback, MoreExecutors.directExecutor());
        return executor.submit(congestionControlTask());
    }

    public synchronized State getAndSet(State state) {
        State res = this.state;
        this.state = state;
        return res;
    }

    public synchronized boolean isRunning() {
        return state == State.Running;
    }

    public synchronized boolean hasCompletedAsSuccess() {
        return state == State.Completed;
    }

    public synchronized boolean hasCompletedAsFailure() {
        return state == State.Failure;
    }

    public int getDegreeOfConcurrency() {
        return this.degreeOfConcurrency;
    }
}
