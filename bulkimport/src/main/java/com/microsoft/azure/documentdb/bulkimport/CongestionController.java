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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.AsyncFunction;
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
	private static final int STARTING_DEGREE_OF_CONCURRENCY = 3;

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
	private static final int DIVISIVE_DECREASE_FACTOR = 2;

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
	private final Duration samplePeriod = Duration.ofSeconds(1);

	/**
	 * The {@link BatchOperator} that exposes a stream of {@link Callable} that insert document batches and returns an {@link InsertMetrics}
	 */
	private final BatchOperator batchOperator;

	/**
	 * The semaphore that throttles the BatchOperator tasks by only allowing at most 'degreeOfConcurrency' to run at a time.
	 */
	private final Semaphore throttleSemaphore;

	/**
	 * The last snapshot of the aggregatedInsertMetrics that gets atomically replaced by a task that monitors for congestion,
	 * while all the task add to this using a lock.
	 */
	private InsertMetrics aggregatedInsertMetrics;

	private Object aggregateLock = new Object();

	/**
	 * Whether or not all the documents have been operated on.
	 */
	private long documentsOperatedSoFar;

	static enum State {
		Running, Completed, Failure
	}

	private volatile State state = State.Running;

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

	/**
	 * captures failures which surface out
	 */
	private final List<Exception> failures = Collections.synchronizedList(new ArrayList<>());

	public CongestionController(ListeningExecutorService executor, int partitionThroughput, String partitionKeyRangeId,
			BatchOperator batchOperator) {
		this(executor, partitionThroughput, partitionKeyRangeId, batchOperator, null);
	}

	public CongestionController(ListeningExecutorService executor, int partitionThroughput, String partitionKeyRangeId,
			BatchOperator batchOperator, Integer startingDegreeOfConcurrency) {
		this.partitionKeyRangeId = partitionKeyRangeId;
		this.batchOperator = batchOperator;

		// Starting the semaphore with 'StartingDegreeOfConcurrency' count and will release when no throttles are received
		// and decrease when we get throttled.
		this.degreeOfConcurrency = startingDegreeOfConcurrency != null ? startingDegreeOfConcurrency: STARTING_DEGREE_OF_CONCURRENCY;
		this.throttleSemaphore = new Semaphore(this.degreeOfConcurrency);
		this.aggregatedInsertMetrics = new InsertMetrics();
		this.executor = executor;
		this.partitionThroughput = partitionThroughput;
	}

	private void addFailure(Exception e) {
		failures.add(e);
	}

	public List<Exception> getFailures() {
		return Collections.unmodifiableList(this.failures);
	}

	private InsertMetrics atomicGetAndReplace(InsertMetrics metrics) {
		synchronized (aggregateLock) {
			InsertMetrics old = this.aggregatedInsertMetrics;
			this.aggregatedInsertMetrics = metrics;
			return old;
		}
	}

	private Callable<Void> congestionControlTask() {
		return new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				while (isRunning()) {
					try {

						logger.debug("pki {} goes to sleep for {} seconds. availabel semaphore permits {}, current degree of parallelism {}",
								partitionKeyRangeId, samplePeriod.getSeconds(), throttleSemaphore.availablePermits(), degreeOfConcurrency);
						Thread.sleep(samplePeriod.toMillis());
						logger.debug("pki {} wakes up", partitionKeyRangeId);

						InsertMetrics insertMetricsSample = atomicGetAndReplace(new InsertMetrics());

						if (insertMetricsSample.numberOfThrottles > 0) {
							logger.debug("pki {} importing encountered {} throttling. current degree of parallelism {}, decreasing amount: {}",
									partitionKeyRangeId, insertMetricsSample.numberOfThrottles, degreeOfConcurrency, degreeOfConcurrency / DIVISIVE_DECREASE_FACTOR);

							// We got a throttle so we need to back off on the degree of concurrency.
							// Get the current degree of concurrency and decrease that (AIMD).

							for (int i = 0; i < degreeOfConcurrency / DIVISIVE_DECREASE_FACTOR; i++) {
								throttleSemaphore.acquire();
							}

							degreeOfConcurrency -= (degreeOfConcurrency / DIVISIVE_DECREASE_FACTOR);

							logger.debug("pki {} degree of parallelism reduced to {}, sem available permits", partitionKeyRangeId, degreeOfConcurrency, throttleSemaphore.availablePermits());
						}

						if (insertMetricsSample.numberOfDocumentsInserted == 0) {
							// We haven't made any progress, since the last sampling
							continue;
						}

						logger.debug("pki {} aggregating inserts metrics", partitionKeyRangeId);

						if (insertMetricsSample.numberOfThrottles == 0) {
							if ((insertMetricsSample.requestUnitsConsumed < THROUGHPUT_THRESHOLD * partitionThroughput) &&
									degreeOfConcurrency + ADDITIVE_INCREASE_FACTOR <= MAX_DEGREE_OF_CONCURRENCY) {
								// We aren't getting throttles, so we should bump of the degree of concurrency (AIMD).
								logger.debug("pki {} increasing degree of prallelism and releasing semaphore", partitionKeyRangeId);

								throttleSemaphore.release(ADDITIVE_INCREASE_FACTOR);
								degreeOfConcurrency += ADDITIVE_INCREASE_FACTOR;

								logger.debug("pki {} degree of parallelism increased to {}. available semaphore permits {}", partitionKeyRangeId, degreeOfConcurrency, throttleSemaphore.availablePermits());
							}
						}

						double ruPerSecond = insertMetricsSample.requestUnitsConsumed / samplePeriod.getSeconds();
						documentsOperatedSoFar += insertMetricsSample.numberOfDocumentsInserted;

						logger.debug("pki {} : Inserted {} docs in {} milli seconds at {} RU/s with {} tasks."
								+ " Faced {} throttles. Total documents inserterd so far {}.",
								partitionKeyRangeId,
								insertMetricsSample.numberOfDocumentsInserted,
								samplePeriod.toMillis(),
								ruPerSecond,
								degreeOfConcurrency,
								insertMetricsSample.numberOfThrottles,
								documentsOperatedSoFar);

					} catch (InterruptedException e) {
						logger.warn("Interrupted", e);
						break;
					} catch (Exception e) {
						logger.error("pki {} unexpected failure", partitionKeyRangeId, e);
						throw e;
					}

				}
				return null;
			};
		};
	}

	public ListenableFuture<Void> executeAllAsync()  {

		Callable<ListenableFuture<Void>> c = new Callable<ListenableFuture<Void>>() {

			@Override
			public ListenableFuture<Void> call() throws Exception {
				return executeAll();
			}
		};

		ListenableFuture<ListenableFuture<Void>> f = executor.submit(c);
		AsyncFunction<ListenableFuture<Void>, Void> function = new AsyncFunction<ListenableFuture<Void>, Void>() {

			@Override
			public ListenableFuture<Void> apply(ListenableFuture<Void> input) throws Exception {
				return input;
			}
		};
		return Futures.transformAsync(f, function, executor);
	}

	public ListenableFuture<Void> executeAll()  {

		logger.debug("pki{} Executing batching", partitionKeyRangeId);

		ListenableFuture<Void> completionFuture = executor.submit(congestionControlTask());

		Iterator<Callable<InsertMetrics>> batchExecutionIterator = batchOperator.miniBatchExecutionCallableIterator();

		List<ListenableFuture<InsertMetrics>> futureList = new ArrayList<>();
		while(batchExecutionIterator.hasNext() && isRunning()) {
			Callable<InsertMetrics> task = batchExecutionIterator.next();

			// Main thread waits on the throttleSem so no more than MaxDegreeOfParallelism Tasks are run at a time.
			try {
				logger.debug("pki {} trying to acquire semaphore to execute a task. available permits {}", partitionKeyRangeId, this.throttleSemaphore.availablePermits());
				this.throttleSemaphore.acquire();
				logger.debug("pki {} acquiring semaphore for executing a task succeeded. available permits {}", partitionKeyRangeId, this.throttleSemaphore.availablePermits());
			} catch (InterruptedException e) {
				logger.error("pki {} Interrupted, releasing semaphore", partitionKeyRangeId, e);
				this.throttleSemaphore.release();
				throw new RuntimeException(e);
			}

			if (failed()) {
				logger.error("pki {} already failed due to earlier failures. not submitting new tasks", partitionKeyRangeId);
				// release the already acquired semaphore
				this.throttleSemaphore.release();
				break;
			}

			ListenableFuture<InsertMetrics> insertMetricsFuture = executor.submit(task);

			FutureCallback<InsertMetrics> aggregateMetricsReleaseSemaphoreCallback = new FutureCallback<InsertMetrics>() {

				@Override
				public void onSuccess(InsertMetrics result) {
					logger.debug("pki {} accquiring a synchronized lock to update metrics", partitionKeyRangeId);

					synchronized (aggregateLock) {
						aggregatedInsertMetrics = InsertMetrics.sum(aggregatedInsertMetrics, result);
					}
					logger.debug("pki {} releasing semaphore on completion of task", partitionKeyRangeId);
					throttleSemaphore.release();
				}

				@Override
				public void onFailure(Throwable t) {
					logger.error("pki {} encountered failure {} releasing semaphore", partitionKeyRangeId, t);
					// if a batch inserter encounters failure which cannot be retried then we have to stop.
					setState(State.Failure);
					addFailure(ExceptionUtils.toException(t));
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
				logger.debug("pki {} importing completed", partitionKeyRangeId);
				setState(State.Completed);
			}

			@Override
			public void onFailure(Throwable t) {
				logger.error("pki {} importing failed", partitionKeyRangeId, t);
				setState(State.Failure);
			}
		};

		Futures.addCallback(allFutureResults, completionCallback, MoreExecutors.directExecutor());
		return completionFuture;
	}

	public void setState(State state) {
		logger.debug("pki {} state set to {}", partitionKeyRangeId, state);
		this.state = state;
	}

	public boolean isRunning() {
		logger.trace("pki {} in isRunning", partitionKeyRangeId);
		return state == State.Running;
	}

	public boolean completed() {
		return state == State.Completed;
	}

	public boolean failed() {
		return state == State.Failure;
	}

	public int getDegreeOfConcurrency() {
		return this.degreeOfConcurrency;
	}
}
