package com.microsoft.azure.documentdb.internal.query;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.microsoft.azure.documentdb.*;
import com.microsoft.azure.documentdb.internal.*;

final class ParallelQueryExecutionContext extends AbstractQueryExecutionContext<Document> {
    // Constants
    private static final long MAXIMUM_TIME_TO_WAIT_IN_SECONDS = 5;
    private static final int AUTO_MODE_TASKS_INCREMENT_FACTOR = 2;
    private static final int DEFAULT_MAX_ITEM_COUNT = 100;
    private static final int DEFAULT_ORDER_BY_PAGE_SIZE = 1000;
    private static final int MINIMUM_PAGE_SIZE = 5;
    private static final double BUFFERED_ITEM_COUNT_SLACK = 0.2;
    private static final String CONTINUATION_TOKEN = "ParallelQueryExecutionContext";
    // DocumentProducers
    private final List<DocumentProducer> documentProducers;
    private final PriorityBlockingQueue<DocumentProducer> documentProducerConsumePriorityQueue;
    private final PriorityBlockingQueue<DocumentProducer> documentProducerProducePriorityQueue;
    // Caps
    private final int maxDegreeOfParallelism;
    private final int maxBufferedItemCount;
    // AtomicIntegers to track states
    private final AtomicInteger totalNumberOfRunningDocumentProducers;
    private final AtomicInteger totalNumberOfRequestRoundtrips;
    private final AtomicInteger totalNumberOfDocumentProducersFinished;
    private final AtomicInteger totalBufferedItems;
    // Locks
    private final ReentrantLock taskSubmissionLock;
    private final Condition canSubmitTaskCondition;
    // Helper member fields
    private final RequestChargeTracker chargeTracker;
    private DocumentProducer currentDocumentProducer;
    private double currentAverageNumberOfRoundTripsPerTask;
    // Futures for initialization and scheduling
    private final Future<ParallelQueryExecutionContext> initializationFuture;
    private Future<ParallelQueryExecutionContext> schedulingFuture;

    public ParallelQueryExecutionContext(DocumentQueryClient client, SqlQuerySpec querySpec, FeedOptions options,
            String resourceLink, PartitionedQueryExecutionInfo partitionedQueryExecutionInfo) {
        super(client, ResourceType.Document, Document.class,
                partitionedQueryExecutionInfo.getQueryInfo().hasRewrittenQuery()
                        ? new SqlQuerySpec(partitionedQueryExecutionInfo.getQueryInfo().getRewrittenQuery(),
                                querySpec.getParameters())
                        : querySpec,
                options, resourceLink);
        boolean hasOrderBy = partitionedQueryExecutionInfo.getQueryInfo().hasOrderBy();
        boolean hasTop = partitionedQueryExecutionInfo.getQueryInfo().hasTop();

        Collection<PartitionKeyRange> ranges = super.getTargetPartitionKeyRanges(
                partitionedQueryExecutionInfo.getQueryRanges());
        this.documentProducers = new ArrayList<DocumentProducer>(ranges.size());
        this.documentProducerProducePriorityQueue = new PriorityBlockingQueue<DocumentProducer>(ranges.size(),
                hasOrderBy ? new OrderByDocumentProducerProduceComparator()
                        : DefaultDocumentProducerComparator.getInstance());
        this.documentProducerConsumePriorityQueue = new PriorityBlockingQueue<DocumentProducer>(ranges.size(),
                hasOrderBy
                        ? new OrderByDocumentProducerConsumeComparator(
                                partitionedQueryExecutionInfo.getQueryInfo().getOrderBy())
                        : DefaultDocumentProducerComparator.getInstance());

        Integer pageSizeForOrderBy = options.getPageSize() == null || options.getPageSize() < 1
                ? DEFAULT_ORDER_BY_PAGE_SIZE : Math.max(options.getPageSize(), MINIMUM_PAGE_SIZE);
        Class<? extends Document> documentProducerClassT = hasOrderBy ? DocumentQueryResult.class : Document.class;

        for (PartitionKeyRange range : ranges) {
            DocumentServiceRequest request = super.createRequest(super.querySpec, range);

            if (hasOrderBy) {
                request.getHeaders().put(HttpConstants.HttpHeaders.PAGE_SIZE, pageSizeForOrderBy.toString());
            }

            this.documentProducers.add(new DocumentProducer(this, request, range, documentProducerClassT));
        }

        this.maxDegreeOfParallelism = Math.min(ranges.size(), options.getMaxDegreeOfParallelism());
        this.maxBufferedItemCount = hasOrderBy
                ? (int) (ranges.size() * pageSizeForOrderBy * (1 + BUFFERED_ITEM_COUNT_SLACK))
                : Math.max(options.getMaxBufferedItemCount(), DEFAULT_MAX_ITEM_COUNT);

        this.totalNumberOfRunningDocumentProducers = new AtomicInteger();
        this.totalNumberOfRequestRoundtrips = new AtomicInteger();
        this.totalNumberOfDocumentProducersFinished = new AtomicInteger();
        this.totalBufferedItems = new AtomicInteger();

        this.taskSubmissionLock = new ReentrantLock();
        this.canSubmitTaskCondition = this.taskSubmissionLock.newCondition();

        this.chargeTracker = new RequestChargeTracker();

        this.currentAverageNumberOfRoundTripsPerTask = 1;

        this.initializationFuture = DocumentDBExecutorService.getExecutorService()
                .submit(this.getInitializationCallable(hasOrderBy || !hasTop));

    }

    @Override
    protected void finalize() throws Throwable {
        this.initializationFuture.cancel(true);
        if (this.schedulingFuture != null) {
            this.schedulingFuture.cancel(true);
        }
    }

    @Override
    public List<Document> fetchNextBlock() throws DocumentClientException {
        throw new UnsupportedOperationException("fetchNextBlock");
    }

    @Override
    public boolean hasNext() {
        try {
            this.initializationFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            throw new IllegalStateException("Failed to initialize.", e);
        }

        return super.hasNextInternal();
    }

    @Override
    public Document next() {
        if (!this.hasNext()) {
            throw new NoSuchElementException("next");
        }

        try {
            this.initializationFuture.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new IllegalStateException("Failed to initialize.", e);
        }

        if (super.responseHeaders == null) {
            super.responseHeaders = new HashMap<String, String>();
        }

        Document result;
        try {
            if (this.currentDocumentProducer == null || !this.currentDocumentProducer.hasNext()) {
                this.currentDocumentProducer = this.documentProducerConsumePriorityQueue.take();
            } else if (!this.documentProducerConsumePriorityQueue.isEmpty()
                    && this.documentProducerConsumePriorityQueue.comparator().compare(this.currentDocumentProducer,
                            this.documentProducerConsumePriorityQueue.peek()) > 0) {
                this.documentProducerConsumePriorityQueue.put(this.currentDocumentProducer);
                this.currentDocumentProducer = this.documentProducerConsumePriorityQueue.take();
            }

            result = this.currentDocumentProducer.next();
            this.totalBufferedItems.decrementAndGet();

            final ReentrantLock lock = this.taskSubmissionLock;
            lock.lockInterruptibly();
            try {
                if (this.canSubmitTask()) {
                    this.canSubmitTaskCondition.signal();
                }
            } finally {
                lock.unlock();
            }
        } catch (InterruptedException e) {
            throw new IllegalStateException("Failed to take from DocumentProducer consume queue.", e);
        }

        if (!this.currentDocumentProducer.hasNext() && this.documentProducerConsumePriorityQueue.isEmpty()) {
            this.onFinish();
        } else {
            super.responseHeaders.put(HttpConstants.HttpHeaders.CONTINUATION, CONTINUATION_TOKEN);
        }

        return result;
    }

    @Override
    public void onNotifyStop() {
        this.totalNumberOfDocumentProducersFinished.set(this.documentProducers.size());
        this.unblockProduceQueue(this.documentProducers.get(0));
        try {
            this.initializationFuture.get();
            if (this.schedulingFuture != null) {
                this.schedulingFuture.get();
            }
        } catch (Exception e) {
            throw new IllegalStateException("Failed to wait for Futures to finish.", e);
        }

        this.onFinish();
    }

    private void onFinish() {
        super.responseHeaders.remove(HttpConstants.HttpHeaders.CONTINUATION);
        super.responseHeaders.put(HttpConstants.HttpHeaders.REQUEST_CHARGE,
                String.valueOf(this.chargeTracker.getAndResetCharge()));
    }

    private void unblockProduceQueue(DocumentProducer documentProducer) {
        /*
         * if Thread 1 sees shouldProduce == true, calls
         * documentProducerProducePriorityQueue.take(), following which Thread 2
         * calls totalNumberOfDocumentProducersFinished.incrementAndGet and
         * makes shouldProduce == false, and also
         * documentProducerProducePriorityQueue is being empty, we would then
         * need to unblock documentProducerProducePriorityQueue.take() call by
         * queuing a finished DocumentProducer
         */
        if (!this.shouldProduce() && this.documentProducerProducePriorityQueue.isEmpty()) {
            synchronized (this.documentProducerProducePriorityQueue) {
                if (this.documentProducerProducePriorityQueue.isEmpty()) {
                    this.documentProducerProducePriorityQueue.put(documentProducer);
                }
            }
        }
    }

    private boolean canSubmitTask() {
        // Note: We should have allow at least one DocumentProducer to run
        return (this.totalNumberOfRunningDocumentProducers.get() < this.getNumberOfTasksToRunBasedOnCurrentState()
                && this.totalBufferedItems.get() < this.maxBufferedItemCount * (1 + BUFFERED_ITEM_COUNT_SLACK))
                || this.totalNumberOfRunningDocumentProducers.get() < 1;
    }

    private Future<DocumentProducer> submitTask(DocumentProducer producer) throws InterruptedException {
        final ReentrantLock lock = this.taskSubmissionLock;
        lock.lockInterruptibly();
        try {
            while (!this.canSubmitTask()) {
                this.canSubmitTaskCondition.await(MAXIMUM_TIME_TO_WAIT_IN_SECONDS, TimeUnit.SECONDS);
            }

            this.totalNumberOfRunningDocumentProducers.incrementAndGet();
            return DocumentDBExecutorService.getExecutorService().submit(this.getDocumentProducerCallable(producer));
        } finally {
            lock.unlock();
        }
    }

    private Callable<ParallelQueryExecutionContext> getInitializationCallable(final boolean prefetchAll) {
        return new Callable<ParallelQueryExecutionContext>() {
            @Override
            public ParallelQueryExecutionContext call() throws Exception {
                // We need to run each DocumentProducer at least once to
                // reliably answer ParallelQueryExecutionContext.hasNext
                ParallelQueryExecutionContext thisContext = ParallelQueryExecutionContext.this;

                List<Future<DocumentProducer>> futures = new ArrayList<Future<DocumentProducer>>();
                for (DocumentProducer producer : thisContext.documentProducers) {
                    futures.add(thisContext.submitTask(producer));
                }

                thisContext.schedulingFuture = DocumentDBExecutorService.getExecutorService()
                        .submit(thisContext.getSchedulingCallable());

                for (Future<DocumentProducer> future : futures) {
                    future.get();
                    
                    // $ISSUE-felixfan-2016-08-30: This optimization causes correctness issue. Will come back later.
                    // if (!prefetchAll && thisContext.documentProducerConsumePriorityQueue.size() > 0) {
                    //    break;
                    // }
                }

                if (thisContext.documentProducerConsumePriorityQueue.isEmpty()) {
                    // Initialize responseHeaders to indicate !hasNext
                    ParallelQueryExecutionContext.super.responseHeaders = new HashMap<String, String>();
                }

                return thisContext;
            }
        };

    }

    private boolean shouldProduce() {
        return this.totalNumberOfDocumentProducersFinished.get() < this.documentProducers.size();
    }

    private Callable<ParallelQueryExecutionContext> getSchedulingCallable() {
        return new Callable<ParallelQueryExecutionContext>() {
            @Override
            public ParallelQueryExecutionContext call() throws Exception {
                ParallelQueryExecutionContext thisContext = ParallelQueryExecutionContext.this;

                while (thisContext.shouldProduce()) {
                    thisContext.submitTask(thisContext.documentProducerProducePriorityQueue.take());
                }

                return thisContext;
            }
        };
    }

    private int getNumberOfTasksToRunBasedOnCurrentState() {
        if (this.maxDegreeOfParallelism >= 1) {
            return this.maxDegreeOfParallelism;
        }

        int numTasksServingParallelRequests = this.totalNumberOfRunningDocumentProducers.get();

        if (numTasksServingParallelRequests == 0) {
            return AUTO_MODE_TASKS_INCREMENT_FACTOR;
        }

        int returnVal = numTasksServingParallelRequests;
        double currentAverageNumberOfRoundTripsPerTask = (double) this.totalNumberOfRequestRoundtrips.get()
                / numTasksServingParallelRequests;

        if (currentAverageNumberOfRoundTripsPerTask > this.currentAverageNumberOfRoundTripsPerTask) {
            returnVal *= AUTO_MODE_TASKS_INCREMENT_FACTOR;
        }

        this.currentAverageNumberOfRoundTripsPerTask = currentAverageNumberOfRoundTripsPerTask;
        return Math.max(returnVal, Runtime.getRuntime().availableProcessors());
    }

    private Callable<DocumentProducer> getDocumentProducerCallable(final DocumentProducer documentProducer) {
        return new Callable<DocumentProducer>() {
            @Override
            public DocumentProducer call() throws Exception {
                // We might see a finished DocumentProducer.
                // See explanation in the following comment.
                if (documentProducer.isFinished()) {
                    return documentProducer;
                }

                ParallelQueryExecutionContext thisContext = ParallelQueryExecutionContext.this;

                boolean hasStarted = documentProducer.hasStarted();
                if (!documentProducer.produce().isFinished()) {
                    thisContext.documentProducerProducePriorityQueue.put(documentProducer);
                } else {
                    thisContext.totalNumberOfDocumentProducersFinished.incrementAndGet();
                    thisContext.unblockProduceQueue(documentProducer);
                }

                if (!hasStarted && documentProducer.hasNext()) {
                    thisContext.documentProducerConsumePriorityQueue.put(documentProducer);
                }

                thisContext.chargeTracker.addCharge(Double.parseDouble(
                        documentProducer.getPreviousResponseHeaders().get(HttpConstants.HttpHeaders.REQUEST_CHARGE)));

                thisContext.totalBufferedItems.addAndGet(documentProducer.getPreviousResponseItemCount());
                thisContext.totalNumberOfRequestRoundtrips.incrementAndGet();
                thisContext.totalNumberOfRunningDocumentProducers.decrementAndGet();

                final ReentrantLock lock = thisContext.taskSubmissionLock;
                lock.lockInterruptibly();
                try {
                    if (thisContext.canSubmitTask()) {
                        thisContext.canSubmitTaskCondition.signal();
                    }
                } finally {
                    lock.unlock();
                }

                return documentProducer;
            }
        };
    }
}