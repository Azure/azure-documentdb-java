package com.microsoft.azure.documentdb;

/**
 * A utility class to manage retries for resource throttled errors. It invokes a
 * delegate function to execute the target operation. Upon error, it waits a
 * period of time as defined in the ResourceThrottleRetryPolicy instance before
 * re-issuing the same request. The ResourceThrottleRetryPolicy object decides
 * the wait period based on the 'x-ms-retry-after-ms' response header sent by server.  
 * 
 */
class BackoffRetryUtility {

    /**
     * Executes the code block in the delegate and retry if needed.
     * 
     * @param delegate
     *            the delegate to execute.
     * @param request
     *            the request parameter for the execution.
     * @param maxRetryAttempts
     *            the max number of retries.
     * @throws DocumentClientException
     *             the original exception if retry is not applicable or if number of retries
     *             exceeded maxRetryAttempts.
     */
    public static DocumentServiceResponse execute(BackoffRetryUtilityDelegate delegate, DocumentServiceRequest request,
            int maxRetryAttempts) throws DocumentClientException {
        DocumentServiceResponse response = null;
        ResourceThrottleRetryPolicy retryPolicy = new ResourceThrottleRetryPolicy(maxRetryAttempts);
        while (true) {
            try {
                response = delegate.apply(request);
                break;
            } catch (DocumentClientException e) {
                boolean retry = retryPolicy.shouldRetry(e);
                if (!retry) {
                    throw e;
                }

                BackoffRetryUtility.delayForRetry(retryPolicy);
            }
        }

        return response;
    }

    /**
     * Executes the code block in the delegate and retry if needed.
     * 
     * @param delegate
     *            the delegate to execute.
     * @param maxRetryAttempts
     *            the max number of retries.
     */
    public static void execute(QueryBackoffRetryUtilityDelegate delegate, int maxRetryAttempts) {
        ResourceThrottleRetryPolicy retryPolicy = new ResourceThrottleRetryPolicy(maxRetryAttempts);
        while (true) {
            try {
                delegate.apply();
                break;
            } catch (Exception e) {
                boolean retry = false;
                if (DocumentClientException.class == e.getClass()) {
                    retry = retryPolicy.shouldRetry((DocumentClientException) e);
                }

                if (!retry) {
                    throw new IllegalStateException("Exception not retriable", e);
                }

                BackoffRetryUtility.delayForRetry(retryPolicy);
            }
        }
    }

    private static void delayForRetry(ResourceThrottleRetryPolicy retryPolicy) {
        final long delay = retryPolicy.getRetryAfterInMilliseconds();
        if (delay > 0) {
            try {
                Thread.sleep(delay);
            } catch (InterruptedException e) {
                // Ignore the interruption.
            }
        }
    }
}
