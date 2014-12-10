package com.microsoft.azure.documentdb;

class BackoffRetryUtility {

    /**
     * Executes the code block in the delegate and maybe retries.
     * 
     * @param delegate the delegate to execute.
     * @param retryPolicy the retry policy.
     */
    public static void execute(BackoffRetryUtilityDelegate delegate, ResourceThrottleRetryPolicy retryPolicy) {
        while (true) {
            try {
                delegate.apply();
                break;  // Break from the while loop if no exception happened.
            } catch (Exception e) {
                boolean retry = retryPolicy.shouldRetry(e);
                if (!retry) {
                    e.printStackTrace();
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
                e.printStackTrace();
            }
        }
    }
}
